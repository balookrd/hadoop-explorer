package org.apache.hadoop.explorer.common.resilience;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.ConsumptionProbe;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Token Bucket rate limiter (Bucket4j) with one bucket per key and tier.
 *
 * <p>Buckets live in a bounded Caffeine cache: a bucket idle for 10 minutes is dropped, which is
 * safe while a bucket refills completely in less than that (10 / 30 per minute for LOGIN,
 * 100 / 600 per minute for API by default). The bound keeps memory flat when keys are spoofed.
 */
public class RateLimiterService {

    public enum Tier { LOGIN, API }

    public record Decision(boolean allowed, long retryAfterSeconds) {
        static final Decision ALLOWED = new Decision(true, 0);
    }

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);
    private static final Duration IDLE_EXPIRY = Duration.ofMinutes(10);

    private final CommonSecurityProperties properties;
    private final Map<Tier, Cache<String, Bucket>> buckets = new EnumMap<>(Tier.class);

    public RateLimiterService(CommonSecurityProperties properties) {
        this.properties = properties;
        long maxKeys = properties.getRateLimiter().getMaxTrackedKeys();
        for (Tier tier : Tier.values()) {
            buckets.put(tier, Caffeine.newBuilder()
                .maximumSize(maxKeys)
                .expireAfterAccess(IDLE_EXPIRY)
                .build());
        }
    }

    /** API-tier check kept for callers that only need a yes/no answer. */
    public boolean tryAcquire(String key) {
        return tryConsume(Tier.API, key).allowed();
    }

    public Decision tryConsume(Tier tier, String key) {
        if (!properties.getRateLimiter().isEnabled()) {
            return Decision.ALLOWED;
        }
        Bucket bucket = buckets.get(tier).get(key, k -> createBucket(tier));
        ConsumptionProbe probe = bucket.tryConsumeAndReturnRemaining(1);
        if (probe.isConsumed()) {
            return Decision.ALLOWED;
        }
        long retryAfter = Math.max(1,
            TimeUnit.NANOSECONDS.toSeconds(probe.getNanosToWaitForRefill() + 999_999_999L));
        log.warn("Rate limit exceeded: tier={}, key={}, retry_after={}s", tier, key, retryAfter);
        return new Decision(false, retryAfter);
    }

    private Bucket createBucket(Tier tier) {
        CommonSecurityProperties.RateLimiterProperties cfg = properties.getRateLimiter();
        int perMinute = tier == Tier.LOGIN ? cfg.getLogin().getRequestsPerMinute() : cfg.getRequestsPerMinute();
        int burst = tier == Tier.LOGIN ? cfg.getLogin().getBurstCapacity() : cfg.getBurstCapacity();
        return Bucket.builder()
            .addLimit(Bandwidth.builder()
                .capacity(burst)
                .refillGreedy(perMinute, Duration.ofMinutes(1))
                .build())
            .build();
    }
}
