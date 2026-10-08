package org.apache.hadoop.explorer.common.resilience;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Ограничитель частоты запросов (Rate Limiter) на алгоритме Token Bucket (Bucket4j).
 */
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);

    private final CommonSecurityProperties properties;
    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    public RateLimiterService(CommonSecurityProperties properties) {
        this.properties = properties;
    }

    private Bucket createBucket() {
        int rps = properties.getRateLimiter().getRequestsPerMinute();
        int burst = properties.getRateLimiter().getBurstCapacity();

        Bandwidth limit = Bandwidth.classic(
            burst,
            Refill.greedy(rps, Duration.ofMinutes(1))
        );
        return Bucket.builder().addLimit(limit).build();
    }

    public boolean tryAcquire(String key) {
        if (!properties.getRateLimiter().isEnabled()) {
            return true;
        }
        Bucket bucket = buckets.computeIfAbsent(key, k -> createBucket());
        boolean consumed = bucket.tryConsume(1);
        if (!consumed) {
            log.warn("Rate limit exceeded for client key: {}", key);
        }
        return consumed;
    }
}
