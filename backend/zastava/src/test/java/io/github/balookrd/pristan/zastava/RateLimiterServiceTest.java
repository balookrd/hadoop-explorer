package io.github.balookrd.pristan.zastava;

import io.github.balookrd.pristan.zastava.config.ZastavaProperties;
import io.github.balookrd.pristan.zastava.resilience.RateLimiterService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimiterServiceTest {

    @Test
    @DisplayName("Должен пропускать запросы в пределах емкости и блокировать при превышении лимита")
    void shouldEnforceRateLimit() {
        ZastavaProperties props = new ZastavaProperties();
        props.getRateLimiter().setEnabled(true);
        props.getRateLimiter().setBurstCapacity(3);
        props.getRateLimiter().setRequestsPerMinute(60);

        RateLimiterService limiter = new RateLimiterService(props);

        String ip = "192.168.1.100";
        assertTrue(limiter.tryAcquire(ip));
        assertTrue(limiter.tryAcquire(ip));
        assertTrue(limiter.tryAcquire(ip));
        // Четвертый запрос превышает burst capacity = 3
        assertFalse(limiter.tryAcquire(ip));

        // Для другого IP лимит независим
        assertTrue(limiter.tryAcquire("10.0.0.1"));
    }

    @Test
    @DisplayName("Корзина входа независима от корзины API и сообщает Retry-After")
    void loginTierIsSeparateAndReportsRetryAfter() {
        ZastavaProperties props = new ZastavaProperties();
        props.getRateLimiter().setEnabled(true);
        props.getRateLimiter().getLogin().setBurstCapacity(2);
        props.getRateLimiter().getLogin().setRequestsPerMinute(30);

        RateLimiterService limiter = new RateLimiterService(props);
        String key = "ip:10.0.0.7";

        assertTrue(limiter.tryConsume(RateLimiterService.Tier.LOGIN, key).allowed());
        assertTrue(limiter.tryConsume(RateLimiterService.Tier.LOGIN, key).allowed());
        RateLimiterService.Decision denied = limiter.tryConsume(RateLimiterService.Tier.LOGIN, key);
        assertFalse(denied.allowed());
        // 30 per minute refills one token every 2 seconds
        assertTrue(denied.retryAfterSeconds() >= 1 && denied.retryAfterSeconds() <= 2,
            "Retry-After: " + denied.retryAfterSeconds());

        assertTrue(limiter.tryConsume(RateLimiterService.Tier.API, key).allowed());
    }

    @Test
    @DisplayName("При enabled=false все запросы проходят")
    void disabledLimiterAllowsEverything() {
        ZastavaProperties props = new ZastavaProperties();
        props.getRateLimiter().setEnabled(false);
        props.getRateLimiter().getLogin().setBurstCapacity(1);

        RateLimiterService limiter = new RateLimiterService(props);
        for (int i = 0; i < 5; i++) {
            RateLimiterService.Decision decision = limiter.tryConsume(RateLimiterService.Tier.LOGIN, "ip:10.0.0.8");
            assertTrue(decision.allowed());
            assertEquals(0, decision.retryAfterSeconds());
        }
    }
}
