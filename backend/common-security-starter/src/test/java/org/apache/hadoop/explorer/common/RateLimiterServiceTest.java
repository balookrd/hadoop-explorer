package org.apache.hadoop.explorer.common;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.resilience.RateLimiterService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimiterServiceTest {

    @Test
    @DisplayName("Должен пропускать запросы в пределах емкости и блокировать при превышении лимита")
    void shouldEnforceRateLimit() {
        CommonSecurityProperties props = new CommonSecurityProperties();
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
}
