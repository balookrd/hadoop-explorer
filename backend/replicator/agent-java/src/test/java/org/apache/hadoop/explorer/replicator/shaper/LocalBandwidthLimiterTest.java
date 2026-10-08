package org.apache.hadoop.explorer.replicator.shaper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LocalBandwidthLimiterTest {

    @Test
    public void testDisabledLimiter() {
        LocalBandwidthLimiter limiter = new LocalBandwidthLimiter(0.0);
        assertFalse(limiter.isEnabled());
        assertEquals(0.0, limiter.throttle(1024 * 1024));
    }

    @Test
    public void testEnabledLimiterThrottling() {
        // Лимит 1 МБ/с, burst 0.1 сек => capacity ~ 100 KB
        LocalBandwidthLimiter limiter = new LocalBandwidthLimiter(1.0, 0.1);
        assertTrue(limiter.isEnabled());
        assertEquals(1.0, limiter.getLimitMbPerSec());

        // Первый запрос укладывается в burst/tokens
        double wait1 = limiter.throttle(50 * 1024);
        assertEquals(0.0, wait1);

        // Большой запрос свыше емкости приводит к задержке
        double wait2 = limiter.throttle(500 * 1024);
        assertTrue(wait2 > 0.0, "Ожидалась ненулевая задержка троттлера");
    }

    @Test
    public void testDynamicLimitAdjustment() {
        LocalBandwidthLimiter limiter = new LocalBandwidthLimiter(10.0);
        assertEquals(10.0, limiter.getLimitMbPerSec());

        limiter.setLimit(25.5);
        assertEquals(25.5, limiter.getLimitMbPerSec());

        limiter.setLimit(0.0);
        assertFalse(limiter.isEnabled());
    }
}
