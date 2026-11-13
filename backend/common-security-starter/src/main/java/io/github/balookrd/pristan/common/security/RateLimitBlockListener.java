package io.github.balookrd.pristan.common.security;

import io.github.balookrd.pristan.common.resilience.RateLimiterService;

/**
 * Notified by {@link RateLimitFilter} each time it answers 429. Keeps the filter free of
 * metrics classes: the Micrometer implementation is registered only when Micrometer is present.
 */
@FunctionalInterface
public interface RateLimitBlockListener {

    void onBlock(RateLimiterService.Tier tier);
}
