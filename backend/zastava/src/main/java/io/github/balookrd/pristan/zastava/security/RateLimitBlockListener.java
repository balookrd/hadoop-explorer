package io.github.balookrd.pristan.zastava.security;

import io.github.balookrd.pristan.zastava.resilience.RateLimiterService;

/**
 * Notified by {@link RateLimitFilter} each time it answers 429. Keeps the filter free of
 * metrics classes: the Micrometer implementation is registered only when Micrometer is present.
 */
@FunctionalInterface
public interface RateLimitBlockListener {

    void onBlock(RateLimiterService.Tier tier);
}
