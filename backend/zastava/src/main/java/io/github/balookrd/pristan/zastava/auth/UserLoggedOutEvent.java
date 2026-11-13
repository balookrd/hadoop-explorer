package io.github.balookrd.pristan.zastava.auth;

/**
 * Published after POST /api/v1/auth/logout for the user whose token was revoked.
 * Services release per-user resources on it (Spark Explorer closes Livy sessions).
 *
 * <p>The event is published through the local {@code ApplicationEventPublisher} only, so with
 * {@code session-store.type: jdbc} and several service instances it reaches listeners of the
 * instance that handled the logout request. Other instances are not notified.
 *
 * <p>Listeners run synchronously in the logout request thread, after the token is revoked and
 * the SecurityContext is cleared. Keep them fast or move network and other heavy work to
 * {@code @Async}. A listener exception is logged at WARN and does not fail the logout.
 */
public record UserLoggedOutEvent(String username) {
}
