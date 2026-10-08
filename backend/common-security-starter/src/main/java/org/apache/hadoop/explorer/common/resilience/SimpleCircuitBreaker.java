package org.apache.hadoop.explorer.common.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Отказоустойчивый Circuit Breaker для внешних Hadoop RPC сервисов (NameNode HA, YARN RM, Livy).
 * Предотвращает лавинные сбои (Fast-Fail) при недоступности нижележащих служб.
 */
public class SimpleCircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(SimpleCircuitBreaker.class);

    public enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private final String name;
    private final int failureThreshold;
    private final long recoveryTimeoutMs;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicReference<Instant> lastFailureTime = new AtomicReference<>(Instant.EPOCH);

    public SimpleCircuitBreaker(String name, int failureThreshold, long recoveryTimeoutMs) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.recoveryTimeoutMs = recoveryTimeoutMs;
    }

    public State getState() {
        if (state.get() == State.OPEN) {
            long elapsed = Instant.now().toEpochMilli() - lastFailureTime.get().toEpochMilli();
            if (elapsed >= recoveryTimeoutMs) {
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    log.info("CircuitBreaker '{}' transitioned from OPEN to HALF_OPEN", name);
                }
            }
        }
        return state.get();
    }

    public <T> T execute(Supplier<T> action, Supplier<T> fallback) {
        State current = getState();
        if (current == State.OPEN) {
            log.warn("CircuitBreaker '{}' is OPEN, executing fast-fail fallback", name);
            if (fallback != null) {
                return fallback.get();
            }
            throw new IllegalStateException("Service '" + name + "' is temporarily unavailable (CircuitBreaker OPEN)");
        }

        try {
            T result = action.get();
            onSuccess();
            return result;
        } catch (Exception e) {
            onFailure(e);
            if (fallback != null) {
                return fallback.get();
            }
            throw e;
        }
    }

    private void onSuccess() {
        if (state.get() == State.HALF_OPEN) {
            state.set(State.CLOSED);
            failureCount.set(0);
            log.info("CircuitBreaker '{}' recovered to CLOSED state", name);
        } else if (state.get() == State.CLOSED) {
            failureCount.set(0);
        }
    }

    private void onFailure(Exception e) {
        lastFailureTime.set(Instant.now());
        int count = failureCount.incrementAndGet();
        log.warn("CircuitBreaker '{}' recorded failure #{}: {}", name, count, e.getMessage());

        if (state.get() == State.HALF_OPEN || count >= failureThreshold) {
            state.set(State.OPEN);
            log.error("CircuitBreaker '{}' tripped to OPEN state! Downstream calls will fast-fail for {} ms",
                name, recoveryTimeoutMs);
        }
    }
}
