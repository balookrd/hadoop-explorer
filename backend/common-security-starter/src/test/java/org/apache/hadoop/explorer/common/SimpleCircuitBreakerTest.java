package org.apache.hadoop.explorer.common;

import org.apache.hadoop.explorer.common.resilience.SimpleCircuitBreaker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SimpleCircuitBreakerTest {

    @Test
    @DisplayName("Должен успешно выполнять действия в состоянии CLOSED")
    void shouldExecuteActionInClosedState() {
        SimpleCircuitBreaker cb = new SimpleCircuitBreaker("test-service", 3, 1000);
        assertEquals(SimpleCircuitBreaker.State.CLOSED, cb.getState());

        String result = cb.execute(() -> "success", () -> "fallback");
        assertEquals("success", result);
        assertEquals(SimpleCircuitBreaker.State.CLOSED, cb.getState());
    }

    @Test
    @DisplayName("Должен переходить в состояние OPEN после превышения порога ошибок и запускать fallback")
    void shouldTripToOpenOnThresholdFailures() {
        SimpleCircuitBreaker cb = new SimpleCircuitBreaker("test-service", 2, 500);

        // Первая ошибка
        assertThrows(RuntimeException.class, () -> cb.execute(() -> {
            throw new RuntimeException("fail 1");
        }, null));
        assertEquals(SimpleCircuitBreaker.State.CLOSED, cb.getState());

        // Вторая ошибка -> переход в OPEN
        assertThrows(RuntimeException.class, () -> cb.execute(() -> {
            throw new RuntimeException("fail 2");
        }, null));
        assertEquals(SimpleCircuitBreaker.State.OPEN, cb.getState());

        // После перехода в OPEN - Fast-Fail с вызовом fallback без обращения к сервису
        String fallbackResult = cb.execute(() -> "should not be called", () -> "fast-fail-fallback");
        assertEquals("fast-fail-fallback", fallbackResult);
    }
}
