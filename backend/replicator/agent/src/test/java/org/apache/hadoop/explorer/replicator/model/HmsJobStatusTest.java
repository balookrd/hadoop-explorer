package org.apache.hadoop.explorer.replicator.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HmsJobStatusTest {

    @Test
    @DisplayName("Валидные переходы FSM жизненного цикла HMS задачи")
    void testValidTransitions() {
        // WAITING_FOR_AGENTS -> QUEUED
        assertTrue(HmsJobStatus.WAITING_FOR_AGENTS.canTransitionTo(HmsJobStatus.QUEUED));
        assertDoesNotThrow(() -> HmsJobStatus.WAITING_FOR_AGENTS.validateTransition(HmsJobStatus.QUEUED));

        // QUEUED -> BOOTSTRAPPING
        assertTrue(HmsJobStatus.QUEUED.canTransitionTo(HmsJobStatus.BOOTSTRAPPING));

        // BOOTSTRAPPING -> ACTIVE
        assertTrue(HmsJobStatus.BOOTSTRAPPING.canTransitionTo(HmsJobStatus.ACTIVE));

        // ACTIVE -> PAUSED
        assertTrue(HmsJobStatus.ACTIVE.canTransitionTo(HmsJobStatus.PAUSED));

        // PAUSED -> ACTIVE
        assertTrue(HmsJobStatus.PAUSED.canTransitionTo(HmsJobStatus.ACTIVE));

        // ACTIVE -> ERROR
        assertTrue(HmsJobStatus.ACTIVE.canTransitionTo(HmsJobStatus.ERROR));

        // ERROR -> ACTIVE
        assertTrue(HmsJobStatus.ERROR.canTransitionTo(HmsJobStatus.ACTIVE));

        // ACTIVE -> STOPPED
        assertTrue(HmsJobStatus.ACTIVE.canTransitionTo(HmsJobStatus.STOPPED));

        // STOPPED -> QUEUED (рестарт)
        assertTrue(HmsJobStatus.STOPPED.canTransitionTo(HmsJobStatus.QUEUED));

        // Идемпотентный переход
        assertTrue(HmsJobStatus.ACTIVE.canTransitionTo(HmsJobStatus.ACTIVE));
    }

    @Test
    @DisplayName("Недопустимые переходы FSM должны пресекаться исключением")
    void testInvalidTransitions() {
        // Из STOPPED напрямую в ACTIVE нельзя
        assertFalse(HmsJobStatus.STOPPED.canTransitionTo(HmsJobStatus.ACTIVE));
        assertThrows(IllegalStateException.class,
                () -> HmsJobStatus.STOPPED.validateTransition(HmsJobStatus.ACTIVE));

        // Из WAITING_FOR_AGENTS напрямую в ACTIVE нельзя
        assertFalse(HmsJobStatus.WAITING_FOR_AGENTS.canTransitionTo(HmsJobStatus.ACTIVE));
        assertThrows(IllegalStateException.class,
                () -> HmsJobStatus.WAITING_FOR_AGENTS.validateTransition(HmsJobStatus.ACTIVE));

        // Из FAILED напрямую в ACTIVE нельзя
        assertFalse(HmsJobStatus.FAILED.canTransitionTo(HmsJobStatus.ACTIVE));
        assertThrows(IllegalStateException.class,
                () -> HmsJobStatus.FAILED.validateTransition(HmsJobStatus.ACTIVE));

        // Переход в null
        assertFalse(HmsJobStatus.ACTIVE.canTransitionTo(null));
        assertThrows(IllegalStateException.class,
                () -> HmsJobStatus.ACTIVE.validateTransition(null));
    }

    @Test
    @DisplayName("Парсинг статусов из строки с fallback")
    void testFromString() {
        assertEquals(HmsJobStatus.ACTIVE, HmsJobStatus.fromString("ACTIVE"));
        assertEquals(HmsJobStatus.ACTIVE, HmsJobStatus.fromString("active"));
        assertEquals(HmsJobStatus.ACTIVE, HmsJobStatus.fromString("RUNNING"));
        assertEquals(HmsJobStatus.BOOTSTRAPPING, HmsJobStatus.fromString("bootstrap"));
        assertEquals(HmsJobStatus.WAITING_FOR_AGENTS, HmsJobStatus.fromString("waiting"));
        assertEquals(HmsJobStatus.QUEUED, HmsJobStatus.fromString(""));
        assertEquals(HmsJobStatus.QUEUED, HmsJobStatus.fromString(null));
    }
}
