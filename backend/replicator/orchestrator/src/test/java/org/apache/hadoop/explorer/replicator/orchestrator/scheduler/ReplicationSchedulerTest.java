package org.apache.hadoop.explorer.replicator.orchestrator.scheduler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationSchedulerTest {

    @Test
    @DisplayName("Должен корректно вычислять время следующего запуска по пресетам")
    void shouldComputeNextRunForPresets() {
        Instant base = Instant.parse("2026-10-08T12:00:00Z");

        // @minutely
        Instant minutely = ReplicationScheduler.computeNextRun("@minutely", base);
        assertEquals(base.plus(Duration.ofMinutes(1)), minutely);

        // @every_5m
        Instant every5m = ReplicationScheduler.computeNextRun("@every_5m", base);
        assertEquals(base.plus(Duration.ofMinutes(5)), every5m);

        // @hourly
        Instant hourly = ReplicationScheduler.computeNextRun("@hourly", base);
        assertEquals(base.plus(Duration.ofHours(1)), hourly);

        // @daily
        Instant daily = ReplicationScheduler.computeNextRun("@daily", base);
        assertEquals(base.plus(Duration.ofDays(1)), daily);

        // interval pattern */10 * * * *
        Instant every10m = ReplicationScheduler.computeNextRun("*/10 * * * *", base);
        assertEquals(base.plus(Duration.ofMinutes(10)), every10m);
    }
}
