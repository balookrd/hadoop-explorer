package org.apache.hadoop.explorer.replicator.orchestrator.scheduler;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ScheduleReconcilerTest {

    private static final Instant NOW = Instant.parse("2026-10-08T12:00:00Z");

    private static JobEntity job(String id, String cron, Instant nextRunAt) {
        JobEntity job = new JobEntity();
        job.setId(id);
        job.setStatus("SCHEDULED");
        job.setScheduled(true);
        job.setCronExpression(cron);
        job.setNextRunAt(nextRunAt);
        return job;
    }

    @Test
    @DisplayName("Старт: устаревший next_run_at календарных задач пересчитывается, невалидные расписания снимаются")
    void reconcilesStoredSchedules() {
        JobEntity staleDaily = job("daily", "0 0 * * *", Instant.parse("2026-10-09T08:00:00Z"));
        JobEntity invalid = job("bad", "every tuesday", Instant.parse("2026-10-08T13:00:00Z"));
        JobEntity interval = job("interval", "@every_5m", Instant.parse("2026-10-08T12:03:00Z"));
        JobEntity overdue = job("overdue", "0 0 * * *", Instant.parse("2026-10-08T11:59:00Z"));
        JobEntity unplanned = job("unplanned", "@every_15m", null);

        JobRepository repo = mock(JobRepository.class);
        when(repo.findScheduledJobs()).thenReturn(List.of(staleDaily, invalid, interval, overdue, unplanned));

        int changed = new ScheduleReconciler(repo, null, null, null).reconcile(NOW);

        assertEquals(3, changed);
        assertEquals(Instant.parse("2026-10-09T00:00:00Z"), staleDaily.getNextRunAt());
        assertFalse(invalid.isScheduled());
        assertEquals("STOPPED", invalid.getStatus());
        assertTrue(invalid.getMessage().startsWith("Расписание отключено"), invalid.getMessage());
        assertEquals(Instant.parse("2026-10-08T12:03:00Z"), interval.getNextRunAt(), "интервальные задачи не трогаются");
        assertEquals(Instant.parse("2026-10-08T11:59:00Z"), overdue.getNextRunAt(), "просроченную задачу запустит ближайший тик");
        assertEquals(Instant.parse("2026-10-08T12:15:00Z"), unplanned.getNextRunAt());

        verify(repo).save(staleDaily);
        verify(repo).save(invalid);
        verify(repo).save(unplanned);
        verify(repo, never()).save(interval);
        verify(repo, never()).save(overdue);
    }

    @Test
    @DisplayName("Исчерпанное расписание (next бросает исключение) снимается")
    void disablesExhaustedSchedule() {
        JobEntity exhausted = job("exhausted", "0 0 29 2 1", Instant.parse("2026-10-09T08:00:00Z"));
        JobRepository repo = mock(JobRepository.class);
        when(repo.findScheduledJobs()).thenReturn(List.of(exhausted));

        CronSchedule schedule = mock(CronSchedule.class);
        when(schedule.isCalendar()).thenReturn(true);
        when(schedule.next(any(), any()))
                .thenThrow(new InvalidCronExpressionException("0 0 29 2 1", "выражение больше не срабатывает"));
        int changed;
        try (var parse = mockStatic(CronSchedule.class)) {
            parse.when(() -> CronSchedule.parse("0 0 29 2 1")).thenReturn(schedule);
            changed = new ScheduleReconciler(repo, null, null, null).reconcile(NOW);
        }

        assertEquals(1, changed);
        assertFalse(exhausted.isScheduled());
        assertEquals("STOPPED", exhausted.getStatus());
        assertTrue(exhausted.getMessage().contains("0 0 29 2 1"), exhausted.getMessage());
        verify(repo).save(exhausted);
    }

    @Test
    @DisplayName("Сбой сверки при старте не валит запуск приложения")
    void startupFailureDoesNotPropagate() {
        JobRepository repo = mock(JobRepository.class);
        when(repo.findScheduledJobs()).thenThrow(new IllegalStateException("db down"));

        assertDoesNotThrow(() -> new ScheduleReconciler(repo, null, null, null).reconcileOnStartup());
    }
}
