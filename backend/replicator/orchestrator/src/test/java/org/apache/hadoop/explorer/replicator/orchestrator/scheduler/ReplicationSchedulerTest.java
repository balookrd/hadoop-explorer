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

    @Test
    @DisplayName("Должен инициировать запуск запланированной задачи, переводить в QUEUED и вычислять следующий запуск")
    void shouldTriggerScheduledJobWhenDue() {
        var jobRepo = org.mockito.Mockito.mock(org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository.class);
        var runRepo = org.mockito.Mockito.mock(org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository.class);

        var scheduler = new ReplicationScheduler(jobRepo, runRepo, null, null);

        var job = new org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity();
        job.setId("cron-job-1");
        job.setStatus("SCHEDULED");
        job.setScheduled(true);
        job.setCronExpression("@every_5m");
        job.setNextRunAt(Instant.now().minusSeconds(10));
        job.setTotalBytes(1024L);

        org.mockito.Mockito.when(jobRepo.findByIsScheduledTrueAndNextRunAtLessThanEqual(org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(java.util.List.of(job));
        org.mockito.Mockito.when(runRepo.findMaxRunNumberByJobId("cron-job-1")).thenReturn(0);

        scheduler.executeScheduledRuns();

        assertEquals("QUEUED", job.getStatus());
        assertNotNull(job.getActiveRunId());
        assertNotNull(job.getLastRunAt());
        assertTrue(job.getNextRunAt().isAfter(Instant.now()));

        org.mockito.Mockito.verify(runRepo).save(org.mockito.ArgumentMatchers.argThat(run ->
                "cron-job-1".equals(run.getJobId())
                        && "QUEUED".equals(run.getStatus())
                        && "SCHEDULED".equals(run.getTriggerType())
                        && run.getRunNumber() == 1
        ));
        org.mockito.Mockito.verify(jobRepo, org.mockito.Mockito.atLeastOnce()).save(job);
    }
}
