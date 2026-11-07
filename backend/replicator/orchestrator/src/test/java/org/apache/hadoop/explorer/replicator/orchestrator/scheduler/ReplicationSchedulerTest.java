package org.apache.hadoop.explorer.replicator.orchestrator.scheduler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationSchedulerTest {

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

    @Test
    @DisplayName("@daily: следующий запуск в полночь UTC, а не через сутки после запуска")
    void shouldPlanNextRunByCalendar() {
        var jobRepo = org.mockito.Mockito.mock(org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository.class);
        var runRepo = org.mockito.Mockito.mock(org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository.class);
        var scheduler = new ReplicationScheduler(jobRepo, runRepo, null, null);

        var job = new org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity();
        job.setId("cron-daily");
        job.setStatus("SCHEDULED");
        job.setScheduled(true);
        job.setCronExpression("@daily");
        job.setNextRunAt(Instant.now().minusSeconds(10));

        org.mockito.Mockito.when(jobRepo.findByIsScheduledTrueAndNextRunAtLessThanEqual(org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(java.util.List.of(job));
        org.mockito.Mockito.when(runRepo.findMaxRunNumberByJobId("cron-daily")).thenReturn(0);

        Instant before = Instant.now();
        scheduler.executeScheduledRuns();

        assertEquals(java.time.LocalTime.MIDNIGHT, job.getNextRunAt().atZone(java.time.ZoneOffset.UTC).toLocalTime());
        assertTrue(job.getNextRunAt().isAfter(before));
        assertTrue(Duration.between(before, job.getNextRunAt()).compareTo(Duration.ofDays(1)) <= 0);
    }

    @Test
    @DisplayName("Невалидное выражение снимает расписание вместо запуска раз в час")
    void shouldDisableJobWithInvalidCron() {
        var jobRepo = org.mockito.Mockito.mock(org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository.class);
        var runRepo = org.mockito.Mockito.mock(org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository.class);
        var scheduler = new ReplicationScheduler(jobRepo, runRepo, null, null);

        var job = new org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity();
        job.setId("cron-bad");
        job.setStatus("SCHEDULED");
        job.setScheduled(true);
        job.setCronExpression("every tuesday");
        job.setNextRunAt(Instant.now().minusSeconds(10));

        org.mockito.Mockito.when(jobRepo.findByIsScheduledTrueAndNextRunAtLessThanEqual(org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(java.util.List.of(job));

        scheduler.executeScheduledRuns();

        assertFalse(job.isScheduled());
        assertEquals("STOPPED", job.getStatus());
        assertTrue(job.getMessage().startsWith("Расписание отключено"), job.getMessage());
        org.mockito.Mockito.verify(runRepo, org.mockito.Mockito.never()).save(org.mockito.ArgumentMatchers.any());
        org.mockito.Mockito.verify(jobRepo).save(job);
    }

    @Test
    @DisplayName("Неизвестный часовой пояс останавливает создание планировщика")
    void shouldRejectUnknownTimeZone() {
        var props = new org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties();
        props.getScheduler().setTimeZone("Mars/Olympus");
        assertThrows(java.time.DateTimeException.class, () -> new ReplicationScheduler(
                org.mockito.Mockito.mock(org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository.class),
                org.mockito.Mockito.mock(org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository.class),
                null, null, props));
    }
}
