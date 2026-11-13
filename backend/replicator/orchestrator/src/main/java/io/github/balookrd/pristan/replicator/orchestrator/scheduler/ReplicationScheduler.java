package io.github.balookrd.pristan.replicator.orchestrator.scheduler;

import io.github.balookrd.pristan.replicator.orchestrator.config.ReplicatorProperties;
import io.github.balookrd.pristan.replicator.orchestrator.entity.JobEntity;
import io.github.balookrd.pristan.replicator.orchestrator.entity.JobRunEntity;
import io.github.balookrd.pristan.replicator.orchestrator.repository.JobRepository;
import io.github.balookrd.pristan.replicator.orchestrator.repository.JobRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

@Component
public class ReplicationScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReplicationScheduler.class);
    static final String LOCK_NAME = "cron_scheduler_lock";

    private final JobRepository jobRepository;
    private final JobRunRepository jobRunRepository;
    private final io.github.balookrd.pristan.replicator.orchestrator.service.DistributedLockService lockService;
    private final org.springframework.transaction.support.TransactionTemplate txTemplate;
    private final ZoneId zone;

    @org.springframework.beans.factory.annotation.Autowired
    public ReplicationScheduler(JobRepository jobRepository, JobRunRepository jobRunRepository,
                                io.github.balookrd.pristan.replicator.orchestrator.service.DistributedLockService lockService,
                                @org.springframework.beans.factory.annotation.Autowired(required = false)
                                org.springframework.transaction.PlatformTransactionManager transactionManager,
                                ReplicatorProperties properties) {
        this.jobRepository = jobRepository;
        this.jobRunRepository = jobRunRepository;
        this.lockService = lockService;
        this.txTemplate = transactionManager != null ? new org.springframework.transaction.support.TransactionTemplate(transactionManager) : null;
        // Fail fast on a misspelled zone instead of on the first tick
        this.zone = properties != null ? properties.getScheduler().getZoneId() : ZoneOffset.UTC;
    }

    public ReplicationScheduler(JobRepository jobRepository, JobRunRepository jobRunRepository,
                                io.github.balookrd.pristan.replicator.orchestrator.service.DistributedLockService lockService,
                                org.springframework.transaction.PlatformTransactionManager transactionManager) {
        this(jobRepository, jobRunRepository, lockService, transactionManager, null);
    }

    public ReplicationScheduler(JobRepository jobRepository, JobRunRepository jobRunRepository) {
        this(jobRepository, jobRunRepository, null, null, null);
    }

    @Scheduled(fixedDelay = 5000)
    public void schedulerTick() {
        if (lockService != null) {
            lockService.runWithLock(LOCK_NAME, Duration.ofSeconds(8), this::executeScheduledRuns);
        } else {
            executeScheduledRuns();
        }
    }

    @Transactional
    public void executeScheduledRuns() {
        if (txTemplate != null) {
            txTemplate.executeWithoutResult(status -> doExecuteScheduledRuns());
        } else {
            doExecuteScheduledRuns();
        }
    }

    private void doExecuteScheduledRuns() {
        Instant now = Instant.now();
        List<JobEntity> dueJobs = jobRepository.findByIsScheduledTrueAndNextRunAtLessThanEqual(now);

        for (JobEntity job : dueJobs) {
            if ("RUNNING".equalsIgnoreCase(job.getStatus())) {
                continue;
            }

            CronSchedule schedule;
            try {
                schedule = CronSchedule.parse(job.getCronExpression());
            } catch (InvalidCronExpressionException e) {
                disableInvalidSchedule(job, e);
                jobRepository.save(job);
                continue;
            }

            Instant nextRunAt;
            try {
                nextRunAt = schedule.next(now, zone);
            } catch (InvalidCronExpressionException e) {
                disableInvalidSchedule(job, e);
                jobRepository.save(job);
                continue;
            }

            int nextRunNum = jobRunRepository.findMaxRunNumberByJobId(job.getId()) + 1;

            String runId = UUID.randomUUID().toString();
            job.setStatus("QUEUED");
            job.setActiveRunId(runId);
            job.setLastRunAt(now);
            job.setNextRunAt(nextRunAt);
            job.setStartedAt(now);
            job.setCopiedBytes(0);
            jobRepository.saveAndFlush(job);

            JobRunEntity run = new JobRunEntity();
            run.setId(runId);
            run.setJobId(job.getId());
            run.setRunNumber(nextRunNum);
            run.setTriggerType("SCHEDULED");
            run.setStatus("QUEUED");
            run.setTotalBytes(job.getTotalBytes());
            run.setCopiedBytes(0);
            run.setStartedAt(now);
            run.setMessage("Задача поставлена в очередь планировщиком");
            run.setTriggeredBy("scheduler");
            run.setCreatedAt(now);

            jobRunRepository.save(run);
            jobRepository.save(job);

            log.info("Scheduler triggered run #{} for job '{}' (run_id={})", nextRunNum, job.getId(), runId);
            pruneJobRuns(job.getId(), job.getHistoryRetentionRuns());
        }
    }

    /** Prefix of the job message written when the scheduler turns off a schedule it cannot evaluate. */
    public static final String SCHEDULE_DISABLED_PREFIX = "Расписание отключено: ";

    /** Turns off a schedule that cannot be evaluated; the job stays visible with the reason. */
    static void disableInvalidSchedule(JobEntity job, InvalidCronExpressionException e) {
        job.setScheduled(false);
        if ("SCHEDULED".equalsIgnoreCase(job.getStatus())) {
            job.setStatus("STOPPED");
        }
        job.setMessage(SCHEDULE_DISABLED_PREFIX + e.getMessage());
        log.warn("Schedule disabled for job '{}': {}", job.getId(), e.getMessage());
    }

    public void pruneJobRuns(String jobId, int retentionLimit) {
        if (retentionLimit <= 0) return;
        List<JobRunEntity> runs = jobRunRepository.findByJobIdOrderByRunNumberDesc(jobId);
        if (runs.size() > retentionLimit) {
            for (int i = retentionLimit; i < runs.size(); i++) {
                jobRunRepository.delete(runs.get(i));
            }
        }
    }
}
