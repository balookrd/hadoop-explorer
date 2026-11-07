package org.apache.hadoop.explorer.replicator.orchestrator.scheduler;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.DistributedLockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Brings stored schedules in line with {@link CronSchedule} on startup.
 *
 * <p>Rows written before calendar evaluation carry next_run_at computed as "previous run plus a
 * fixed interval", and expressions the old parser silently ran hourly. Calendar schedules
 * planned in the future are recomputed, rows without next_run_at are planned, invalid
 * expressions are disabled. Overdue rows are left for the next tick.
 */
@Component
public class ScheduleReconciler {

    private static final Logger log = LoggerFactory.getLogger(ScheduleReconciler.class);
    private static final Duration LOCK_LEASE = Duration.ofSeconds(60);

    private final JobRepository jobRepository;
    private final DistributedLockService lockService;
    private final TransactionTemplate txTemplate;
    private final ZoneId zone;

    @Autowired
    public ScheduleReconciler(JobRepository jobRepository,
                              @Autowired(required = false) DistributedLockService lockService,
                              @Autowired(required = false) PlatformTransactionManager transactionManager,
                              @Autowired(required = false) ReplicatorProperties properties) {
        this.jobRepository = jobRepository;
        this.lockService = lockService;
        this.txTemplate = transactionManager != null ? new TransactionTemplate(transactionManager) : null;
        this.zone = properties != null ? properties.getScheduler().getZoneId() : ZoneOffset.UTC;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void reconcileOnStartup() {
        // Best effort: a failure here must not abort startup, the tick still disables invalid schedules
        try {
            if (lockService != null) {
                // runWithLock silently skips the action when another node holds the lock
                AtomicBoolean ran = new AtomicBoolean(false);
                lockService.runWithLock(ReplicationScheduler.LOCK_NAME, LOCK_LEASE, () -> {
                    ran.set(true);
                    runInTransaction(Instant.now());
                });
                if (!ran.get()) {
                    log.info("Schedule reconciliation skipped: lock '{}' is held by another node",
                        ReplicationScheduler.LOCK_NAME);
                }
            } else {
                runInTransaction(Instant.now());
            }
        } catch (RuntimeException e) {
            log.error("Schedule reconciliation failed, continuing startup", e);
        }
    }

    private void runInTransaction(Instant now) {
        if (txTemplate != null) {
            txTemplate.executeWithoutResult(status -> reconcile(now));
        } else {
            reconcile(now);
        }
    }

    /** Returns the number of jobs whose schedule changed. */
    int reconcile(Instant now) {
        int changed = 0;
        for (JobEntity job : jobRepository.findScheduledJobs()) {
            Instant planned = job.getNextRunAt();
            try {
                CronSchedule schedule = CronSchedule.parse(job.getCronExpression());
                if (planned == null || (schedule.isCalendar() && planned.isAfter(now))) {
                    Instant next = schedule.next(now, zone);
                    if (!next.equals(planned)) {
                        job.setNextRunAt(next);
                        jobRepository.save(job);
                        changed++;
                    }
                }
            } catch (InvalidCronExpressionException e) {
                ReplicationScheduler.disableInvalidSchedule(job, e);
                jobRepository.save(job);
                changed++;
            }
        }
        if (changed > 0) {
            log.info("Schedule reconciliation updated {} job(s)", changed);
        }
        return changed;
    }
}
