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
        if (lockService != null) {
            lockService.runWithLock(ReplicationScheduler.LOCK_NAME, LOCK_LEASE, () -> runInTransaction(Instant.now()));
        } else {
            runInTransaction(Instant.now());
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
            CronSchedule schedule;
            try {
                schedule = CronSchedule.parse(job.getCronExpression());
            } catch (InvalidCronExpressionException e) {
                ReplicationScheduler.disableInvalidSchedule(job, e);
                jobRepository.save(job);
                changed++;
                continue;
            }
            Instant planned = job.getNextRunAt();
            if (planned == null || (schedule.isCalendar() && planned.isAfter(now))) {
                Instant next;
                try {
                    next = schedule.next(now, zone);
                } catch (InvalidCronExpressionException e) {
                    ReplicationScheduler.disableInvalidSchedule(job, e);
                    jobRepository.save(job);
                    changed++;
                    continue;
                }
                if (!next.equals(planned)) {
                    job.setNextRunAt(next);
                    jobRepository.save(job);
                    changed++;
                }
            }
        }
        if (changed > 0) {
            log.info("Schedule reconciliation updated {} job(s)", changed);
        }
        return changed;
    }
}
