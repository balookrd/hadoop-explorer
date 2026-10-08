package org.apache.hadoop.explorer.replicator.orchestrator.scheduler;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobRunEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class ReplicationScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReplicationScheduler.class);
    private static final Pattern INTERVAL_PATTERN = Pattern.compile("^\\*/(\\d+)\\s+\\*\\s+\\*\\s+\\*\\s+\\*$");

    private final JobRepository jobRepository;
    private final JobRunRepository jobRunRepository;

    public ReplicationScheduler(JobRepository jobRepository, JobRunRepository jobRunRepository) {
        this.jobRepository = jobRepository;
        this.jobRunRepository = jobRunRepository;
    }

    public static Instant computeNextRun(String cronExpr, Instant baseTime) {
        Instant now = baseTime != null ? baseTime : Instant.now();
        String expr = (cronExpr != null ? cronExpr.trim().toLowerCase() : "");

        if (expr.equals("@minutely") || expr.equals("* * * * *")) {
            return now.plus(Duration.ofMinutes(1));
        } else if (expr.equals("@every_5m") || expr.equals("*/5 * * * *")) {
            return now.plus(Duration.ofMinutes(5));
        } else if (expr.equals("@every_15m") || expr.equals("*/15 * * * *")) {
            return now.plus(Duration.ofMinutes(15));
        } else if (expr.equals("@hourly") || expr.equals("0 * * * *")) {
            return now.plus(Duration.ofHours(1));
        } else if (expr.equals("@daily") || expr.equals("0 0 * * *") || expr.equals("@midnight")) {
            return now.plus(Duration.ofDays(1));
        } else if (expr.startsWith("every_") && expr.endsWith("s")) {
            try {
                int sec = Integer.parseInt(expr.replace("every_", "").replace("s", ""));
                return now.plus(Duration.ofSeconds(Math.max(5, sec)));
            } catch (NumberFormatException ignored) {}
        }

        Matcher matcher = INTERVAL_PATTERN.matcher(expr);
        if (matcher.matches()) {
            int mins = Integer.parseInt(matcher.group(1));
            return now.plus(Duration.ofMinutes(Math.max(1, mins)));
        }

        return now.plus(Duration.ofHours(1));
    }

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void schedulerTick() {
        Instant now = Instant.now();
        List<JobEntity> dueJobs = jobRepository.findByIsScheduledTrueAndNextRunAtLessThanEqual(now);

        for (JobEntity job : dueJobs) {
            if ("RUNNING".equalsIgnoreCase(job.getStatus())) {
                continue;
            }

            int nextRunNum = jobRunRepository.findByJobIdOrderByRunNumberDesc(job.getId()).stream()
                .mapToInt(JobRunEntity::getRunNumber)
                .max()
                .orElse(0) + 1;

            String runId = UUID.randomUUID().toString();
            job.setStatus("QUEUED");
            job.setActiveRunId(runId);
            job.setLastRunAt(now);
            job.setNextRunAt(computeNextRun(job.getCronExpression(), now));
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
