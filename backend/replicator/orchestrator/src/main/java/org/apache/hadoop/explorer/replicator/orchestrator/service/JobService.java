package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.model.UpdateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobRunEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.scheduler.ReplicationScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final JobRepository jobRepository;
    private final JobRunRepository jobRunRepository;
    private final TaskRepository taskRepository;
    private final org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties properties;

    @Autowired
    public JobService(JobRepository jobRepository, JobRunRepository jobRunRepository, TaskRepository taskRepository,
                      org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties properties) {
        this.jobRepository = jobRepository;
        this.jobRunRepository = jobRunRepository;
        this.taskRepository = taskRepository;
        this.properties = properties != null ? properties : new org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties();
    }

    public JobService(JobRepository jobRepository, JobRunRepository jobRunRepository, TaskRepository taskRepository) {
        this(jobRepository, jobRunRepository, taskRepository, new org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties());
    }

    @PostConstruct
    @Transactional
    public void initializeExistingJobs() {
        // Автоматическое заполнение истории запусков для любых существующих задач без runs
        for (JobEntity existing : jobRepository.findAll()) {
            if (jobRunRepository.countByJobId(existing.getId()) == 0) {
                createInitialRunForJob(existing);
            }
        }
    }

    private void createInitialRunForJob(JobEntity job) {
        jobRepository.saveAndFlush(job);
        String runId = UUID.randomUUID().toString();
        JobRunEntity run = new JobRunEntity();
        run.setId(runId);
        run.setJobId(job.getId());
        run.setRunNumber(1);
        run.setTriggerType(job.isScheduled() ? "SCHEDULED" : "MANUAL");
        run.setStatus(job.getStatus());
        run.setTotalBytes(job.getTotalBytes());
        run.setCopiedBytes(job.getCopiedBytes());
        run.setStartedAt(job.getStartedAt() != null ? job.getStartedAt() : job.getCreatedAt());
        run.setCompletedAt(job.getCompletedAt() != null ? job.getCompletedAt() : ("COMPLETED".equalsIgnoreCase(job.getStatus()) ? Instant.now() : null));
        run.calculateMetrics();
        run.setMessage(job.getMessage() != null ? job.getMessage() : "Запуск репликации");
        run.setTriggeredBy(job.getCreatedBy() != null ? job.getCreatedBy() : "system");
        run.setCreatedAt(job.getCreatedAt() != null ? job.getCreatedAt() : Instant.now());
        jobRunRepository.save(run);

        job.setActiveRunId(runId);
        jobRepository.save(job);
        log.info("Создана начальная запись запуска для задачи {}", job.getId());
    }

    @Transactional
    public JobResponse createJob(CreateJobRequest req, String username) {
        if (req.sourceClusterId() == null || req.sourceClusterId().isBlank()) {
            throw new IllegalArgumentException("Параметр sourceClusterId обязателен для создания задачи репликации");
        }
        if (req.targetClusterId() == null || req.targetClusterId().isBlank()) {
            throw new IllegalArgumentException("Параметр targetClusterId обязателен для создания задачи репликации");
        }
        String id = UUID.randomUUID().toString();
        JobEntity entity = new JobEntity();
        entity.setId(id);
        entity.setSourcePath(req.sourcePath());
        entity.setTargetPath(req.targetPath());
        entity.setSourceClusterId(req.sourceClusterId());
        entity.setTargetClusterId(req.targetClusterId());
        entity.setTotalBytes(req.totalBytes());
        entity.setExecutionPrincipal(req.executionPrincipal());
        entity.setRunAsServiceAccount(req.runAsServiceAccount());
        entity.setCreatedBy(username != null ? username : "system");

        String syncMode = req.syncMode() != null ? req.syncMode().toUpperCase() :
            (Boolean.TRUE.equals(req.isScheduled()) ? "SCHEDULED" : "MANUAL");

        if ("STREAMING_INOTIFY".equalsIgnoreCase(syncMode)) {
            if (!properties.getStreaming().isEnabled()) {
                throw new IllegalArgumentException(
                    "Потоковая репликация через HDFS Inotify отключена в конфигурации оркестратора " +
                    "(hadoop.replicator.streaming.enabled=false). Обратитесь к администратору платформы."
                );
            }
            entity.setSyncMode("STREAMING_INOTIFY");
            entity.setScheduled(false);
            entity.setStatus("STREAMING");
            entity.setMessage("Потоковая репликация HDFS Inotify активна");
            entity.setJobType(req.jobType() != null ? req.jobType() : "STANDARD");
            entity.setParentJobId(req.parentJobId());
            if (req.historyRetentionRuns() != null) {
                entity.setHistoryRetentionRuns(req.historyRetentionRuns());
            }
            jobRepository.save(entity);
        } else if (req.isScheduled()) {
            entity.setSyncMode("SCHEDULED");
            entity.setScheduled(true);
            entity.setCronExpression(req.cronExpression());
            entity.setJobType(req.jobType() != null ? req.jobType() : "STANDARD");
            entity.setParentJobId(req.parentJobId());
            if (req.historyRetentionRuns() != null) {
                entity.setHistoryRetentionRuns(req.historyRetentionRuns());
            }
            entity.setStatus("SCHEDULED");
            // Немедленный первый запуск: планировщик (тик каждые 5 сек) сразу выполнит первый запуск,
            // а затем рассчитает время следующего запуска на основе cronExpression
            entity.setNextRunAt(Instant.now());
            jobRepository.save(entity);
        } else {
            entity.setSyncMode("MANUAL");
            entity.setScheduled(false);
            entity.setJobType(req.jobType() != null ? req.jobType() : "STANDARD");
            entity.setParentJobId(req.parentJobId());
            if (req.historyRetentionRuns() != null) {
                entity.setHistoryRetentionRuns(req.historyRetentionRuns());
            }
            entity.setStatus("QUEUED");
            jobRepository.saveAndFlush(entity);

            String runId = UUID.randomUUID().toString();
            JobRunEntity run = new JobRunEntity();
            run.setId(runId);
            run.setJobId(id);
            run.setRunNumber(1);
            run.setTriggerType("MANUAL");
            run.setStatus("QUEUED");
            run.setTotalBytes(req.totalBytes());
            run.setCopiedBytes(0);
            run.setMessage("Задача поставлена в очередь репликации");
            run.setTriggeredBy(entity.getCreatedBy());
            run.setCreatedAt(Instant.now());
            jobRunRepository.save(run);

            entity.setActiveRunId(runId);
            jobRepository.save(entity);
        }

        log.info("Created replication job: id={}, type={}, syncMode={}, parent={}, src={}, dst={}, author={}",
            id, entity.getJobType(), entity.getSyncMode(), entity.getParentJobId(), req.sourcePath(), req.targetPath(), entity.getCreatedBy());
        return JobResponse.fromEntity(entity, jobRunRepository.countByJobId(id));
    }

    /**
     * Поиск активной или недавно завершенной HDFS-подзадачи для предотвращения дублирования при ретраях CDC событий.
     */
    public Optional<JobResponse> findReusableSubjob(String parentJobId, String sourcePath, String targetPath) {
        if (parentJobId == null || parentJobId.isBlank()) {
            return Optional.empty();
        }
        List<JobEntity> list = jobRepository.findByParentJobIdAndSourcePathAndTargetPath(parentJobId, sourcePath, targetPath);
        if (list == null || list.isEmpty()) {
            return Optional.empty();
        }

        // 1. Приоритет: активная подзадача (QUEUED, ANALYZING, RUNNING)
        for (JobEntity j : list) {
            String s = j.getStatus();
            if ("RUNNING".equalsIgnoreCase(s) || "QUEUED".equalsIgnoreCase(s) || "ANALYZING".equalsIgnoreCase(s)) {
                return Optional.of(JobResponse.fromEntity(j, jobRunRepository.countByJobId(j.getId())));
            }
        }

        // 2. Недавно завершенная подзадача (за последние 10 минут)
        Instant cutoff = Instant.now().minusSeconds(600);
        for (JobEntity j : list) {
            if ("COMPLETED".equalsIgnoreCase(j.getStatus())
                    && (j.getCompletedAt() == null || j.getCompletedAt().isAfter(cutoff))) {
                return Optional.of(JobResponse.fromEntity(j, jobRunRepository.countByJobId(j.getId())));
            }
        }

        return Optional.empty();
    }

    public List<JobResponse> listJobs(String status, String username, boolean isAdmin, boolean isReader) {
        return listJobs(status, username, isAdmin, isReader, false);
    }

    public List<JobResponse> listJobs(String status, String username, boolean isAdmin, boolean isReader, boolean includeSubjobs) {
        List<JobEntity> list = (status != null && !status.isBlank())
            ? jobRepository.findByStatus(status.toUpperCase())
            : jobRepository.findAll();

        Stream<JobEntity> stream = (isAdmin || isReader || username == null)
            ? list.stream()
            : list.stream().filter(j -> matchesJobOwner(j, username));

        // Исключаем автоматические саб-джобы метастора из регламентного раздела HDFS (если не запрошено явно агентом)
        if (!includeSubjobs) {
            stream = stream.filter(j -> !"HMS_SUBJOB".equalsIgnoreCase(j.getJobType()));
        }

        List<JobEntity> filteredJobs = stream.toList();
        if (filteredJobs.isEmpty()) {
            return List.of();
        }

        List<String> jobIds = filteredJobs.stream().map(JobEntity::getId).toList();
        Map<String, Integer> runCounts = jobRunRepository.countByJobIdIn(jobIds).stream()
            .collect(Collectors.toMap(JobRunRepository.JobRunCount::getJobId, c -> (int) c.getRunCount()));

        return filteredJobs.stream()
            .map(j -> JobResponse.fromEntity(j, runCounts.getOrDefault(j.getId(), 0)))
            .toList();
    }

    public List<JobResponse> listSubjobsByParentId(String parentJobId) {
        if (parentJobId == null || parentJobId.isBlank()) {
            return List.of();
        }
        List<JobEntity> subjobs = jobRepository.findByParentJobId(parentJobId);
        if (subjobs.isEmpty()) {
            return List.of();
        }

        List<String> jobIds = subjobs.stream().map(JobEntity::getId).toList();
        Map<String, Integer> runCounts = jobRunRepository.countByJobIdIn(jobIds).stream()
            .collect(Collectors.toMap(JobRunRepository.JobRunCount::getJobId, c -> (int) c.getRunCount()));

        return subjobs.stream()
            .map(j -> JobResponse.fromEntity(j, runCounts.getOrDefault(j.getId(), 0)))
            .toList();
    }

    public Map<String, List<JobResponse>> listSubjobsByParentIds(java.util.Collection<String> parentJobIds) {
        if (parentJobIds == null || parentJobIds.isEmpty()) {
            return Map.of();
        }
        List<JobEntity> subjobs = jobRepository.findByParentJobIdIn(parentJobIds);
        if (subjobs.isEmpty()) {
            return Map.of();
        }

        List<String> jobIds = subjobs.stream().map(JobEntity::getId).toList();
        Map<String, Integer> runCounts = jobRunRepository.countByJobIdIn(jobIds).stream()
            .collect(Collectors.toMap(JobRunRepository.JobRunCount::getJobId, c -> (int) c.getRunCount()));

        return subjobs.stream()
            .map(j -> JobResponse.fromEntity(j, runCounts.getOrDefault(j.getId(), 0)))
            .filter(j -> j.parentJobId() != null)
            .collect(Collectors.groupingBy(JobResponse::parentJobId));
    }

    @Transactional
    public void deleteSubjobsByParentId(String parentJobId) {
        if (parentJobId == null || parentJobId.isBlank()) {
            return;
        }
        List<JobEntity> subjobs = jobRepository.findByParentJobId(parentJobId);
        for (JobEntity sub : subjobs) {
            deleteJob(sub.getId());
        }
    }

    public List<JobResponse> listJobs(String status, String username, boolean isAdmin) {
        return listJobs(status, username, isAdmin, false);
    }

    public List<JobResponse> listJobs(String status) {
        return listJobs(status, null, true, true);
    }

    public static boolean matchesJobOwner(JobEntity job, String username) {
        if (job == null || username == null) return true;
        String createdBy = job.getCreatedBy();
        String principal = job.getExecutionPrincipal();

        // Извлекаем имена без суффикса Kerberos realm (@REALM.LOCAL)
        String author = createdBy != null ? (createdBy.contains("@") ? createdBy.substring(0, createdBy.indexOf('@')) : createdBy) : "";
        String user = username.contains("@") ? username.substring(0, username.indexOf('@')) : username;

        // Системные задачи доступны для платформы
        if (author.isBlank() || "system".equalsIgnoreCase(author)) {
            return true;
        }

        if (author.equalsIgnoreCase(user)) {
            return true;
        }

        if (principal != null && !principal.isBlank()) {
            String princUser = principal.contains("@") ? principal.substring(0, principal.indexOf('@')) : principal;
            if (princUser.equalsIgnoreCase(user)) {
                return true;
            }
        }

        return false;
    }

    public Optional<JobEntity> getJobEntity(String id) {
        return jobRepository.findById(id);
    }

    public Optional<JobResponse> getJob(String id) {
        return jobRepository.findById(id)
            .map(j -> JobResponse.fromEntity(j, jobRunRepository.countByJobId(j.getId())));
    }

    @Transactional
    public Optional<JobResponse> updateJob(String id, CreateJobRequest req) {
        return jobRepository.findById(id).map(job -> {
            if (req.sourcePath() != null) job.setSourcePath(req.sourcePath());
            if (req.targetPath() != null) job.setTargetPath(req.targetPath());
            if (req.sourceClusterId() != null) job.setSourceClusterId(req.sourceClusterId());
            if (req.targetClusterId() != null) job.setTargetClusterId(req.targetClusterId());
            if (req.executionPrincipal() != null && !req.executionPrincipal().isBlank()) {
                job.setExecutionPrincipal(req.executionPrincipal());
            }
            if (req.isScheduled() != null) {
                boolean wasScheduled = job.isScheduled();
                job.setScheduled(req.isScheduled());
                if (req.isScheduled() && (!wasScheduled || job.getNextRunAt() == null)) {
                    job.setNextRunAt(Instant.now());
                }
            }
            if (req.cronExpression() != null) job.setCronExpression(req.cronExpression());
            if (req.historyRetentionRuns() != null) job.setHistoryRetentionRuns(req.historyRetentionRuns());
            jobRepository.save(job);
            return JobResponse.fromEntity(job, jobRunRepository.countByJobId(job.getId()));
        });
    }

    @Transactional
    public Optional<JobResponse> updateJobProgress(String id, UpdateJobRequest req) {
        if (req.getStatus() != null && "ANALYZING".equalsIgnoreCase(req.getStatus().trim())) {
            int updated = jobRepository.claimJobForAnalysis(
                    id,
                    req.getMessage() != null ? req.getMessage() : "Анализ каталога и вычисление diff деревьев",
                    Instant.now()
            );
            if (updated == 0) {
                log.info("Задание '{}' уже переведено в обработку другим агентом (статус не QUEUED)", id);
                return Optional.empty();
            }
        }

        return jobRepository.findById(id).map(job -> {
            if (req.getStatus() != null && !req.getStatus().isBlank()) {
                job.setStatus(req.getStatus().toUpperCase());
            }
            if (req.getCopiedBytes() != null) {
                job.setCopiedBytes(req.getCopiedBytes());
            }
            if (req.getTotalBytes() != null) {
                job.setTotalBytes(req.getTotalBytes());
            }
            if (req.getMessage() != null) {
                job.setMessage(req.getMessage());
            }
            if (req.getTotalObjects() != null) {
                job.setTotalObjects(req.getTotalObjects());
            }
            if (req.getTransferredObjects() != null) {
                job.setTransferredObjects(req.getTransferredObjects());
            }
            if (req.getSkippedObjects() != null) {
                job.setSkippedObjects(req.getSkippedObjects());
            }
            if (req.getFailedObjects() != null) {
                job.setFailedObjects(req.getFailedObjects());
            }

            Instant now = Instant.now();
            if ("RUNNING".equalsIgnoreCase(job.getStatus()) && job.getStartedAt() == null) {
                job.setStartedAt(now);
            }
            if ("COMPLETED".equalsIgnoreCase(job.getStatus()) || "FAILED".equalsIgnoreCase(job.getStatus())) {
                job.setCompletedAt(now);
            }

            // Находим или создаем текущий run
            JobRunEntity run = null;
            if (job.getActiveRunId() != null) {
                run = jobRunRepository.findById(job.getActiveRunId()).orElse(null);
            }
            if (run == null) {
                int nextRunNum = jobRunRepository.findMaxRunNumberByJobId(job.getId()) + 1;
                String runId = UUID.randomUUID().toString();
                run = new JobRunEntity();
                run.setId(runId);
                run.setJobId(job.getId());
                run.setRunNumber(nextRunNum);
                run.setTriggerType(job.isScheduled() ? "SCHEDULED" : "MANUAL");
                run.setTotalBytes(job.getTotalBytes());
                run.setTriggeredBy(job.getCreatedBy());
                job.setActiveRunId(runId);
            }

            run.setStatus(job.getStatus());
            run.setCopiedBytes(job.getCopiedBytes());
            run.setTotalBytes(job.getTotalBytes());
            run.setTotalObjects(job.getTotalObjects());
            run.setTransferredObjects(job.getTransferredObjects());
            run.setSkippedObjects(job.getSkippedObjects());
            run.setFailedObjects(job.getFailedObjects());
            run.setMessage(job.getMessage());
            if ("RUNNING".equalsIgnoreCase(job.getStatus()) && run.getStartedAt() == null) {
                run.setStartedAt(job.getStartedAt() != null ? job.getStartedAt() : now);
            }
            if ("COMPLETED".equalsIgnoreCase(job.getStatus()) || "FAILED".equalsIgnoreCase(job.getStatus())) {
                if (run.getStartedAt() == null) {
                    run.setStartedAt(job.getStartedAt() != null ? job.getStartedAt() : now.minusSeconds(1));
                }
                run.setCompletedAt(job.getCompletedAt() != null ? job.getCompletedAt() : now);
                run.calculateMetrics();
            }
            jobRunRepository.save(run);

            jobRepository.save(job);
            int runsCount = jobRunRepository.countByJobId(job.getId());
            return JobResponse.fromEntity(job, runsCount);
        });
    }

    @Transactional
    public Optional<JobResponse> startJob(String id) {
        return jobRepository.findById(id).map(job -> {
            Instant now = Instant.now();
            job.setStatus("QUEUED");
            job.setStartedAt(now);
            job.setCompletedAt(null);
            job.setCopiedBytes(0);
            jobRepository.saveAndFlush(job);

            String runId = UUID.randomUUID().toString();
            int nextRunNum = jobRunRepository.countByJobId(job.getId()) + 1;

            JobRunEntity run = new JobRunEntity();
            run.setId(runId);
            run.setJobId(job.getId());
            run.setRunNumber(nextRunNum);
            run.setTriggerType("MANUAL");
            run.setStatus("QUEUED");
            run.setTotalBytes(job.getTotalBytes());
            run.setCopiedBytes(0);
            run.setStartedAt(now);
            run.setMessage("Задача поставлена в очередь репликации (ручной запуск)");
            run.setTriggeredBy(job.getCreatedBy());
            run.setCreatedAt(now);
            jobRunRepository.save(run);

            job.setActiveRunId(runId);
            jobRepository.save(job);
            int runsCount = jobRunRepository.countByJobId(job.getId());
            return JobResponse.fromEntity(job, runsCount);
        });
    }

    @Transactional
    public Optional<JobResponse> stopJob(String id) {
        return jobRepository.findById(id).map(job -> {
            job.setStatus("STOPPED");
            Instant now = Instant.now();
            job.setCompletedAt(now);
            if (job.getActiveRunId() != null) {
                jobRunRepository.findById(job.getActiveRunId()).ifPresent(run -> {
                    run.setStatus("STOPPED");
                    run.setCompletedAt(now);
                    run.setMessage("Остановлено оператором");
                    run.calculateMetrics();
                    jobRunRepository.save(run);
                });
            }
            job.setActiveRunId(null);
            jobRepository.save(job);
            int runsCount = jobRunRepository.countByJobId(job.getId());
            return JobResponse.fromEntity(job, runsCount);
        });
    }

    @Transactional
    public Optional<JobResponse> cancelJob(String id) {
        return jobRepository.findById(id).map(job -> {
            job.setStatus("CANCELLED");
            Instant now = Instant.now();
            job.setCompletedAt(now);
            if (job.getActiveRunId() != null) {
                jobRunRepository.findById(job.getActiveRunId()).ifPresent(run -> {
                    run.setStatus("CANCELLED");
                    run.setCompletedAt(now);
                    run.setMessage("Отменено оператором");
                    run.calculateMetrics();
                    jobRunRepository.save(run);
                });
            }
            job.setActiveRunId(null);
            jobRepository.save(job);
            int runsCount = jobRunRepository.countByJobId(job.getId());
            return JobResponse.fromEntity(job, runsCount);
        });
    }

    @Transactional
    public boolean deleteJob(String id) {
        if (jobRepository.existsById(id)) {
            taskRepository.deleteByJobId(id);
            jobRunRepository.deleteByJobId(id);
            jobRepository.deleteById(id);
            return true;
        }
        return false;
    }

    public List<JobRunEntity> getJobRuns(String id) {
        return jobRunRepository.findByJobIdOrderByRunNumberDesc(id);
    }
}
