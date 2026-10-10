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
import java.util.Optional;
import java.util.UUID;
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
    public void seedDemoJobsIfEmpty() {
        Instant now = Instant.now();

        if (!jobRepository.existsById("job-writer-sales")) {
            // 1. Задача инженера данных: writer_user
            JobEntity j1 = new JobEntity();
            j1.setId("job-writer-sales");
            j1.setSourcePath("/tmp/data/prod_sales_dc1.csv");
            j1.setTargetPath("/tmp/data/replicated_prod_sales_dc2.csv");
            j1.setSourceClusterId("dc1");
            j1.setTargetClusterId("dc2");
            j1.setTotalBytes(5242880L);
            j1.setCopiedBytes(5242880L);
            j1.setStatus("COMPLETED");
            j1.setCreatedBy("writer_user");
            j1.setExecutionPrincipal("writer_user@REALM.LOCAL");
            j1.setMessage("Репликация витрин продаж успешно завершена");
            j1.setStartedAt(now.minusSeconds(120));
            j1.setCompletedAt(now.minusSeconds(110));
            jobRepository.saveAndFlush(j1);

            JobRunEntity r1 = new JobRunEntity();
            r1.setId("run-writer-sales-1");
            r1.setJobId("job-writer-sales");
            r1.setRunNumber(1);
            r1.setTriggerType("MANUAL");
            r1.setStatus("COMPLETED");
            r1.setTotalBytes(5242880L);
            r1.setCopiedBytes(5242880L);
            r1.setStartedAt(j1.getStartedAt());
            r1.setCompletedAt(j1.getCompletedAt());
            r1.calculateMetrics();
            r1.setMessage("Репликация витрин продаж успешно завершена");
            r1.setTriggeredBy("writer_user");
            jobRunRepository.save(r1);
            j1.setActiveRunId(r1.getId());
            jobRepository.save(j1);

            // 2. Задача инженера данных: de_user
            JobEntity j2 = new JobEntity();
            j2.setId("job-de-analytics");
            j2.setSourcePath("/tmp/data/analytics_report_dc2.parquet");
            j2.setTargetPath("/tmp/data/restored_analytics_dc1.parquet");
            j2.setSourceClusterId("dc2");
            j2.setTargetClusterId("dc1");
            j2.setTotalBytes(3145728L);
            j2.setCopiedBytes(3145728L);
            j2.setStatus("COMPLETED");
            j2.setCreatedBy("de_user");
            j2.setExecutionPrincipal("de_user@REALM.LOCAL");
            j2.setMessage("Синхронизация аналитического отчета завершена");
            j2.setStartedAt(now.minusSeconds(90));
            j2.setCompletedAt(now.minusSeconds(85));
            jobRepository.saveAndFlush(j2);

            JobRunEntity r2 = new JobRunEntity();
            r2.setId("run-de-analytics-1");
            r2.setJobId("job-de-analytics");
            r2.setRunNumber(1);
            r2.setTriggerType("MANUAL");
            r2.setStatus("COMPLETED");
            r2.setTotalBytes(3145728L);
            r2.setCopiedBytes(3145728L);
            r2.setStartedAt(j2.getStartedAt());
            r2.setCompletedAt(j2.getCompletedAt());
            r2.calculateMetrics();
            r2.setMessage("Синхронизация аналитического отчета завершена");
            r2.setTriggeredBy("de_user");
            jobRunRepository.save(r2);
            j2.setActiveRunId(r2.getId());
            jobRepository.save(j2);

            // 3. Задача наблюдателя / аналитика: reader_user
            JobEntity j3 = new JobEntity();
            j3.setId("job-reader-reports");
            j3.setSourcePath("/data/analytics/reports/2026-q4.parquet");
            j3.setTargetPath("/backup/analytics/reports/2026-q4.parquet");
            j3.setSourceClusterId("dc1");
            j3.setTargetClusterId("dc2");
            j3.setTotalBytes(2097152L);
            j3.setCopiedBytes(2097152L);
            j3.setStatus("COMPLETED");
            j3.setCreatedBy("reader_user");
            j3.setExecutionPrincipal("reader_user@REALM.LOCAL");
            j3.setMessage("Аналитическая выгрузка скопирована в режиме аудита");
            j3.setStartedAt(now.minusSeconds(60));
            j3.setCompletedAt(now.minusSeconds(55));
            jobRepository.saveAndFlush(j3);

            JobRunEntity r3 = new JobRunEntity();
            r3.setId("run-reader-reports-1");
            r3.setJobId("job-reader-reports");
            r3.setRunNumber(1);
            r3.setTriggerType("MANUAL");
            r3.setStatus("COMPLETED");
            r3.setTotalBytes(2097152L);
            r3.setCopiedBytes(2097152L);
            r3.setStartedAt(j3.getStartedAt());
            r3.setCompletedAt(j3.getCompletedAt());
            r3.calculateMetrics();
            r3.setMessage("Аналитическая выгрузка скопирована в режиме аудита");
            r3.setTriggeredBy("reader_user");
            jobRunRepository.save(r3);
            j3.setActiveRunId(r3.getId());
            jobRepository.save(j3);

            // 4. Задача администратора платформы: admin_user
            JobEntity j4 = new JobEntity();
            j4.setId("job-admin-backup");
            j4.setSourcePath("/data/production/events/2026-10");
            j4.setTargetPath("/backup/mirror/events/2026-10");
            j4.setSourceClusterId("dc2");
            j4.setTargetClusterId("dc1");
            j4.setTotalBytes(2097152L);
            j4.setCopiedBytes(2097152L);
            j4.setStatus("COMPLETED");
            j4.setCreatedBy("admin_user");
            j4.setExecutionPrincipal("admin_user@REALM.LOCAL");
            j4.setMessage("Резервное копирование событий завершено");
            j4.setStartedAt(now.minusSeconds(40));
            j4.setCompletedAt(now.minusSeconds(30));
            jobRepository.saveAndFlush(j4);

            JobRunEntity r4 = new JobRunEntity();
            r4.setId("run-admin-backup-1");
            r4.setJobId("job-admin-backup");
            r4.setRunNumber(1);
            r4.setTriggerType("MANUAL");
            r4.setStatus("COMPLETED");
            r4.setTotalBytes(2097152L);
            r4.setCopiedBytes(2097152L);
            r4.setStartedAt(j4.getStartedAt());
            r4.setCompletedAt(j4.getCompletedAt());
            r4.calculateMetrics();
            r4.setMessage("Резервное копирование событий завершено");
            r4.setTriggeredBy("admin_user");
            jobRunRepository.save(r4);
            j4.setActiveRunId(r4.getId());
            jobRepository.save(j4);

            // 5. Периодическая системная задача по расписанию: system_operator
            JobEntity j5 = new JobEntity();
            j5.setId("job-scheduled-sync");
            j5.setSourcePath("/data/warehouse/sync_delta");
            j5.setTargetPath("/backup/warehouse/sync_delta");
            j5.setSourceClusterId("dc1");
            j5.setTargetClusterId("dc2");
            j5.setTotalBytes(1048576L);
            j5.setCopiedBytes(0L);
            j5.setStatus("SCHEDULED");
            j5.setScheduled(true);
            j5.setCronExpression("@every_5m");
            j5.setNextRunAt(now.plusSeconds(300));
            j5.setCreatedBy("system_operator");
            j5.setExecutionPrincipal("hdfs-replicator@REALM.LOCAL");
            j5.setRunAsServiceAccount(true);
            j5.setMessage("Ожидание очередного запуска по расписанию (@every_5m)");
            jobRepository.saveAndFlush(j5);

            // Исторический запуск #1 (10 минут назад)
            JobRunEntity r5_1 = new JobRunEntity();
            r5_1.setId("run-scheduled-sync-1");
            r5_1.setJobId("job-scheduled-sync");
            r5_1.setRunNumber(1);
            r5_1.setTriggerType("SCHEDULED");
            r5_1.setStatus("COMPLETED");
            r5_1.setTotalBytes(1048576L);
            r5_1.setCopiedBytes(1048576L);
            r5_1.setStartedAt(now.minusSeconds(600));
            r5_1.setCompletedAt(now.minusSeconds(595));
            r5_1.calculateMetrics();
            r5_1.setMessage("Синхронизация дельты успешно завершена (цикл #1)");
            r5_1.setTriggeredBy("scheduler");
            jobRunRepository.save(r5_1);

            // Исторический запуск #2 (5 минут назад)
            JobRunEntity r5_2 = new JobRunEntity();
            r5_2.setId("run-scheduled-sync-2");
            r5_2.setJobId("job-scheduled-sync");
            r5_2.setRunNumber(2);
            r5_2.setTriggerType("SCHEDULED");
            r5_2.setStatus("COMPLETED");
            r5_2.setTotalBytes(1048576L);
            r5_2.setCopiedBytes(1048576L);
            r5_2.setStartedAt(now.minusSeconds(300));
            r5_2.setCompletedAt(now.minusSeconds(296));
            r5_2.calculateMetrics();
            r5_2.setMessage("Синхронизация дельты успешно завершена (цикл #2)");
            r5_2.setTriggeredBy("scheduler");
            jobRunRepository.save(r5_2);

            log.info("Сидирование демо-задач репликации с историей запусков успешно завершено");
        }

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
        entity.setCreatedBy(username != null ? username : "system_operator");

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
            entity.setNextRunAt(ReplicationScheduler.computeNextRun(req.cronExpression(), Instant.now()));
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

        return stream
            .map(j -> JobResponse.fromEntity(j, jobRunRepository.countByJobId(j.getId())))
            .toList();
    }

    public List<JobResponse> listSubjobsByParentId(String parentJobId) {
        if (parentJobId == null || parentJobId.isBlank()) {
            return List.of();
        }
        return jobRepository.findByParentJobId(parentJobId).stream()
            .map(j -> JobResponse.fromEntity(j, jobRunRepository.countByJobId(j.getId())))
            .toList();
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

        // Системные и административные задачи видны всем пользователям платформы
        if (author.isBlank()
                || "system_operator".equalsIgnoreCase(author)
                || "demo-admin".equalsIgnoreCase(author)
                || "admin_user".equalsIgnoreCase(author)) {
            return true;
        }

        if (author.equalsIgnoreCase(user)) {
            return true;
        }

        if (principal != null && principal.toLowerCase().startsWith(user.toLowerCase())) {
            return true;
        }

        // Выравнивание ролей инженеров данных (RW): writer_user и de_user
        boolean isEngineerUser = "writer_user".equalsIgnoreCase(user) || "de_user".equalsIgnoreCase(user)
                || user.toLowerCase().contains("writer") || user.toLowerCase().contains("engineer");
        if (isEngineerUser) {
            String cbLower = author.toLowerCase();
            return cbLower.contains("writer") || cbLower.contains("engineer") || cbLower.contains("de_");
        }

        // Выравнивание ролей наблюдателей (RO): reader_user и analyst_user
        boolean isViewerUser = "reader_user".equalsIgnoreCase(user) || "analyst_user".equalsIgnoreCase(user)
                || user.toLowerCase().contains("reader") || user.toLowerCase().contains("analyst");
        if (isViewerUser) {
            String cbLower = author.toLowerCase();
            return cbLower.contains("reader") || cbLower.contains("analyst");
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
            if (req.isScheduled() != null) job.setScheduled(req.isScheduled());
            if (req.cronExpression() != null) job.setCronExpression(req.cronExpression());
            if (req.historyRetentionRuns() != null) job.setHistoryRetentionRuns(req.historyRetentionRuns());
            jobRepository.save(job);
            return JobResponse.fromEntity(job, jobRunRepository.countByJobId(job.getId()));
        });
    }

    @Transactional
    public Optional<JobResponse> updateJobProgress(String id, UpdateJobRequest req) {
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
                int nextRunNum = jobRunRepository.countByJobId(job.getId()) + 1;
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
