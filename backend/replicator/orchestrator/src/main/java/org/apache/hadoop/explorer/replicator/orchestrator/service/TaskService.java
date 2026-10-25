package org.apache.hadoop.explorer.replicator.orchestrator.service;

import jakarta.annotation.PreDestroy;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobRunEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

/**
 * Сервис управления распределенным пулом пофайловых задач (Distributed Task Queue).
 * Координирует параллельную репликацию файлов между всеми доступными агентами кластера
 * и обеспечивает автоматический failover при сбое или падении агентов.
 */
@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);

    private final TaskRepository taskRepository;
    private final JobRepository jobRepository;
    private final JobRunRepository jobRunRepository;
    private final AgentRegistry agentRegistry;
    private final TopologyRegistry topologyRegistry;
    private final ReplicatorProperties properties;
    private final org.apache.hadoop.explorer.replicator.orchestrator.service.DistributedLockService lockService;
    private final org.apache.hadoop.explorer.replicator.orchestrator.repository.StreamingLeaseRepository leaseRepository;
    /**
     * Явная граница транзакции для путей, которые идут мимо прокси Spring.
     *
     * <p>{@code @Transactional} срабатывает только на вызове через прокси, а остановка контейнера
     * и действие, переданное в {@code runWithLock} ссылкой на свой же метод, вызывают метод
     * напрямую. Шаблон задает транзакцию независимо от способа вызова.
     */
    private final org.springframework.transaction.support.TransactionTemplate txTemplate;
    private final JobProgressBuffer progressBuffer = new JobProgressBuffer();
    private final java.util.concurrent.atomic.AtomicInteger reconciliationTick = new java.util.concurrent.atomic.AtomicInteger(0);
    private final java.util.concurrent.ConcurrentMap<String, Long> emptyQueueCooldownUntil = new java.util.concurrent.ConcurrentHashMap<>();

    @Autowired
    public TaskService(TaskRepository taskRepository,
                       JobRepository jobRepository,
                       JobRunRepository jobRunRepository,
                       AgentRegistry agentRegistry,
                       TopologyRegistry topologyRegistry,
                       ReplicatorProperties properties,
                       @org.springframework.lang.Nullable org.apache.hadoop.explorer.replicator.orchestrator.service.DistributedLockService lockService,
                       @Autowired(required = false) org.apache.hadoop.explorer.replicator.orchestrator.repository.StreamingLeaseRepository leaseRepository,
                       @Autowired(required = false) org.springframework.transaction.PlatformTransactionManager transactionManager) {
        this.taskRepository = taskRepository;
        this.jobRepository = jobRepository;
        this.jobRunRepository = jobRunRepository;
        this.agentRegistry = agentRegistry;
        this.topologyRegistry = topologyRegistry;
        this.properties = properties;
        this.lockService = lockService;
        this.leaseRepository = leaseRepository;
        this.txTemplate = transactionManager != null
                ? new org.springframework.transaction.support.TransactionTemplate(transactionManager)
                : null;
    }

    public TaskService(TaskRepository taskRepository,
                       JobRepository jobRepository,
                       JobRunRepository jobRunRepository,
                       AgentRegistry agentRegistry,
                       TopologyRegistry topologyRegistry,
                       ReplicatorProperties properties) {
        this(taskRepository, jobRepository, jobRunRepository, agentRegistry, topologyRegistry, properties, null, null, null);
    }

    /**
     * Пакетная регистрация пула пофайловых задач агентом-анализатором после diff деревьев.
     */
    @Transactional
    public boolean batchCreateTasks(String jobId, BatchCreateTasksRequest req) {
        Optional<JobEntity> jobOpt = jobRepository.findById(jobId);
        if (jobOpt.isEmpty()) {
            log.warn("Попытка создания пула задач для несуществующего задания: {}", jobId);
            return false;
        }

        JobEntity job = jobOpt.get();
        boolean isStreaming = "STREAMING_INOTIFY".equalsIgnoreCase(job.getSyncMode());

        if (isStreaming && leaseRepository != null && req.getEpoch() != null && req.getEpoch() > 0) {
            String clusterId = job.getSourceClusterId() != null ? job.getSourceClusterId() : "default";
            var leaseOpt = leaseRepository.findById(clusterId);
            if (leaseOpt.isPresent() && req.getEpoch() < leaseOpt.get().getEpoch()) {
                log.warn("[Epoch Fencing] Отклонен пакет streaming задач от устаревшего/отгороженного стримера: jobId={}, clusterId={}, requestEpoch={}, activeEpoch={}",
                        jobId, clusterId, req.getEpoch(), leaseOpt.get().getEpoch());
                return false;
            }
        }

        List<TaskCreateItem> items = req.getTasks();
        if (items == null || items.isEmpty()) {
            if (!isStreaming) {
                log.info("Пул задач для job '{}' пуст. Завершение задания.", jobId);
                job.setStatus("COMPLETED");
                job.setTotalBytes(0L);
                job.setCopiedBytes(0L);
                job.setCompletedAt(Instant.now());
                job.setMessage("Каталог пуст, файлов для передачи нет");
                updateActiveRun(job);
                jobRepository.save(job);
            }
            return true;
        }

        if (!isStreaming) {
            // Очищаем предыдущие таски для этой джобы (если был перезапуск обычной пакетной джобы)
            taskRepository.deleteByJobId(jobId);
        }

        long totalBytes = 0;
        long skippedBytes = 0;
        int skippedObjectsCount = 0;
        int totalObjectsCount = 0;

        List<TaskEntity> entities = new ArrayList<>(items.size());
        for (TaskCreateItem item : items) {
            totalBytes += item.getFileSize();
            int count = item.getFileCount() > 0 ? item.getFileCount() : 1;
            totalObjectsCount += count;

            TaskEntity task = new TaskEntity();
            task.setId(item.getId() != null ? item.getId() : UUID.randomUUID().toString());
            task.setJobId(jobId);
            task.setRunId(job.getActiveRunId());
            task.setSourcePath(item.getSourcePath());
            task.setTargetPath(item.getTargetPath());
            task.setFileSize(item.getFileSize());
            task.setTaskType(item.getTaskType() != null ? item.getTaskType() : "FILE");
            task.setFileCount(count);
            task.setBundleManifest(item.getBundleManifest());

            int maxRetries = (item.getMaxRetries() != null) ? item.getMaxRetries() : properties.getMaxTaskRetries();
            task.setMaxRetries(maxRetries);
            task.setRetryCount(0);
            task.setUpdatedAt(Instant.now());

            if (item.isSkipped()) {
                task.setStatus("SKIPPED");
                skippedBytes += item.getFileSize();
                skippedObjectsCount += count;
            } else {
                task.setStatus("QUEUED");
            }
            entities.add(task);
        }

        taskRepository.saveAll(entities);

        if (isStreaming) {
            int currentTotal = job.getTotalObjects();
            long currentBytes = job.getTotalBytes();
            int currentSkipped = job.getSkippedObjects();
            long currentCopied = job.getCopiedBytes();

            job.setTotalObjects(currentTotal + totalObjectsCount);
            job.setTotalBytes(currentBytes + totalBytes);
            job.setSkippedObjects(currentSkipped + skippedObjectsCount);
            job.setCopiedBytes(currentCopied + skippedBytes);
            job.setStatus("STREAMING");
            if (job.getStartedAt() == null) {
                job.setStartedAt(Instant.now());
            }
            if (req.getLastProcessedTxid() != null && req.getLastProcessedTxid() > 0) {
                job.setLastProcessedTxid(req.getLastProcessedTxid());
            }
            job.setMessage(String.format("Inotify поток активен: добавлено %d объектов (%d байт)", totalObjectsCount, totalBytes));
        } else {
            job.setTotalObjects(totalObjectsCount);
            job.setTransferredObjects(0);
            job.setSkippedObjects(skippedObjectsCount);
            job.setFailedObjects(0);
            job.setTotalBytes(totalBytes);
            job.setCopiedBytes(skippedBytes);

            if (totalObjectsCount == 0 || skippedObjectsCount == totalObjectsCount) {
                // Все файлы уже актуальны или каталог пуст
                job.setStatus("COMPLETED");
                job.setCompletedAt(Instant.now());
                job.setMessage(totalObjectsCount == 0
                        ? "Синхронизация завершена: каталог пуст (0 объектов)"
                        : String.format("Синхронизация завершена: все %d объектов актуальны (инкрементальный пропуск)", totalObjectsCount));
            } else {
                job.setStatus("RUNNING");
                if (job.getStartedAt() == null) {
                    job.setStartedAt(Instant.now());
                }
                job.setMessage(String.format("Сформирован пул задач: %d объектов к передаче, %d пропущено (всего %d объектов в %d задачах, %d байт)",
                        totalObjectsCount - skippedObjectsCount, skippedObjectsCount, totalObjectsCount, items.size(), totalBytes));
            }
        }

        updateActiveRun(job);
        jobRepository.save(job);
        notifyNewTasksAvailable(job.getSourceClusterId());

        log.info("Для job '{}' сформирован пул из {} задач (всего объектов: {}, к передаче: {}, пропущено: {})",
                jobId, items.size(), totalObjectsCount, totalObjectsCount - skippedObjectsCount, skippedObjectsCount);
        return true;
    }

    /**
     * Проверяет, отгорожен ли стример по эпохе лизинга (Epoch Fencing).
     */
    public boolean isEpochFenced(String jobId, Long requestEpoch) {
        if (jobId == null || requestEpoch == null || requestEpoch <= 0 || leaseRepository == null) {
            return false;
        }
        var jobOpt = jobRepository.findById(jobId);
        if (jobOpt.isEmpty()) {
            return false;
        }
        JobEntity job = jobOpt.get();
        if (!"STREAMING_INOTIFY".equalsIgnoreCase(job.getSyncMode())) {
            return false;
        }
        String clusterId = job.getSourceClusterId() != null ? job.getSourceClusterId() : "default";
        var leaseOpt = leaseRepository.findById(clusterId);
        return leaseOpt.isPresent() && requestEpoch < leaseOpt.get().getEpoch();
    }

    /**
     * Проверяет, переполнен ли пул задач оркестратора (Backpressure).
     */
    public boolean isQueueSaturated(String jobId, String clusterId) {
        int threshold = properties.getStreaming() != null ? properties.getStreaming().getMaxPendingTasksThreshold() : 5000;
        if (threshold <= 0) threshold = 5000;

        if (jobId != null && !jobId.isBlank()) {
            long count = taskRepository.countByJobIdAndStatus(jobId, "QUEUED");
            return count >= threshold;
        }

        if (clusterId != null && !clusterId.isBlank()) {
            return getPendingTasksCountForCluster(clusterId) >= threshold;
        }

        long queuedTotal = taskRepository.countByStatus("QUEUED");
        return queuedTotal >= threshold;
    }

    public boolean isQueueSaturated(String clusterId) {
        return isQueueSaturated(null, clusterId);
    }

    public boolean isQueueSaturated() {
        return isQueueSaturated(null, null);
    }

    /**
     * Возвращает текущее количество задач в статусе QUEUED.
     */
    public long getQueuedTasksCount(String jobId) {
        if (jobId != null && !jobId.isBlank()) {
            return taskRepository.countByJobIdAndStatus(jobId, "QUEUED");
        }
        return taskRepository.countByStatus("QUEUED");
    }

    /**
     * Возвращает количество ожидающих задач для кластера.
     */
    public long getPendingTasksCountForCluster(String clusterId) {
        if (clusterId == null || clusterId.isBlank()) {
            return taskRepository.countByStatus("QUEUED");
        }
        return taskRepository.countPendingTasksByClusterId(clusterId, "QUEUED");
    }

    /**
     * Атомарный забор задач из пула доступным воркером кластера.
     */
    @Transactional
    public List<TaskItemDto> claimTasks(ClaimTasksRequest req) {
        String agentId = req.getAgentId();
        String clusterId = req.getClusterId();
        int limit = req.getLimit() > 0 ? req.getLimit() : 5;

        // Защита и изоляция привилегий: агент в режиме streamer не имеет права забирать пользовательские задачи передачи данных
        var agentEntry = agentRegistry.getAgent(agentId);
        boolean isStreamer = (agentEntry.isPresent() && "streamer".equalsIgnoreCase(agentEntry.get().getMode()))
                || (agentEntry.isEmpty() && agentId.toLowerCase().contains("streamer"));
        if (isStreamer) {
            log.warn("[Security Isolation] Агент-стример '{}' отклонен от забора задач передачи данных", agentId);
            throw new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatus.FORBIDDEN,
                    "Агент-стример (" + agentId + ") изолирован и не имеет права исполнять пользовательские задачи передачи данных"
            );
        }

        // In-Memory Fast-Path (PERF-07): если очередь кластера пуста и активен кулдаун,
        // мгновенно возвращаем emptyList() без блокировок, SQL-запросов и нагрузки на пул соединений
        String clusterKey = (clusterId != null && !clusterId.isBlank()) ? clusterId.trim().toLowerCase() : "*";
        long now = System.currentTimeMillis();
        Long cooldownUntil = emptyQueueCooldownUntil.get(clusterKey);
        if (cooldownUntil != null && now < cooldownUntil) {
            return Collections.emptyList();
        }

        // Запрашиваем ограниченную пачку кандидатов QUEUED с пагинацией и блокировкой SKIP LOCKED для предотвращения состояния гонки (Race Condition)
        int batchSize = Math.max(limit * 3, 20);
        List<TaskEntity> queuedTasks;
        if (clusterId != null && !clusterId.isBlank()) {
            queuedTasks = taskRepository.findPendingTasksForClusterSkipLocked("QUEUED", clusterId, PageRequest.of(0, batchSize));
        } else {
            queuedTasks = taskRepository.findByStatusForUpdateSkipLocked("QUEUED", PageRequest.of(0, batchSize));
        }
        if (queuedTasks.isEmpty()) {
            emptyQueueCooldownUntil.put(clusterKey, now + 1500L);
            return Collections.emptyList();
        }

        // Если задачи найдены — инвалидируем кулдаун для немедленной раздачи остальным агентам
        emptyQueueCooldownUntil.remove(clusterKey);
        emptyQueueCooldownUntil.remove("*");

        Map<String, JobEntity> cachedJobs = new HashMap<>();
        List<TaskItemDto> claimed = new ArrayList<>(limit);
        for (TaskEntity task : queuedTasks) {
            if (claimed.size() >= limit) {
                break;
            }

            JobEntity job = cachedJobs.computeIfAbsent(task.getJobId(), id -> jobRepository.findById(id).orElse(null));
            if (job == null) {
                continue;
            }
            if (!"RUNNING".equalsIgnoreCase(job.getStatus()) && !"STREAMING".equalsIgnoreCase(job.getStatus())) {
                continue;
            }

            // Проверяем принадлежность задачи к кластеру-источнику воркера
            if (clusterId != null && job.getSourceClusterId() != null
                    && !matchesCluster(clusterId, job.getSourceClusterId())) {
                continue;
            }

            // Если задача уже сбоила на этом агенте и в кластере есть другие активные агенты,
            // отдаем предпочтение другому агенту для распределения нагрузки и изоляции сбойного узла
            if (task.getLastFailedAgentId() != null
                    && task.getLastFailedAgentId().equalsIgnoreCase(agentId)
                    && agentRegistry.hasOtherOnlineAgentsForCluster(clusterId, agentId)) {
                continue;
            }

            // Назначаем задачу данному агенту
            task.setStatus("RUNNING");
            task.setAssignedAgentId(agentId);
            task.setUpdatedAt(Instant.now());
            taskRepository.save(task);

            // Разрешаем оптимальный gRPC адрес приемника с балансировкой нагрузки (Least Loaded)
            String targetAddress = resolveTargetAddress(job.getTargetClusterId());

            TaskItemDto dto = new TaskItemDto(
                    task.getId(),
                    task.getJobId(),
                    task.getRunId(),
                    task.getSourcePath(),
                    task.getTargetPath(),
                    task.getFileSize(),
                    task.getStatus(),
                    agentId,
                    task.getChecksum(),
                    targetAddress,
                    job.getExecutionPrincipal(),
                    job.isRunAsServiceAccount(),
                    task.getRetryCount(),
                    task.getMaxRetries(),
                    task.getLastFailedAgentId(),
                    task.getTaskType(),
                    task.getFileCount(),
                    task.getBundleManifest(),
                    job.getSourceClusterId(),
                    job.getTargetClusterId()
            );

            // Криптографическая аттестация задачи (SEC-01: Zero-Trust Task Attestation Token)
            String secret = properties != null ? properties.getAgentSecret() : null;
            if (secret != null && !secret.isBlank()) {
                String token = org.apache.hadoop.explorer.replicator.security.TaskAttestationService.generateToken(
                        task.getId(),
                        task.getJobId(),
                        task.getTargetPath(),
                        job.getExecutionPrincipal(),
                        job.isRunAsServiceAccount(),
                        secret
                );
                dto.setTaskToken(token);
            }

            claimed.add(dto);
        }

        if (!claimed.isEmpty()) {
            log.info("Агент '{}' (кластер '{}') взял в работу {} задач из пула", agentId, clusterId, claimed.size());
        }
        return claimed;
    }

    /**
     * Фиксация успешного завершения пофайловой задачи воркером.
     */
    @Transactional
    public boolean completeTask(CompleteTaskRequest req) {
        Optional<TaskEntity> taskOpt = taskRepository.findById(req.getTaskId());
        if (taskOpt.isEmpty()) {
            log.warn("Не найдена задача для завершения: {}", req.getTaskId());
            return false;
        }

        TaskEntity task = taskOpt.get();
        if ("COMPLETED".equalsIgnoreCase(task.getStatus())) {
            return true; // Идемпотентность
        }

        task.setStatus("COMPLETED");
        task.setChecksum(req.getChecksum());
        Instant now = Instant.now();
        task.setUpdatedAt(now);
        if (req.getAgentId() != null) {
            task.setAssignedAgentId(req.getAgentId());
        }
        taskRepository.save(task);

        long transferredBytes = req.getBytesTransferred() > 0 ? req.getBytesTransferred() : task.getFileSize();
        int filesDone = req.getFilesCount() > 0 ? req.getFilesCount() : (task.getFileCount() > 0 ? task.getFileCount() : 1);

        // Буферизация приращения прогресса в памяти (PERF-06): устраняет Row Lock Contention на строке replication_jobs при высоком RPS
        progressBuffer.record(task.getJobId(), transferredBytes, filesDone);

        // Быстрая проверка завершения задания через exists (индексный LIMIT 1 за 0.05 мс вместо тяжелого COUNT)
        boolean hasActiveTasks = taskRepository.existsByJobIdAndStatusIn(task.getJobId(), List.of("QUEUED", "RUNNING"));
        if (!hasActiveTasks) {
            finalizeJobCompletion(task.getJobId(), now);
        }

        return true;
    }

    /**
     * Пакетная фиксация завершения подзадач для минимизации транзакционного overhead от 200+ агентов (PERF-06).
     * Загружает и обновляет задачи пакетом через findAllById и saveAll, аккумулируя прогресс в буфере.
     */
    @Transactional
    public int completeTasksBatch(List<CompleteTaskRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            return 0;
        }

        List<String> taskIds = requests.stream()
                .map(CompleteTaskRequest::getTaskId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        if (taskIds.isEmpty()) {
            return 0;
        }

        Map<String, TaskEntity> existingTasks = taskRepository.findAllById(taskIds).stream()
                .collect(java.util.stream.Collectors.toMap(TaskEntity::getId, java.util.function.Function.identity()));

        Instant now = Instant.now();
        List<TaskEntity> tasksToSave = new ArrayList<>(requests.size());
        Set<String> affectedJobIds = new HashSet<>();
        int completedCount = 0;

        for (CompleteTaskRequest req : requests) {
            if (req.getTaskId() == null) {
                continue;
            }
            TaskEntity task = existingTasks.get(req.getTaskId());
            if (task == null) {
                log.warn("Не найдена задача для пакетного завершения: {}", req.getTaskId());
                continue;
            }

            if ("COMPLETED".equalsIgnoreCase(task.getStatus())) {
                completedCount++; // Идемпотентность
                continue;
            }

            task.setStatus("COMPLETED");
            task.setChecksum(req.getChecksum());
            task.setUpdatedAt(now);
            if (req.getAgentId() != null) {
                task.setAssignedAgentId(req.getAgentId());
            }
            tasksToSave.add(task);

            long transferredBytes = req.getBytesTransferred() > 0 ? req.getBytesTransferred() : task.getFileSize();
            int filesDone = req.getFilesCount() > 0 ? req.getFilesCount() : (task.getFileCount() > 0 ? task.getFileCount() : 1);

            progressBuffer.record(task.getJobId(), transferredBytes, filesDone);
            affectedJobIds.add(task.getJobId());
            completedCount++;
        }

        if (!tasksToSave.isEmpty()) {
            taskRepository.saveAll(tasksToSave);
        }

        for (String jobId : affectedJobIds) {
            boolean hasActiveTasks = taskRepository.existsByJobIdAndStatusIn(jobId, List.of("QUEUED", "RUNNING"));
            if (!hasActiveTasks) {
                finalizeJobCompletion(jobId, now);
            }
        }

        return completedCount;
    }

    /**
     * Единая финализация задания при завершении всех дочерних задач.
     */
    private void finalizeJobCompletion(String jobId, Instant now) {
        // PERF-06: Принудительный сброс дельты прогресса из буфера в БД перед финализацией задания
        long[] delta = progressBuffer.drainJob(jobId);
        if (delta != null && (delta[0] > 0 || delta[1] > 0)) {
            jobRepository.incrementProgress(jobId, delta[0], (int) delta[1], now);
        }

        jobRepository.findById(jobId).ifPresent(job -> {
            if (!"RUNNING".equalsIgnoreCase(job.getStatus()) && !"QUEUED".equalsIgnoreCase(job.getStatus())) {
                return;
            }
            if (taskRepository.existsByJobIdAndStatusIn(job.getId(), List.of("QUEUED", "RUNNING"))) {
                return;
            }

            long failedCount = taskRepository.countByJobIdAndStatus(job.getId(), "FAILED");
            if (failedCount > 0 || job.getFailedObjects() > 0) {
                job.setStatus("FAILED");
                job.setCompletedAt(now);
                job.setMessage(String.format("Репликация завершена с ошибками: передано %d/%d объектов (%d байт), ошибок: %d, пропущено: %d",
                        job.getTransferredObjects(), job.getTotalObjects(), job.getCopiedBytes(), Math.max(failedCount, (long) job.getFailedObjects()), job.getSkippedObjects()));
                log.error("Задание '{}' завершено с ошибками (передано: {}, ошибок: {})",
                        job.getId(), job.getTransferredObjects(), job.getFailedObjects());
            } else {
                job.setStatus("COMPLETED");
                job.setCompletedAt(now);
                job.setMessage(String.format("Репликация всех объектов успешно завершена: передано %d/%d объектов (%d байт), пропущено: %d",
                        job.getTransferredObjects(), job.getTotalObjects(), job.getCopiedBytes(), job.getSkippedObjects()));
                log.info("Задание '{}' полностью успешно завершено пулом агентов!", job.getId());
            }

            updateActiveRun(job);
            jobRepository.save(job);
        });
    }

    /**
     * Фиксация сбоя при передаче файла воркером с автоматическим failover на другой агент.
     */
    @Transactional
    public boolean failTask(FailTaskRequest req) {
        Optional<TaskEntity> taskOpt = taskRepository.findById(req.getTaskId());
        if (taskOpt.isEmpty()) {
            return false;
        }

        TaskEntity task = taskOpt.get();
        if ("FAILED".equalsIgnoreCase(task.getStatus()) || "COMPLETED".equalsIgnoreCase(task.getStatus())) {
            return true;
        }

        String failedAgent = req.getAgentId() != null ? req.getAgentId() : task.getAssignedAgentId();
        return failoverTask(task, failedAgent, req.getErrorMessage());
    }

    /**
     * Единый механизм автоматического failover для сбойных или брошенных подзадач.
     */
    @Transactional
    public boolean failoverTask(TaskEntity task, String failedAgentId, String reason) {
        int currentRetries = task.getRetryCount();
        int maxRetries = task.getMaxRetries();

        if (currentRetries < maxRetries) {
            // Автоматический failover: повтор задачи на другом агенте
            task.setRetryCount(currentRetries + 1);
            task.setStatus("QUEUED");
            task.setLastFailedAgentId(failedAgentId);
            task.setAssignedAgentId(null);
            task.setErrorMessage(String.format("Сбой на агенте '%s' (попытка %d/%d): %s. Задача возвращена в очередь.",
                    failedAgentId != null ? failedAgentId : "неизвестен", task.getRetryCount(), maxRetries, reason));
            task.setUpdatedAt(Instant.now());
            taskRepository.save(task);

            log.warn("Failover подзадачи '{}' задания '{}': сбой на агенте '{}' (попытка {}/{}). Задача возвращена в очередь для другого агента: {}",
                    task.getId(), task.getJobId(), failedAgentId, task.getRetryCount(), maxRetries, reason);

            jobRepository.findById(task.getJobId()).ifPresent(job -> {
                long activeRemaining = taskRepository.countByJobIdAndStatusIn(job.getId(), List.of("QUEUED", "RUNNING"));
                job.setMessage(String.format("Авто-failover: сбой передачи '%s' на агенте '%s' (%s), перезапуск (%d/%d), активных задач: %d",
                        task.getSourcePath(), failedAgentId != null ? failedAgentId : "неизвестен", reason,
                        task.getRetryCount(), maxRetries, activeRemaining));
                updateActiveRun(job);
                jobRepository.save(job);
                notifyNewTasksAvailable(job.getSourceClusterId());
            });

            return true;
        }

        // Превышен лимит попыток: окончательный перевод в FAILED
        task.setStatus("FAILED");
        task.setErrorMessage(String.format("Превышен лимит попыток (%d/%d). Последняя ошибка: %s",
                task.getRetryCount(), maxRetries, reason));
        task.setUpdatedAt(Instant.now());
        taskRepository.save(task);

        jobRepository.findById(task.getJobId()).ifPresent(job -> {
            job.setFailedObjects(job.getFailedObjects() + 1);

            long activeRemaining = taskRepository.countByJobIdAndStatusIn(job.getId(), List.of("QUEUED", "RUNNING"));
            if (activeRemaining == 0) {
                // Все задачи завершились, переводим джобу в FAILED
                job.setStatus("FAILED");
                job.setCompletedAt(Instant.now());
                job.setMessage(String.format("Репликация завершена с ошибками: передано %d/%d объектов, ошибок: %d: последняя ошибка: %s",
                        job.getTransferredObjects(), job.getTotalObjects(), job.getFailedObjects(), reason));
                log.error("Задание '{}' завершено с ошибками (все подзадачи обработаны, ошибок: {})",
                        job.getId(), job.getFailedObjects());
            } else {
                job.setMessage(String.format("В процессе с ошибками: исчерпаны попытки failover для '%s' (%s), осталось активных задач: %d",
                        task.getSourcePath(), reason, activeRemaining));
                log.warn("Сбой подзадачи '{}' задания '{}': {}. Осталось активных задач: {}",
                        task.getId(), job.getId(), reason, activeRemaining);
            }

            updateActiveRun(job);
            jobRepository.save(job);
        });

        return true;
    }

    /**
     * Оповещение об изменении пула задач для мгновенной инвалидации Fast-Path кулдауна.
     */
    public void notifyNewTasksAvailable(String clusterId) {
        if (clusterId != null && !clusterId.isBlank()) {
            emptyQueueCooldownUntil.remove(clusterId.trim().toLowerCase());
        }
        emptyQueueCooldownUntil.remove("*");
    }

    /**
     * Очистка Fast-Path кэша очередей (для тестирования и сброса).
     */
    public void clearEmptyQueueCooldowns() {
        emptyQueueCooldownUntil.clear();
    }

    /**
     * Эвакуация задач при явном отключении/дерегистрации агента.
     */
    @Transactional
    public int failoverTasksForAgent(String agentId, String reason) {
        if (agentId == null) return 0;
        List<TaskEntity> running = taskRepository.findByAssignedAgentIdAndStatus(agentId, "RUNNING");
        int count = 0;
        for (TaskEntity task : running) {
            failoverTask(task, agentId, reason);
            count++;
        }
        if (count > 0) {
            log.info("Выполнен failover {} активных задач для агента '{}' ({})", count, agentId, reason);
        }
        return count;
    }

    /**
     * Статусы, после которых задача больше не нужна планировщику и агентам.
     *
     * <p>Активные ({@code QUEUED}, {@code RUNNING}) не удаляются ни при каком возрасте: задача в
     * {@code RUNNING} может прямо сейчас передаваться агентом, а зависшую подберет failover.
     */
    static final List<String> TERMINAL_TASK_STATUSES = List.of("COMPLETED", "SKIPPED", "FAILED");

    /**
     * Периодическая очистка истории пофайловых задач по возрасту.
     *
     * <p>Заменяет прежний {@code cleanOldTasks}, который удалял историю одним {@code DELETE} по
     * всему окну: на миллионах пофайловых записей это длинная транзакция с блокировками и
     * раздутым WAL, а прерывание откатывало всю работу. Кроме того, прежняя очистка не удаляла
     * {@code SKIPPED} — пропущенные файлы копились навсегда — и выполнялась одновременно на всех
     * узлах active-active, то есть узлы мешали друг другу на одних и тех же страницах.
     */
    @Scheduled(fixedDelayString = "${hadoop.replicator.task-cleanup-interval-ms:3600000}")
    public void purgeExpiredTasks() {
        if (lockService != null) {
            lockService.runWithLock("task_retention_lock", java.time.Duration.ofMinutes(5),
                    this::executePurgeExpiredTasks);
        } else {
            executePurgeExpiredTasks();
        }
    }

    /**
     * Очистка истории задач пакетами.
     *
     * <p>Метод намеренно не транзакционный: каждый пакет удаляется своей транзакцией репозитория.
     * Одна транзакция на миллионы строк держала бы блокировки и раздувала WAL, а прерванная
     * очистка откатывала бы всю проделанную работу. За один проход удаляется не больше
     * {@code task-retention-batch-size × task-retention-max-batches-per-run} записей, остаток
     * догоняется следующими проходами.
     *
     * @return сколько записей удалено за этот проход
     */
    public int executePurgeExpiredTasks() {
        int retentionDays = properties.getTaskRetentionDays();
        if (retentionDays <= 0) {
            // Ноль означает «хранить все»: очистка выключена явным решением оператора
            return 0;
        }

        Instant cutoff = Instant.now().minus(retentionDays, java.time.temporal.ChronoUnit.DAYS);
        int batchSize = Math.max(1, properties.getTaskRetentionBatchSize());
        int maxBatches = Math.max(1, properties.getTaskRetentionMaxBatchesPerRun());

        int removed = 0;
        for (int batch = 0; batch < maxBatches; batch++) {
            List<String> expiredIds = taskRepository.findExpiredTaskIds(
                    TERMINAL_TASK_STATUSES, cutoff, PageRequest.of(0, batchSize));
            if (expiredIds.isEmpty()) {
                break;
            }
            taskRepository.deleteAllByIdInBatch(expiredIds);
            removed += expiredIds.size();
            if (expiredIds.size() < batchSize) {
                break;
            }
        }

        if (removed > 0) {
            log.info("Очистка истории задач: удалено {} завершенных задач старше {} дней", removed, retentionDays);
        }
        return removed;
    }

    /**
     * Периодический мониторинг зависших задач и автоматический failover при падении агентов.
     * Оптимизирован под 200+ агентов: сканирует только задачи с истекшим таймаутом обновления
     * или потерявшие назначение агента (0.1 мс при нормальной работе).
     */
    @Scheduled(fixedDelayString = "${hadoop.replicator.orphan-task-check-ms:10000}")
    public void checkAndFailoverOrphanedTasks() {
        if (lockService != null) {
            lockService.runWithLock("orphan_task_failover_lock", java.time.Duration.ofSeconds(5),
                    this::runOrphanedTaskFailoverInTransaction);
        } else {
            runOrphanedTaskFailoverInTransaction();
        }
    }

    /**
     * Пересчет осиротевших задач в явной транзакции.
     *
     * <p>Шедулер передает действие в {@code runWithLock} ссылкой на свой же метод, то есть вызов
     * идет мимо прокси и аннотация на {@code executeCheckAndFailoverOrphanedTasks} не действует.
     * Без транзакции каждый перевод задачи фиксировался бы отдельно, и сбой на середине прохода
     * оставлял бы failover примененным частично.
     */
    private void runOrphanedTaskFailoverInTransaction() {
        if (txTemplate != null) {
            txTemplate.executeWithoutResult(status -> executeCheckAndFailoverOrphanedTasks());
        } else {
            executeCheckAndFailoverOrphanedTasks();
        }
    }

    @Transactional
    public void executeCheckAndFailoverOrphanedTasks() {
        // 1. Быстрый failover для задач агентов, переставших присылать heartbeat.
        // Решение принимается через реестр с опорой на БД: планировщик может выполняться
        // на узле, который heartbeat этого агента не принимает, и тогда локальный статус
        // показывал бы OFFLINE у живого агента.
        for (AgentRegistry.AgentEntry agent : agentRegistry.getAgents()) {
            if (!agentRegistry.isAgentLive(agent.getAgentId())) {
                failoverTasksForAgent(agent.getAgentId(), "Агент перестал присылать heartbeat");
            }
        }

        // 2. Сканирование осиротевших задач без назначенного агента
        List<TaskEntity> unassignedTasks = taskRepository.findByStatusAndAssignedAgentIdIsNull("RUNNING", PageRequest.of(0, 50));
        for (TaskEntity task : unassignedTasks) {
            failoverTask(task, null, "Задача в RUNNING без назначенного агента");
        }

        // 3. Сканирование зависших задач по таймауту обновления
        Instant cutoff = Instant.now().minusSeconds(properties.getAgentHeartbeatTimeoutSeconds());
        List<TaskEntity> staleTasks = taskRepository.findByStatusAndUpdatedAtBefore("RUNNING", cutoff, PageRequest.of(0, 50));
        for (TaskEntity task : staleTasks) {
            String assignedAgent = task.getAssignedAgentId();
            if (assignedAgent != null) {
                if (!agentRegistry.isAgentLive(assignedAgent)) {
                    failoverTask(task, assignedAgent, "Агент '" + assignedAgent + "' потерял связь (STALE) более "
                            + properties.getAgentOfflineTimeoutSeconds() + "с");
                }
            }
        }
    }

    /**
     * Получение страницы подзадач с пагинацией для предотвращения OOM в UI / мониторинге.
     */
    public org.springframework.data.domain.Page<TaskItemDto> getTasksByJob(String jobId, org.springframework.data.domain.Pageable pageable) {
        return taskRepository.findByJobId(jobId, pageable).map(this::toTaskItemDto);
    }

    /**
     * Получение списка всех подзадач для мониторинга в UI.
     */
    public List<TaskItemDto> getTasksByJob(String jobId) {
        return taskRepository.findByJobId(jobId).stream()
                .map(this::toTaskItemDto)
                .toList();
    }

    private TaskItemDto toTaskItemDto(TaskEntity t) {
        return new TaskItemDto(
                t.getId(),
                t.getJobId(),
                t.getRunId(),
                t.getSourcePath(),
                t.getTargetPath(),
                t.getFileSize(),
                t.getStatus(),
                t.getAssignedAgentId(),
                t.getChecksum(),
                null,
                null,
                false,
                t.getRetryCount(),
                t.getMaxRetries(),
                t.getLastFailedAgentId()
        );
    }

    private void updateActiveRun(JobEntity job) {
        if (job.getActiveRunId() != null) {
            jobRunRepository.findById(job.getActiveRunId()).ifPresent(run -> {
                run.setStatus(job.getStatus());
                run.setTotalBytes(job.getTotalBytes());
                run.setCopiedBytes(job.getCopiedBytes());
                run.setTotalObjects(job.getTotalObjects());
                run.setTransferredObjects(job.getTransferredObjects());
                run.setSkippedObjects(job.getSkippedObjects());
                run.setFailedObjects(job.getFailedObjects());
                run.setMessage(job.getMessage());
                if ("COMPLETED".equalsIgnoreCase(job.getStatus()) || "FAILED".equalsIgnoreCase(job.getStatus())) {
                    run.setCompletedAt(job.getCompletedAt() != null ? job.getCompletedAt() : Instant.now());
                    run.calculateMetrics();
                }
                jobRunRepository.save(run);
            });
        }
    }

    private String resolveTargetAddress(String targetClusterId) {
        if (targetClusterId == null || targetClusterId.isBlank()) {
            return null;
        }

        // 1. Поиск наименее загруженного живого агента в целевом кластере (Least Loaded)
        Optional<AgentRegistry.AgentEntry> leastLoaded = agentRegistry.getLeastLoadedAgentForCluster(targetClusterId);
        if (leastLoaded.isPresent()) {
            return leastLoaded.get().getGrpcAddress();
        }

        // 2. Резервный поиск из TopologyRegistry
        String topoAddr = topologyRegistry.getClusterGrpcAddress(targetClusterId);
        if (topoAddr != null && !topoAddr.isBlank()) {
            return topoAddr;
        }

        return null;
    }

    private boolean matchesCluster(String agentCluster, String jobCluster) {
        if (agentCluster == null || jobCluster == null) return false;
        if (agentCluster.equalsIgnoreCase(jobCluster)) return true;
        String aNorm = agentCluster.toLowerCase().replace("-", "").replace("_", "");
        String jNorm = jobCluster.toLowerCase().replace("-", "").replace("_", "");
        return aNorm.equals(jNorm);
    }

    /**
     * Возвращает буфер прогресса задач для инспекции и тестов (PERF-06).
     */
    public JobProgressBuffer getProgressBuffer() {
        return progressBuffer;
    }

    /**
     * Периодический сброс накопленных в памяти дельт прогресса задач в базу данных (PERF-06).
     * Срабатывает раз в секунду, агрегируя отчеты тысяч мелких файлов в одиночные атомарные апдейты строк.
     */
    @Scheduled(fixedDelayString = "${hadoop.replicator.progress-flush-interval-ms:1000}")
    @Transactional
    public void flushPendingProgress() {
        Map<String, long[]> drained = progressBuffer.drain();
        Instant now = Instant.now();
        if (!drained.isEmpty()) {
            for (Map.Entry<String, long[]> entry : drained.entrySet()) {
                String jobId = entry.getKey();
                long[] delta = entry.getValue();
                long bytes = delta[0];
                int files = (int) delta[1];
                if (bytes > 0 || files > 0) {
                    try {
                        jobRepository.incrementProgress(jobId, bytes, files, now);
                    } catch (Exception e) {
                        log.error("Ошибка сброса буферизованного прогресса для задания '{}': {}", jobId, e.getMessage(), e);
                    }
                }
                if (!taskRepository.existsByJobIdAndStatusIn(jobId, List.of("QUEUED", "RUNNING"))) {
                    finalizeJobCompletion(jobId, now);
                }
            }
        }

        // Авто-реконсиляция (Self-Healing Reconciliation): финализация заданий, чьи подзадачи уже завершены пулом воркеров.
        // На холостом ходу не дергаем БД каждую секунду — выполняем опрос раз в 5 секунд или при наличии активного сброса.
        if (!drained.isEmpty() || reconciliationTick.incrementAndGet() % 5 == 0) {
            try {
                List<JobEntity> runningJobs = jobRepository.findByStatus("RUNNING");
                for (JobEntity j : runningJobs) {
                    if (!"STREAMING_INOTIFY".equalsIgnoreCase(j.getSyncMode())) {
                        if (j.getTotalObjects() >= 0 && (j.getTransferredObjects() + j.getSkippedObjects() + j.getFailedObjects() >= j.getTotalObjects())) {
                            if (!taskRepository.existsByJobIdAndStatusIn(j.getId(), List.of("QUEUED", "RUNNING"))) {
                                finalizeJobCompletion(j.getId(), now);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Ошибка периодической сверки завершения заданий: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Остаточный сброс буфера прогресса при остановке узла.
     *
     * <p>Вызов из {@code @PreDestroy} идет мимо прокси, поэтому транзакция открывается явно:
     * иначе модифицирующий запрос прогресса выполнялся бы вне транзакции и на PostgreSQL
     * отвергался read-only транзакцией репозитория — накопленные байты и файлы терялись бы
     * при каждой плановой остановке.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Остановка TaskService: принудительный сброс остаточного буфера прогресса...");
        if (txTemplate != null) {
            txTemplate.executeWithoutResult(status -> flushPendingProgress());
        } else {
            flushPendingProgress();
        }
    }
}
