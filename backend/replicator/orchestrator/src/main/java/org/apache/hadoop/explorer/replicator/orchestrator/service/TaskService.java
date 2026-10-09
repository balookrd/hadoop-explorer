package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.model.*;
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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

/**
 * Сервис управления распределенным пулом пофайловых задач (Distributed Task Queue).
 * Координирует параллельную репликацию файлов между всеми доступными агентами кластера.
 */
@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);

    private final TaskRepository taskRepository;
    private final JobRepository jobRepository;
    private final JobRunRepository jobRunRepository;
    private final AgentRegistry agentRegistry;
    private final TopologyRegistry topologyRegistry;

    public TaskService(TaskRepository taskRepository,
                       JobRepository jobRepository,
                       JobRunRepository jobRunRepository,
                       AgentRegistry agentRegistry,
                       TopologyRegistry topologyRegistry) {
        this.taskRepository = taskRepository;
        this.jobRepository = jobRepository;
        this.jobRunRepository = jobRunRepository;
        this.agentRegistry = agentRegistry;
        this.topologyRegistry = topologyRegistry;
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
        List<TaskCreateItem> items = req.getTasks();
        if (items == null || items.isEmpty()) {
            log.info("Пул задач для job '{}' пуст. Завершение задания.", jobId);
            job.setStatus("COMPLETED");
            job.setTotalBytes(0L);
            job.setCopiedBytes(0L);
            job.setCompletedAt(Instant.now());
            job.setMessage("Каталог пуст, файлов для передачи нет");
            updateActiveRun(job);
            jobRepository.save(job);
            return true;
        }

        // Очищаем предыдущие таски для этой джобы (если был перезапуск)
        taskRepository.deleteByJobId(jobId);

        long totalBytes = 0;
        long skippedBytes = 0;
        int skippedCount = 0;

        List<TaskEntity> entities = new ArrayList<>(items.size());
        for (TaskCreateItem item : items) {
            totalBytes += item.getFileSize();

            TaskEntity task = new TaskEntity();
            task.setId(item.getId() != null ? item.getId() : UUID.randomUUID().toString());
            task.setJobId(jobId);
            task.setRunId(job.getActiveRunId());
            task.setSourcePath(item.getSourcePath());
            task.setTargetPath(item.getTargetPath());
            task.setFileSize(item.getFileSize());

            if (item.isSkipped()) {
                task.setStatus("SKIPPED");
                skippedBytes += item.getFileSize();
                skippedCount++;
            } else {
                task.setStatus("QUEUED");
            }
            entities.add(task);
        }

        taskRepository.saveAll(entities);

        job.setTotalObjects(items.size());
        job.setTransferredObjects(0);
        job.setSkippedObjects(skippedCount);
        job.setFailedObjects(0);
        job.setTotalBytes(totalBytes);
        job.setCopiedBytes(skippedBytes);

        if (skippedCount == items.size()) {
            // Все файлы уже актуальны
            job.setStatus("COMPLETED");
            job.setCompletedAt(Instant.now());
            job.setMessage(String.format("Синхронизация завершена: все %d объектов актуальны (инкрементальный пропуск)", items.size()));
        } else {
            job.setStatus("RUNNING");
            if (job.getStartedAt() == null) {
                job.setStartedAt(Instant.now());
            }
            job.setMessage(String.format("Сформирован пул задач: %d к передаче, %d пропущено (всего %d объектов, %d байт)",
                    items.size() - skippedCount, skippedCount, items.size(), totalBytes));
        }

        updateActiveRun(job);
        jobRepository.save(job);

        log.info("Для job '{}' сформирован пул из {} задач (к передаче: {}, пропущено: {})",
                jobId, items.size(), items.size() - skippedCount, skippedCount);
        return true;
    }

    /**
     * Атомарный забор задач из пула доступным воркером кластера.
     */
    @Transactional
    public List<TaskItemDto> claimTasks(ClaimTasksRequest req) {
        String agentId = req.getAgentId();
        String clusterId = req.getClusterId();
        int limit = req.getLimit() > 0 ? req.getLimit() : 5;

        // Находим все задачи со статусом QUEUED
        List<TaskEntity> queuedTasks = taskRepository.findByStatus("QUEUED");
        if (queuedTasks.isEmpty()) {
            return Collections.emptyList();
        }

        List<TaskItemDto> claimed = new ArrayList<>();
        for (TaskEntity task : queuedTasks) {
            if (claimed.size() >= limit) {
                break;
            }

            Optional<JobEntity> jobOpt = jobRepository.findById(task.getJobId());
            if (jobOpt.isEmpty()) {
                continue;
            }

            JobEntity job = jobOpt.get();
            if (!"RUNNING".equalsIgnoreCase(job.getStatus())) {
                continue;
            }

            // Проверяем принадлежность задачи к кластеру-источнику воркера
            if (clusterId != null && job.getSourceClusterId() != null
                    && !matchesCluster(clusterId, job.getSourceClusterId())) {
                continue;
            }

            // Назначаем задачу данному агенту
            task.setStatus("RUNNING");
            task.setAssignedAgentId(agentId);
            taskRepository.save(task);

            // Разрешаем оптимальный gRPC адрес приемника с балансировкой нагрузки (Least Loaded)
            String targetAddress = resolveTargetAddress(job.getTargetClusterId());

            claimed.add(new TaskItemDto(
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
                    job.isRunAsServiceAccount()
            ));
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
        if (req.getAgentId() != null) {
            task.setAssignedAgentId(req.getAgentId());
        }
        taskRepository.save(task);

        // Обновляем родительскую задачу
        jobRepository.findById(task.getJobId()).ifPresent(job -> {
            long newCopied = job.getCopiedBytes() + req.getBytesTransferred();
            job.setCopiedBytes(newCopied);
            job.setTransferredObjects(job.getTransferredObjects() + 1);

            long activeRemaining = taskRepository.countByJobIdAndStatusIn(job.getId(), List.of("QUEUED", "RUNNING"));
            if (activeRemaining == 0) {
                long failedCount = taskRepository.countByJobIdAndStatus(job.getId(), "FAILED");
                if (failedCount > 0 || job.getFailedObjects() > 0) {
                    job.setStatus("FAILED");
                    job.setCompletedAt(Instant.now());
                    job.setMessage(String.format("Репликация завершена с ошибками: передано %d/%d объектов (%d байт), ошибок: %d, пропущено: %d",
                            job.getTransferredObjects(), job.getTotalObjects(), newCopied, Math.max(failedCount, (long) job.getFailedObjects()), job.getSkippedObjects()));
                    log.error("Задание '{}' завершено с ошибками (передано: {}, ошибок: {})",
                            job.getId(), job.getTransferredObjects(), job.getFailedObjects());
                } else {
                    job.setStatus("COMPLETED");
                    job.setCompletedAt(Instant.now());
                    job.setMessage(String.format("Репликация всех объектов успешно завершена: передано %d/%d объектов (%d байт), пропущено: %d",
                            job.getTransferredObjects(), job.getTotalObjects(), newCopied, job.getSkippedObjects()));
                    log.info("Задание '{}' полностью успешно завершено пулом агентов!", job.getId());
                }
            } else {
                job.setMessage(String.format("В процессе: передано %d/%d объектов (%d байт), осталось активных задач: %d",
                        job.getTransferredObjects(), job.getTotalObjects(), newCopied, activeRemaining));
            }

            updateActiveRun(job);
            jobRepository.save(job);
        });

        return true;
    }

    /**
     * Фиксация сбоя при передаче файла воркером.
     */
    @Transactional
    public boolean failTask(FailTaskRequest req) {
        Optional<TaskEntity> taskOpt = taskRepository.findById(req.getTaskId());
        if (taskOpt.isEmpty()) {
            return false;
        }

        TaskEntity task = taskOpt.get();
        if ("FAILED".equalsIgnoreCase(task.getStatus())) {
            return true;
        }
        task.setStatus("FAILED");
        task.setErrorMessage(req.getErrorMessage());
        taskRepository.save(task);

        jobRepository.findById(task.getJobId()).ifPresent(job -> {
            job.setFailedObjects(job.getFailedObjects() + 1);

            long activeRemaining = taskRepository.countByJobIdAndStatusIn(job.getId(), List.of("QUEUED", "RUNNING"));
            if (activeRemaining == 0) {
                // Все задачи завершились, переводим джобу в FAILED
                job.setStatus("FAILED");
                job.setCompletedAt(Instant.now());
                job.setMessage(String.format("Репликация завершена с ошибками: передано %d/%d объектов, ошибок: %d: последняя ошибка: %s",
                        job.getTransferredObjects(), job.getTotalObjects(), job.getFailedObjects(), req.getErrorMessage()));
                log.error("Задание '{}' завершено с ошибками (все подзадачи обработаны, ошибок: {})",
                        job.getId(), job.getFailedObjects());
            } else {
                // Задание остается RUNNING, чтобы пул агентов мог продолжить параллельную обработку остальных файлов
                job.setMessage(String.format("В процессе с ошибками: сбой передачи '%s' (%s), осталось активных задач: %d",
                        task.getSourcePath(), req.getErrorMessage(), activeRemaining));
                log.warn("Сбой подзадачи '{}' задания '{}': {}. Осталось активных задач: {}",
                        task.getId(), job.getId(), req.getErrorMessage(), activeRemaining);
            }

            updateActiveRun(job);
            jobRepository.save(job);
        });

        return true;
    }

    /**
     * Получение списка всех подзадач для мониторинга в UI.
     */
    public List<TaskItemDto> getTasksByJob(String jobId) {
        return taskRepository.findByJobId(jobId).stream()
                .map(t -> new TaskItemDto(
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
                        null
                ))
                .toList();
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
        if (targetClusterId == null) return "localhost:50051";

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

        return "localhost:50051";
    }

    private boolean matchesCluster(String agentCluster, String jobCluster) {
        if (agentCluster.equalsIgnoreCase(jobCluster)) return true;
        String aNorm = agentCluster.toLowerCase().replace("-", "").replace("_", "");
        String jNorm = jobCluster.toLowerCase().replace("-", "").replace("_", "");
        return aNorm.equals(jNorm) || aNorm.contains(jNorm) || jNorm.contains(aNorm);
    }
}
