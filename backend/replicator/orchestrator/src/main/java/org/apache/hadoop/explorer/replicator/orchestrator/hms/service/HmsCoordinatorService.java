package org.apache.hadoop.explorer.replicator.orchestrator.hms.service;

import org.apache.hadoop.explorer.replicator.model.HmsEventReportDto;
import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
import org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

/**
 * Control Plane координатор задач репликации схем Hive Metastore (HMS).
 * <p>
 * Принципы проектирования:
 * 1. Чистый Control Plane: Оркестратор НЕ выполняет локальных обращений в HDFS
 *    и НЕ держит Thrift RPC соединений с Hive Metastore.
 * 2. Диспетчеризация задач: Оркестратор находит пару зарегистрированных агентов
 *    (Source Agent в DC1 и Target Agent в DC2) и переводит задачу в статус QUEUED.
 * 3. Отсутствие агентов: если агенты не зарегистрированы, задача переходит в
 *    статус WAITING_FOR_AGENTS без fallback-попыток выполнить перенос самостоятельно.
 * 4. Телеметрия и аудит: принимает отчеты о прогрессе и DDL-логи от агентов
 *    через REST API для сохранения в БД и отображения в UI.
 */
@Service
public class HmsCoordinatorService {

    private static final Logger log = LoggerFactory.getLogger(HmsCoordinatorService.class);

    private final HmsReplicationJobRepository hmsJobRepository;
    private final HmsEventLogRepository hmsEventLogRepository;
    private final AgentRegistry agentRegistry;
    private final JobService jobService;
    private final org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties properties;
    private final org.apache.hadoop.explorer.replicator.orchestrator.service.DistributedLockService lockService;

    @Autowired
    public HmsCoordinatorService(
            HmsReplicationJobRepository hmsJobRepository,
            HmsEventLogRepository hmsEventLogRepository,
            AgentRegistry agentRegistry,
            JobService jobService,
            org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties properties,
            @org.springframework.lang.Nullable org.apache.hadoop.explorer.replicator.orchestrator.service.DistributedLockService lockService
    ) {
        this.hmsJobRepository = hmsJobRepository;
        this.hmsEventLogRepository = hmsEventLogRepository;
        this.agentRegistry = agentRegistry;
        this.jobService = jobService;
        this.properties = properties;
        this.lockService = lockService;
    }

    public HmsCoordinatorService(
            HmsReplicationJobRepository hmsJobRepository,
            HmsEventLogRepository hmsEventLogRepository,
            AgentRegistry agentRegistry,
            JobService jobService,
            org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties properties
    ) {
        this(hmsJobRepository, hmsEventLogRepository, agentRegistry, jobService, properties, null);
    }

    public HmsCoordinatorService(
            HmsReplicationJobRepository hmsJobRepository,
            HmsEventLogRepository hmsEventLogRepository,
            AgentRegistry agentRegistry,
            JobService jobService
    ) {
        this(hmsJobRepository, hmsEventLogRepository, agentRegistry, jobService, new org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties());
    }

    /**
     * Создание новой задачи репликации схемы с дефолтными флагами.
     */
    @Transactional
    public HmsReplicationJobEntity createAndStartReplication(
            String sourceClusterId,
            String targetClusterId,
            String sourceDb,
            String targetDb,
            String tablePattern,
            String author
    ) {
        return createAndStartReplication(sourceClusterId, targetClusterId, sourceDb, targetDb, tablePattern, author, false, false);
    }

    @Transactional
    public HmsReplicationJobEntity createAndStartReplication(
            String sourceClusterId,
            String targetClusterId,
            String sourceDb,
            String targetDb,
            String tablePattern,
            String author,
            boolean dropExtraneousTables,
            boolean dropExtraneousPartitions,
            String executionPrincipal
    ) {
        String id = "hms-job-" + sourceDb + "-" + UUID.randomUUID().toString().substring(0, 8);

        HmsReplicationJobEntity entity = new HmsReplicationJobEntity();
        entity.setId(id);
        entity.setSourceClusterId(sourceClusterId != null ? sourceClusterId : "dc1");
        entity.setTargetClusterId(targetClusterId != null ? targetClusterId : "dc2");
        entity.setSourceDbName(sourceDb);
        entity.setTargetDbName(targetDb != null ? targetDb : sourceDb);
        entity.setTableIncludePattern(tablePattern != null ? tablePattern : "*");
        entity.setCreatedBy(author != null ? author : "system_operator");
        entity.setExecutionPrincipal(executionPrincipal != null && !executionPrincipal.isBlank() ? executionPrincipal : "hdfs@EXAMPLE.COM");
        entity.setDropExtraneousTables(dropExtraneousTables);
        entity.setDropExtraneousPartitions(dropExtraneousPartitions);

        dispatchJob(entity);
        return entity;
    }

    /**
     * Создание новой задачи репликации схемы и диспетчеризация паре агентов (обратная совместимость).
     */
    @Transactional
    public HmsReplicationJobEntity createAndStartReplication(
            String sourceClusterId,
            String targetClusterId,
            String sourceDb,
            String targetDb,
            String tablePattern,
            String author,
            boolean dropExtraneousTables,
            boolean dropExtraneousPartitions
    ) {
        String defaultPrincipal = (author != null && !author.isBlank())
                ? (author.contains("@") ? author : author + "@REALM.LOCAL")
                : "hdfs@EXAMPLE.COM";
        return createAndStartReplication(sourceClusterId, targetClusterId, sourceDb, targetDb, tablePattern, author, dropExtraneousTables, dropExtraneousPartitions, defaultPrincipal);
    }

    /**
     * Диспетчеризация задачи паре агентов (Source Agent и Target Agent).
     * Если хотя бы один агент недоступен, задача переводится в WAITING_FOR_AGENTS.
     */
    public void dispatchJob(HmsReplicationJobEntity job) {
        job.setAssignedAgentId(null);
        job.setLeaseExpiresAt(null);

        var srcAgentOpt = agentRegistry.getLiveHmsAgentForCluster(job.getSourceClusterId());
        var dstAgentOpt = agentRegistry.getLiveHmsAgentForCluster(job.getTargetClusterId());

        if (srcAgentOpt.isEmpty() || dstAgentOpt.isEmpty()) {
            String missing;
            if (srcAgentOpt.isEmpty() && dstAgentOpt.isEmpty()) {
                missing = job.getSourceClusterId() + " и " + job.getTargetClusterId();
            } else if (srcAgentOpt.isEmpty()) {
                missing = job.getSourceClusterId();
            } else {
                missing = job.getTargetClusterId();
            }

            job.setStatus("WAITING_FOR_AGENTS");
            job.setMessage("Ожидание подключения агентов репликации для кластера(ов): " + missing);
            log.info("[HmsCoordinator] Задача {} переведена в WAITING_FOR_AGENTS (нет агентов для {})", job.getId(), missing);
        } else {
            var srcAgent = srcAgentOpt.get();
            var dstAgent = dstAgentOpt.get();

            job.setStatus("QUEUED");
            job.setMessage(String.format("Задача назначена источнику '%s' (gRPC целевого агента: %s)",
                    srcAgent.getAgentId(), dstAgent.getGrpcAddress()));
            log.info("[HmsCoordinator] Задача {} диспетчеризована: источник='{}', целевой gRPC='{}'",
                    job.getId(), srcAgent.getAgentId(), dstAgent.getGrpcAddress());
        }

        hmsJobRepository.saveAndFlush(job);
    }

    /**
     * Получение списка задач, готовых к исполнению или находящихся в процессе, для агента источника.
     */
    public List<HmsPendingJobDto> getPendingJobsForCluster(String clusterId) {
        return getPendingJobsForCluster(clusterId, null);
    }

    /**
     * Получение списка задач с эксклюзивным распределенным лизингом (Distributed Lease).
     * Предотвращает дублирование CDC потоков между несколькими активными агентами одного кластера.
     */
    @Transactional
    public List<HmsPendingJobDto> getPendingJobsForCluster(String clusterId, String requestingAgentId) {
        List<HmsReplicationJobEntity> allJobs = hmsJobRepository.findAll();
        List<HmsPendingJobDto> result = new ArrayList<>();
        Instant now = Instant.now();
        long leaseSec = getLeaseDurationSeconds();

        for (HmsReplicationJobEntity job : allJobs) {
            if (clusterId != null && !matchesCluster(job.getSourceClusterId(), clusterId)) {
                continue;
            }

            if (!"QUEUED".equalsIgnoreCase(job.getStatus())
                    && !"BOOTSTRAPPING".equalsIgnoreCase(job.getStatus())
                    && !"ACTIVE".equalsIgnoreCase(job.getStatus())) {
                continue;
            }

            var dstOpt = agentRegistry.getLiveHmsAgentForCluster(job.getTargetClusterId());
            String targetAddress = dstOpt.map(AgentRegistry.AgentEntry::getGrpcAddress).orElse(null);

            if (targetAddress == null && !"ACTIVE".equalsIgnoreCase(job.getStatus())) {
                job.setStatus("WAITING_FOR_AGENTS");
                job.setMessage("Целевой агент для кластера " + job.getTargetClusterId() + " недоступен");
                job.setAssignedAgentId(null);
                job.setLeaseExpiresAt(null);
                hmsJobRepository.save(job);
                continue;
            }

            // Механизм эксклюзивного лизинга:
            if (requestingAgentId != null && !requestingAgentId.isBlank()) {
                String assigned = job.getAssignedAgentId();
                Instant leaseExp = job.getLeaseExpiresAt();
                boolean isAssignedToMe = requestingAgentId.equalsIgnoreCase(assigned);
                boolean isLeaseExpired = leaseExp == null || leaseExp.isBefore(now);
                boolean isAssignedAgentDead = assigned != null && !isAgentOnline(assigned);

                if (isAssignedToMe) {
                    // Продление существующей аренды
                    job.setLeaseExpiresAt(now.plusSeconds(leaseSec));
                    hmsJobRepository.save(job);
                } else if (assigned == null || isLeaseExpired || isAssignedAgentDead) {
                    // Если задача недавно упала на этом агенте, отдаем предпочтение другому агенту пула
                    if (job.getLastFailedAgentId() != null
                            && job.getLastFailedAgentId().equalsIgnoreCase(requestingAgentId)
                            && agentRegistry.hasOtherOnlineAgentsForCluster(clusterId, requestingAgentId)) {
                        continue;
                    }

                    // Эксклюзивный захват задачи
                    job.setAssignedAgentId(requestingAgentId);
                    job.setLeaseExpiresAt(now.plusSeconds(leaseSec));
                    if (assigned != null && !assigned.equalsIgnoreCase(requestingAgentId)) {
                        log.info("[HmsCoordinator] Задача {} перехвачена агентом '{}' (прежний агент '{}': dead={}, expired={})",
                                job.getId(), requestingAgentId, assigned, isAssignedAgentDead, isLeaseExpired);
                    }
                    hmsJobRepository.save(job);
                } else {
                    // Задача удерживается другим живым агентом — пропускаем
                    continue;
                }
            }

            result.add(new HmsPendingJobDto(
                    job.getId(),
                    job.getSourceClusterId(),
                    job.getTargetClusterId(),
                    job.getSourceDbName(),
                    job.getTargetDbName(),
                    job.getTableIncludePattern(),
                    job.isDropExtraneousTables(),
                    job.isDropExtraneousPartitions(),
                    targetAddress,
                    job.getStatus(),
                    job.getLastProcessedEventId(),
                    job.getAssignedAgentId(),
                    job.getExecutionPrincipal()
            ));
        }

        return result;
    }

    /**
     * Прием отчета о прогрессе и DDL логов от Replicator Agent.
     */
    @Transactional
    public void updateJobProgress(String jobId, HmsProgressReportRequest report) {
        Optional<HmsReplicationJobEntity> optJob = hmsJobRepository.findById(jobId);
        if (optJob.isEmpty()) {
            log.warn("[HmsCoordinator] Попытка обновить прогресс несуществующей задачи {}", jobId);
            return;
        }

        HmsReplicationJobEntity job = optJob.get();

        if (report.status() != null && !report.status().isBlank()) {
            job.setStatus(report.status());
        }
        if (report.totalTables() > 0) {
            job.setTotalTables(report.totalTables());
        }
        if (report.replicatedTables() > 0) {
            job.setReplicatedTables(report.replicatedTables());
        }
        if (report.totalPartitions() > 0) {
            job.setTotalPartitions(report.totalPartitions());
        }
        if (report.replicatedPartitions() > 0) {
            job.setReplicatedPartitions(report.replicatedPartitions());
        }
        if (report.lastProcessedEventId() != null) {
            job.setLastProcessedEventId(report.lastProcessedEventId());
        }
        if (report.bootstrapEventId() != null) {
            job.setBootstrapEventId(report.bootstrapEventId());
        }
        if (report.eventLag() != null) {
            job.setEventLag(report.eventLag());
        }
        // Сохранение DDL событий в историю аудита
        if (report.events() != null && !report.events().isEmpty()) {
            for (HmsEventReportDto ev : report.events()) {
                HmsEventLogEntity eventEntity = new HmsEventLogEntity();
                eventEntity.setId(UUID.randomUUID().toString());
                eventEntity.setHmsJobId(jobId);
                eventEntity.setEventId(ev.eventId());
                eventEntity.setEventType(ev.eventType());
                eventEntity.setTableName(ev.tableName());
                eventEntity.setPartitionName(ev.partitionName());
                eventEntity.setSourceUri(ev.sourceUri());
                eventEntity.setTargetUri(ev.targetUri());
                eventEntity.setSubjobId(ev.subjobId());
                eventEntity.setStatus(ev.status());
                eventEntity.setErrorMessage(ev.errorMessage());
                hmsEventLogRepository.save(eventEntity);
            }
        }

        if (report.message() != null) {
            if (report.message().startsWith("Синхронизировано")) {
                long totalEvents = hmsEventLogRepository.countByHmsJobId(jobId);
                long lag = job.getEventLag() != null ? job.getEventLag() : 0L;
                if (totalEvents > 0) {
                    job.setMessage(String.format("Синхронизировано %d событий CDC. Текущее отставание: %d событий.", totalEvents, lag));
                } else {
                    job.setMessage(report.message());
                }
            } else {
                job.setMessage(report.message());
            }
        }
        if (report.agentId() != null && !report.agentId().isBlank()) {
            job.setAssignedAgentId(report.agentId());
            job.setLeaseExpiresAt(Instant.now().plusSeconds(getLeaseDurationSeconds()));
        }
        job.setLastSyncAt(Instant.now());
        hmsJobRepository.save(job);
    }

    /**
     * Фоновая периодическая проверка задач в статусе WAITING_FOR_AGENTS с защитой от параллельного запуска.
     */
    @Scheduled(fixedDelayString = "${hadoop.replicator.hms-agent-check-ms:5000}")
    public void scheduledCheckWaitingJobs() {
        if (lockService != null) {
            lockService.runWithLock("hms_agent_check_lock", java.time.Duration.ofSeconds(4), this::checkWaitingJobs);
        } else {
            checkWaitingJobs();
        }
    }

    /**
     * Проверка задач в статусе WAITING_FOR_AGENTS.
     * При появлении агентов переводит задачу в QUEUED.
     */
    public void checkWaitingJobs() {
        List<HmsReplicationJobEntity> waiting = hmsJobRepository.findByStatus("WAITING_FOR_AGENTS");
        for (HmsReplicationJobEntity job : waiting) {
            var srcOpt = agentRegistry.getLiveHmsAgentForCluster(job.getSourceClusterId());
            var dstOpt = agentRegistry.getLiveHmsAgentForCluster(job.getTargetClusterId());
            if (srcOpt.isPresent() && dstOpt.isPresent()) {
                log.info("[HmsCoordinator] Появились активные агенты для задачи {}. Перевод в QUEUED...", job.getId());
                dispatchJob(job);
            }
        }
    }

    /**
     * Перезапуск начального Bootstrap (Rebootstrap) по запросу оператора.
     */
    @Transactional
    public void rebootstrap(String id) {
        HmsReplicationJobEntity job = hmsJobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Задача репликации HMS не найдена: " + id));
        job.setReplicatedTables(0);
        job.setReplicatedPartitions(0);
        job.setLastProcessedEventId(0L);
        job.setBootstrapEventId(0L);
        dispatchJob(job);
    }

    /**
     * Восстановление прерванных задач при старте Оркестратора.
     * Оркестратор проверяет наличие агентов и выставляет QUEUED или WAITING_FOR_AGENTS.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void recoverInterruptedJobsOnStartup() {
        List<HmsReplicationJobEntity> interrupted = hmsJobRepository.findByStatus("BOOTSTRAPPING");
        if (interrupted.isEmpty()) {
            return;
        }
        log.warn("[HmsCoordinator] Обнаружено {} задач в BOOTSTRAPPING после перезапуска Оркестратора. Диспетчеризация агентам...",
                interrupted.size());
        for (HmsReplicationJobEntity job : interrupted) {
            dispatchJob(job);
        }
    }

    /**
     * Удаление задачи репликации, истории событий и связанных HDFS саб-джоб.
     */
    @Transactional
    public boolean deleteReplicationJob(String id) {
        if (!hmsJobRepository.existsById(id)) {
            return false;
        }
        hmsEventLogRepository.deleteByHmsJobId(id);
        jobService.deleteSubjobsByParentId(id);
        hmsJobRepository.deleteById(id);
        log.info("[HmsCoordinator] Задача {} и связанные ресурсы удалены", id);
        return true;
    }

    /**
    /**
     * Фоновый периодический мониторинг зависших или брошенных HMS задач с защитой от параллельного запуска.
     */
    @Scheduled(fixedDelayString = "${hadoop.replicator.hms-failover-check-ms:3000}")
    public void scheduledCheckAndFailoverOrphanedHmsJobs() {
        if (lockService != null) {
            lockService.runWithLock("hms_failover_check_lock", java.time.Duration.ofSeconds(4), this::checkAndFailoverOrphanedHmsJobs);
        } else {
            checkAndFailoverOrphanedHmsJobs();
        }
    }

    /**
     * Мониторинг зависших или брошенных HMS задач (сбой или падение агента).
     * При переходе агента в OFFLINE или протухании аренды выполняет авто-failover.
     */
    @Transactional
    public void checkAndFailoverOrphanedHmsJobs() {
        List<HmsReplicationJobEntity> allJobs = hmsJobRepository.findAll();
        Instant now = Instant.now();

        for (HmsReplicationJobEntity job : allJobs) {
            String assignedAgent = job.getAssignedAgentId();
            if (assignedAgent == null || assignedAgent.isBlank()) {
                continue;
            }

            if (!"BOOTSTRAPPING".equalsIgnoreCase(job.getStatus())
                    && !"ACTIVE".equalsIgnoreCase(job.getStatus())
                    && !"QUEUED".equalsIgnoreCase(job.getStatus())) {
                continue;
            }

            boolean shouldFailover = false;
            String reason = null;

            var agentOpt = agentRegistry.getAgent(assignedAgent);
            if (agentOpt.isEmpty()) {
                shouldFailover = true;
                reason = "Агент '" + assignedAgent + "' не зарегистрирован в реестре";
            } else {
                var agent = agentOpt.get();
                if (agent.getStatus() == AgentRegistry.AgentStatus.OFFLINE) {
                    shouldFailover = true;
                    reason = "Агент '" + assignedAgent + "' перешел в статус OFFLINE";
                } else if (job.getLeaseExpiresAt() != null && job.getLeaseExpiresAt().isBefore(now)) {
                    shouldFailover = true;
                    reason = "Истекла аренда (lease) задачи более " + getLeaseDurationSeconds() + "с";
                }
            }

            if (shouldFailover) {
                log.warn("[HmsCoordinator] Зависшая/брошенная задача '{}' на агенте '{}' ({}). Выполняется авто-failover...",
                        job.getId(), assignedAgent, reason);
                failoverHmsJob(job, assignedAgent, reason);
            }
        }
    }

    /**
     * Автоматический failover задачи репликации схемы на другой агент.
     */
    @Transactional
    public void failoverHmsJob(HmsReplicationJobEntity job, String failedAgentId, String reason) {
        job.setLastFailedAgentId(failedAgentId);
        job.setAssignedAgentId(null);
        job.setLeaseExpiresAt(null);

        var srcOpt = agentRegistry.getLiveHmsAgentForCluster(job.getSourceClusterId());
        var dstOpt = agentRegistry.getLiveHmsAgentForCluster(job.getTargetClusterId());

        if (srcOpt.isEmpty() || dstOpt.isEmpty()) {
            job.setStatus("WAITING_FOR_AGENTS");
            job.setMessage(String.format("Авто-failover: сбой на агенте '%s' (%s). Ожидание подключения новых агентов.",
                    failedAgentId != null ? failedAgentId : "неизвестен", reason));
        } else {
            if ("BOOTSTRAPPING".equalsIgnoreCase(job.getStatus())) {
                job.setStatus("QUEUED");
                job.setMessage(String.format("Авто-failover: сбой Bootstrap на агенте '%s' (%s). Задача возвращена в очередь.",
                        failedAgentId != null ? failedAgentId : "неизвестен", reason));
            } else if ("ACTIVE".equalsIgnoreCase(job.getStatus())) {
                job.setMessage(String.format("Авто-failover: сбой CDC на агенте '%s' (%s). Задача доступна для перехвата другим агентом кластера.",
                        failedAgentId != null ? failedAgentId : "неизвестен", reason));
            }
        }

        hmsJobRepository.saveAndFlush(job);
        log.info("[HmsCoordinator] Авто-failover задачи {} завершен: статус={}, причина={}",
                job.getId(), job.getStatus(), reason);
    }

    /**
     * Эвакуация задач при явном отключении агента.
     */
    @Transactional
    public int failoverHmsJobsForAgent(String agentId, String reason) {
        if (agentId == null || agentId.isBlank()) return 0;
        List<HmsReplicationJobEntity> allJobs = hmsJobRepository.findAll();
        int count = 0;
        for (HmsReplicationJobEntity job : allJobs) {
            if (agentId.equalsIgnoreCase(job.getAssignedAgentId())) {
                failoverHmsJob(job, agentId, reason);
                count++;
            }
        }
        return count;
    }

    private long getLeaseDurationSeconds() {
        return properties != null && properties.getHmsLeaseTimeoutSeconds() > 0
                ? properties.getHmsLeaseTimeoutSeconds()
                : 30L;
    }

    private boolean isAgentOnline(String agentId) {
        if (agentId == null) return false;
        return agentRegistry.getAgent(agentId)
                .map(a -> a.getStatus() == AgentRegistry.AgentStatus.ONLINE)
                .orElse(false);
    }

    private boolean matchesCluster(String c1, String c2) {
        if (c1 == null || c2 == null) return false;
        if (c1.equalsIgnoreCase(c2)) return true;
        String n1 = c1.toLowerCase().replace("-", "").replace("_", "");
        String n2 = c2.toLowerCase().replace("-", "").replace("_", "");
        return n1.equals(n2) || n1.contains(n2) || n2.contains(n1);
    }
}
