package org.apache.hadoop.explorer.replicator.orchestrator.hms.service;

import org.apache.hadoop.explorer.replicator.model.HmsEventReportDto;
import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
import org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
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
    private final org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsDeadLetterEventRepository hmsDeadLetterEventRepository;
    private final AgentRegistry agentRegistry;
    private final JobService jobService;
    private final org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties properties;
    private final org.apache.hadoop.explorer.replicator.orchestrator.service.DistributedLockService lockService;
    /**
     * Явная граница транзакции для путей, идущих мимо прокси Spring (действие под блокировкой
     * передается ссылкой на свой же метод, и аннотация на нем не срабатывает).
     */
    private final org.springframework.transaction.support.TransactionTemplate txTemplate;

    @Autowired
    public HmsCoordinatorService(
            HmsReplicationJobRepository hmsJobRepository,
            HmsEventLogRepository hmsEventLogRepository,
            @Autowired(required = false) org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsDeadLetterEventRepository hmsDeadLetterEventRepository,
            AgentRegistry agentRegistry,
            JobService jobService,
            org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties properties,
            @org.springframework.lang.Nullable org.apache.hadoop.explorer.replicator.orchestrator.service.DistributedLockService lockService,
            @Autowired(required = false) org.springframework.transaction.PlatformTransactionManager transactionManager
    ) {
        this.hmsJobRepository = hmsJobRepository;
        this.hmsEventLogRepository = hmsEventLogRepository;
        this.hmsDeadLetterEventRepository = hmsDeadLetterEventRepository;
        this.agentRegistry = agentRegistry;
        this.jobService = jobService;
        this.properties = properties;
        this.lockService = lockService;
        this.txTemplate = transactionManager != null
                ? new org.springframework.transaction.support.TransactionTemplate(transactionManager)
                : null;
    }

    public HmsCoordinatorService(
            HmsReplicationJobRepository hmsJobRepository,
            HmsEventLogRepository hmsEventLogRepository,
            AgentRegistry agentRegistry,
            JobService jobService,
            org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties properties
    ) {
        this(hmsJobRepository, hmsEventLogRepository, null, agentRegistry, jobService, properties, null, null);
    }

    public HmsCoordinatorService(
            HmsReplicationJobRepository hmsJobRepository,
            HmsEventLogRepository hmsEventLogRepository,
            AgentRegistry agentRegistry,
            JobService jobService
    ) {
        this(hmsJobRepository, hmsEventLogRepository, null, agentRegistry, jobService, new org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties(), null, null);
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
        return createAndStartReplication(sourceClusterId, targetClusterId, sourceDb, targetDb, tablePattern,
                author, dropExtraneousTables, dropExtraneousPartitions, executionPrincipal, false);
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
            String executionPrincipal,
            boolean enableHdfsStreaming
    ) {
        if (sourceClusterId == null || sourceClusterId.isBlank()) {
            throw new IllegalArgumentException("Параметр sourceClusterId обязателен для создания задачи HMS репликации");
        }
        if (targetClusterId == null || targetClusterId.isBlank()) {
            throw new IllegalArgumentException("Параметр targetClusterId обязателен для создания задачи HMS репликации");
        }
        String id = "hms-job-" + sourceDb + "-" + UUID.randomUUID().toString().substring(0, 8);

        HmsReplicationJobEntity entity = new HmsReplicationJobEntity();
        entity.setId(id);
        entity.setSourceClusterId(sourceClusterId);
        entity.setTargetClusterId(targetClusterId);
        entity.setSourceDbName(sourceDb);
        entity.setTargetDbName(targetDb != null ? targetDb : sourceDb);
        entity.setTableIncludePattern(tablePattern != null ? tablePattern : "*");
        entity.setCreatedBy(author != null ? author : "system");
        entity.setExecutionPrincipal(executionPrincipal != null && !executionPrincipal.isBlank() ? executionPrincipal : null);
        entity.setDropExtraneousTables(dropExtraneousTables);
        entity.setDropExtraneousPartitions(dropExtraneousPartitions);

        if (enableHdfsStreaming) {
            boolean hasStreamer = agentRegistry != null && agentRegistry.hasLiveStreamerForCluster(sourceClusterId);
            if (!hasStreamer) {
                throw new IllegalArgumentException(
                    "На исходном кластере '" + sourceClusterId + "' отсутствует зарегистрированный Inotify-агент " +
                    "(невозможно создать задачу со связанным HDFS Inotify-стримингом). " +
                    "Отключите опцию стриминга для создания стандартной задачи репликации схемы или запустите Inotify-стример."
                );
            }
        }
        entity.setEnableHdfsStreaming(enableHdfsStreaming);
        entity.setCompanionStreamerOnline(enableHdfsStreaming);

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
                : null;
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
        List<HmsReplicationJobEntity> allJobs = hmsJobRepository.findByStatusIn(List.of("QUEUED", "BOOTSTRAPPING", "ACTIVE"));
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
                    job.getExecutionPrincipal(),
                    job.isEnableHdfsStreaming(),
                    job.getCompanionHdfsJobId(),
                    job.getDbWarehouseLocation()
            ));
        }

        return result;
    }

    /**
     * Прием отчета о прогрессе и DDL логов от Replicator Agent.
     */
    @Transactional
    public boolean updateJobProgress(String jobId, HmsProgressReportRequest report) {
        Optional<HmsReplicationJobEntity> optJob = hmsJobRepository.findById(jobId);
        if (optJob.isEmpty()) {
            log.warn("[HmsCoordinator] Попытка обновить прогресс несуществующей задачи {}", jobId);
            return false;
        }

        HmsReplicationJobEntity job = optJob.get();

        // Preemptive Fencing: отклонение прогресса и попытки записи для остановленной задачи (STOPPED)
        if ("STOPPED".equalsIgnoreCase(job.getStatus())) {
            log.warn("[HmsCoordinator] Fencing: отклонен отчет прогресса для остановленной задачи {} (STOPPED)", jobId);
            return false;
        }

        // Fencing по владельцу аренды: отчет от агента, у которого задачу уже отобрали,
        // иначе «подвисший» агент вернул бы себе владение и откатил позицию CDC назад.
        String owner = job.getAssignedAgentId();
        String reporter = report.agentId();
        if (owner != null && !owner.isBlank() && reporter != null && !reporter.isBlank()
                && !owner.equals(reporter)) {
            log.warn("[HmsCoordinator] Fencing: отклонен отчет агента '{}' для задачи {}: аренду держит '{}'",
                    reporter, jobId, owner);
            return false;
        }

        if (report.status() != null && !report.status().isBlank()) {
            org.apache.hadoop.explorer.replicator.model.HmsJobStatus targetStatus =
                    org.apache.hadoop.explorer.replicator.model.HmsJobStatus.fromString(report.status());
            if (job.getJobStatus().canTransitionTo(targetStatus)) {
                job.transitionTo(targetStatus);
            } else {
                log.warn("[HmsCoordinator] Проигнорирован невалидный переход статуса задачи {}: '{}' -> '{}'",
                        jobId, job.getStatus(), targetStatus);
            }
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
            // Позиция в потоке событий монотонна: откат назад переиграл бы уже примененные
            // DDL-события (повторный DROP_PARTITION после того, как новый агент ушел вперед).
            Long currentPosition = job.getLastProcessedEventId();
            if (currentPosition == null || report.lastProcessedEventId() >= currentPosition) {
                job.setLastProcessedEventId(report.lastProcessedEventId());
            } else {
                log.warn("[HmsCoordinator] Отклонен откат позиции событий задачи {}: {} -> {}",
                        jobId, currentPosition, report.lastProcessedEventId());
            }
        }
        if (report.bootstrapEventId() != null) {
            job.setBootstrapEventId(report.bootstrapEventId());
        }
        if (report.eventLag() != null) {
            job.setEventLag(report.eventLag());
        }
        // Сохранение DDL событий в историю аудита батчем
        if (report.events() != null && !report.events().isEmpty()) {
            List<HmsEventLogEntity> entities = new ArrayList<>(report.events().size());
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
                entities.add(eventEntity);

                // Автоматическая фиксация в Dead-Letter Queue при изоляции ядовитого события
                if ("DLQ_ISOLATED".equalsIgnoreCase(ev.status()) && hmsDeadLetterEventRepository != null) {
                    recordDeadLetterEvent(jobId, ev.eventId(), ev.eventType(), ev.tableName(), ev.partitionName(), null, ev.errorMessage());
                }
            }
            hmsEventLogRepository.saveAll(entities);
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
        return true;
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
     * Удаление задачи репликации, истории событий, записей DLQ и связанных HDFS саб-джоб.
     */
    @Transactional
    public boolean deleteReplicationJob(String id) {
        if (!hmsJobRepository.existsById(id)) {
            return false;
        }
        hmsEventLogRepository.deleteByHmsJobId(id);
        if (hmsDeadLetterEventRepository != null) {
            hmsDeadLetterEventRepository.deleteByHmsJobId(id);
        }
        jobService.deleteSubjobsByParentId(id);
        hmsJobRepository.deleteById(id);
        log.info("[HmsCoordinator] Задача {} и связанные ресурсы удалены", id);
        return true;
    }

    /**
     * Привязка связанного фонового HDFS Inotify-стримера к задаче HMS репликации.
     */
    @Transactional
    public void linkCompanionHdfsJob(String hmsJobId, String companionJobId, String warehouseLocation) {
        if (hmsJobId == null || hmsJobId.isBlank()) return;
        hmsJobRepository.findById(hmsJobId).ifPresent(job -> {
            job.setCompanionHdfsJobId(companionJobId);
            if (warehouseLocation != null && !warehouseLocation.isBlank()) {
                job.setDbWarehouseLocation(warehouseLocation);
            }
            hmsJobRepository.save(job);
            log.info("[HmsCoordinator] К задаче HMS '{}' привязан Companion HDFS Inotify стример '{}' (путь: '{}')",
                    hmsJobId, companionJobId, warehouseLocation);
        });
    }

    /**
     * Приостановка связанного фонового HDFS стримера при паузе задачи HMS.
     */
    @Transactional
    public void pauseCompanionJob(HmsReplicationJobEntity job) {
        if (job != null && job.getCompanionHdfsJobId() != null && !job.getCompanionHdfsJobId().isBlank()) {
            try {
                jobService.stopJob(job.getCompanionHdfsJobId());
                log.info("[HmsCoordinator] Приостановлен Companion HDFS стример '{}' для задачи HMS '{}'",
                        job.getCompanionHdfsJobId(), job.getId());
            } catch (Exception e) {
                log.warn("[HmsCoordinator] Ошибка приостановки Companion HDFS стримера '{}': {}",
                        job.getCompanionHdfsJobId(), e.getMessage());
            }
        }
    }

    /**
     * Возобновление связанного фонового HDFS стримера при снятии задачи HMS с паузы.
     */
    @Transactional
    public void resumeCompanionJob(HmsReplicationJobEntity job) {
        if (job != null && job.getCompanionHdfsJobId() != null && !job.getCompanionHdfsJobId().isBlank()) {
            try {
                jobService.startJob(job.getCompanionHdfsJobId());
                log.info("[HmsCoordinator] Возобновлен Companion HDFS стример '{}' для задачи HMS '{}'",
                        job.getCompanionHdfsJobId(), job.getId());
            } catch (Exception e) {
                log.warn("[HmsCoordinator] Ошибка возобновления Companion HDFS стримера '{}': {}",
                        job.getCompanionHdfsJobId(), e.getMessage());
            }
        }
    }

    /**
     * Регистрация или инкремент попытки события в Dead-Letter Queue (DLQ).
     */
    @Transactional
    public void recordDeadLetterEvent(
            String jobId,
            Long eventId,
            String eventType,
            String tableName,
            String partitionName,
            String payload,
            String errorMessage
    ) {
        if (hmsDeadLetterEventRepository == null) return;
        var existing = hmsDeadLetterEventRepository.findByHmsJobIdAndEventId(jobId, eventId);
        if (existing.isPresent()) {
            var entity = existing.get();
            entity.setRetryCount(entity.getRetryCount() + 1);
            entity.setErrorMessage(errorMessage);
            entity.setFailedAt(Instant.now());
            entity.setStatus("NEW");
            hmsDeadLetterEventRepository.save(entity);
            log.warn("[HmsCoordinator] Обновлено событие в DLQ: job={}, eventId={}, attempts={}",
                    jobId, eventId, entity.getRetryCount());
        } else {
            var entity = new org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsDeadLetterEventEntity(
                    UUID.randomUUID().toString(),
                    jobId,
                    eventId,
                    eventType != null ? eventType : "UNKNOWN",
                    tableName,
                    partitionName,
                    payload,
                    errorMessage
            );
            hmsDeadLetterEventRepository.save(entity);
            log.warn("[HmsCoordinator] Новое ядовитое событие помещено в DLQ: job={}, eventId={}, table={}, error={}",
                    jobId, eventId, tableName, errorMessage);
        }
    }

    /**
     * Получение событий из DLQ с постраничностью и фильтром по статусу.
     */
    public List<org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsDeadLetterEventEntity> getDeadLetterEvents(
            String jobId,
            String status,
            int limit
    ) {
        if (hmsDeadLetterEventRepository == null) return Collections.emptyList();
        org.springframework.data.domain.Pageable pageable =
                org.springframework.data.domain.PageRequest.of(0, Math.min(Math.max(limit, 1), 500));
        if (status != null && !status.isBlank() && !"ALL".equalsIgnoreCase(status)) {
            return hmsDeadLetterEventRepository.findByHmsJobIdAndStatusOrderByFailedAtDesc(jobId, status.toUpperCase(), pageable);
        }
        return hmsDeadLetterEventRepository.findByHmsJobIdOrderByFailedAtDesc(jobId, pageable);
    }

    /**
     * Пропуск события в DLQ (пометка как IGNORED).
     */
    @Transactional
    public boolean skipDeadLetterEvent(String jobId, String dlqId) {
        if (hmsDeadLetterEventRepository == null) return false;
        return hmsDeadLetterEventRepository.findById(dlqId)
                .filter(e -> e.getHmsJobId().equals(jobId))
                .map(e -> {
                    e.setStatus("IGNORED");
                    hmsDeadLetterEventRepository.save(e);
                    log.info("[HmsCoordinator] Событие DLQ {} для задачи {} помечено как IGNORED", dlqId, jobId);
                    return true;
                }).orElse(false);
    }

    /**
     * Повтор события из DLQ (пометка как REPLAYED).
     */
    @Transactional
    public boolean replayDeadLetterEvent(String jobId, String dlqId) {
        if (hmsDeadLetterEventRepository == null) return false;
        return hmsDeadLetterEventRepository.findById(dlqId)
                .filter(e -> e.getHmsJobId().equals(jobId))
                .map(e -> {
                    e.setStatus("REPLAYED");
                    e.setRetryCount(e.getRetryCount() + 1);
                    hmsDeadLetterEventRepository.save(e);
                    log.info("[HmsCoordinator] Событие DLQ {} для задачи {} помечено как REPLAYED", dlqId, jobId);
                    return true;
                }).orElse(false);
    }

    /**
     * Отчет о дрейфе схемы по журналу задачи.
     *
     * <p>Расхождения находит агент источника при переносе: он единственный видит и свой метастор,
     * и метастор приемника, и сравнивает схемы до применения DDL. Найденное он присылает
     * записями журнала со статусом {@code SCHEMA_DRIFT}, откуда и собирается отчет — по одной
     * последней записи на таблицу. Прежняя реализация возвращала фиксированный «дрейфа нет» с
     * именем таблицы из шаблона включения, то есть интерфейс показывал зеленый статус всегда.
     * Пустой список означает, что расхождений в журнале нет, а не что их не бывает.
     */
    public List<org.apache.hadoop.explorer.replicator.model.HmsSchemaDriftReport> scanSchemaDrift(String jobId) {
        HmsReplicationJobEntity job = hmsJobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Задача репликации HMS не найдена: " + jobId));

        List<HmsEventLogEntity> driftEvents = hmsEventLogRepository.findByHmsJobIdAndStatus(
                jobId, org.apache.hadoop.explorer.replicator.model.HmsSchemaDriftReport.EVENT_STATUS);

        if (driftEvents.isEmpty()) {
            log.info("[HmsCoordinator] Расхождений схем для задачи {} ({}) в журнале нет",
                    jobId, job.getSourceDbName());
            return List.of();
        }

        Map<String, HmsEventLogEntity> latestPerTable = new LinkedHashMap<>();
        driftEvents.stream()
                .sorted(Comparator.comparing(HmsEventLogEntity::getCreatedAt,
                                Comparator.nullsFirst(Comparator.naturalOrder()))
                        .thenComparing(HmsEventLogEntity::getId, Comparator.nullsFirst(Comparator.naturalOrder())))
                .forEach(event -> latestPerTable.put(event.getTableName(), event));

        log.info("[HmsCoordinator] Дрейф схемы для задачи {}: расхождения по {} таблицам",
                jobId, latestPerTable.size());

        return latestPerTable.values().stream()
                .map(event -> new org.apache.hadoop.explorer.replicator.model.HmsSchemaDriftReport(
                        job.getTargetDbName(),
                        event.getTableName(),
                        true,
                        org.apache.hadoop.explorer.replicator.model.HmsSchemaDriftReport
                                .splitDiscrepancies(event.getErrorMessage()),
                        event.getCreatedAt()))
                .toList();
    }

    /**
     * Фоновый периодический мониторинг зависших или брошенных HMS задач с защитой от параллельного запуска.
     */
    @Scheduled(fixedDelayString = "${hadoop.replicator.hms-failover-check-ms:10000}")
    public void scheduledCheckAndFailoverOrphanedHmsJobs() {
        if (lockService != null) {
            lockService.runWithLock("hms_failover_check_lock", java.time.Duration.ofSeconds(4),
                    this::runOrphanedHmsFailoverInTransaction);
        } else {
            runOrphanedHmsFailoverInTransaction();
        }
    }

    /**
     * Проход failover в явной транзакции.
     *
     * <p>Действие под блокировкой передается ссылкой на свой же метод, то есть вызов идет мимо
     * прокси и {@code @Transactional} на {@code checkAndFailoverOrphanedHmsJobs} не действует.
     * Без транзакции снятие аренды, запись журнала и переназначение задачи фиксируются порознь,
     * и сбой на середине прохода оставляет задачу без владельца со старой позицией в CDC.
     */
    private void runOrphanedHmsFailoverInTransaction() {
        if (txTemplate != null) {
            txTemplate.executeWithoutResult(status -> checkAndFailoverOrphanedHmsJobs());
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
        List<HmsReplicationJobEntity> allJobs = hmsJobRepository.findByAssignedAgentIdIsNotNullAndStatusIn(
                List.of("BOOTSTRAPPING", "ACTIVE", "QUEUED")
        );
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
                // Решение по самому heartbeat: поле статуса обновляет планировщик
                // с задержкой, и failover не должен ее ждать
                if (!agentRegistry.isAgentLive(assignedAgent)) {
                    shouldFailover = true;
                    reason = "Агент '" + assignedAgent + "' не присылает heartbeat дольше порога OFFLINE";
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
        // Выборка по агенту запросом: прежний проход читал таблицу задач целиком, а эвакуация
        // случается на каждом отключении агента
        List<HmsReplicationJobEntity> assigned = hmsJobRepository.findByAssignedAgentId(agentId);
        int count = 0;
        for (HmsReplicationJobEntity job : assigned) {
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

    /**
     * Доступность агента по времени последнего heartbeat.
     *
     * <p>Поле статуса в реестре обновляет периодический планировщик, поэтому для failover
     * и выдачи задач оно может отставать. Решение принимается по самому heartbeat.
     */
    private boolean isAgentOnline(String agentId) {
        return agentRegistry.isAgentLive(agentId);
    }

    /**
     * Сопоставление идентификаторов кластеров: нормализация разделителей и регистра,
     * но без сравнения подстрокой.
     *
     * <p>Проверка {@code contains} позволяла агенту кластера `prod-dc1-backup` забрать задачу
     * кластера `prod-dc1`: он читал CDC чужого метастора и применял DDL в чужом датацентре.
     */
    private boolean matchesCluster(String c1, String c2) {
        if (c1 == null || c2 == null) return false;
        if (c1.equalsIgnoreCase(c2)) return true;
        String n1 = c1.toLowerCase().replace("-", "").replace("_", "");
        String n2 = c2.toLowerCase().replace("-", "").replace("_", "");
        return n1.equals(n2);
    }

    /**
     * Обогащение одной задачи HMS метриками первичной синхронизации (скорость, время старта, расчетное окончание).
     */
    public void enrichWithSubtasksMetrics(HmsReplicationJobEntity job) {
        if (job == null) return;
        List<JobResponse> subtasks = jobService.listSubjobsByParentId(job.getId());
        enrichJobWithSubtasks(job, subtasks);
    }

    /**
     * Обогащение списка задач HMS метриками первичной синхронизации (пакетный запрос без N+1).
     */
    public void enrichAllWithSubtasksMetrics(List<HmsReplicationJobEntity> jobs) {
        if (jobs == null || jobs.isEmpty()) return;
        List<String> parentIds = jobs.stream().map(HmsReplicationJobEntity::getId).toList();
        Map<String, List<JobResponse>> subtasksByParent = jobService.listSubjobsByParentIds(parentIds);
        for (HmsReplicationJobEntity job : jobs) {
            List<JobResponse> subtasks = subtasksByParent.getOrDefault(job.getId(), Collections.emptyList());
            enrichJobWithSubtasks(job, subtasks);
        }
    }

    private void enrichJobWithSubtasks(HmsReplicationJobEntity job, List<JobResponse> subtasks) {
        if (subtasks != null && !subtasks.isEmpty()) {
            long totalBytes = 0;
            long copiedBytes = 0;
            Instant minStartedAt = null;
            Instant maxCompletedAt = null;
            int activeCount = 0;
            double speedSum = 0;
            int speedCount = 0;

            for (JobResponse sub : subtasks) {
                totalBytes += sub.totalBytes();
                copiedBytes += sub.copiedBytes();
                String st = sub.status();
                if ("RUNNING".equalsIgnoreCase(st) || "QUEUED".equalsIgnoreCase(st)) {
                    activeCount++;
                }
                Instant subStart = sub.startedAt() != null ? sub.startedAt() : sub.createdAt();
                if (subStart != null) {
                    if (minStartedAt == null || subStart.isBefore(minStartedAt)) {
                        minStartedAt = subStart;
                    }
                }
                if (sub.completedAt() != null) {
                    if (maxCompletedAt == null || sub.completedAt().isAfter(maxCompletedAt)) {
                        maxCompletedAt = sub.completedAt();
                    }
                }
                if (sub.averageSpeedMbS() != null && sub.averageSpeedMbS() > 0) {
                    speedSum += sub.averageSpeedMbS();
                    speedCount++;
                }
            }

            if (minStartedAt == null) {
                minStartedAt = job.getCreatedAt();
            }
            job.setStartedAt(minStartedAt);
            job.setTotalBytes(totalBytes);
            job.setCopiedBytes(copiedBytes);
            job.setSubtasksCount(subtasks.size());
            job.setActiveSubtasksCount(activeCount);

            double progress = totalBytes > 0
                    ? Math.min(100.0, Math.round(((double) copiedBytes / totalBytes) * 10000.0) / 100.0)
                    : (job.getTotalTables() > 0 ? Math.min(100.0, ((double) job.getReplicatedTables() / job.getTotalTables()) * 100.0) : 0.0);
            job.setProgressPercent(progress);

            Instant end = (activeCount == 0 && maxCompletedAt != null) ? maxCompletedAt : Instant.now();
            long elapsedSec = Math.max(1, end.getEpochSecond() - minStartedAt.getEpochSecond());
            Double avgSpeed = null;
            if (copiedBytes > 0 && elapsedSec > 0) {
                double mbS = ((double) copiedBytes / (1024.0 * 1024.0)) / elapsedSec;
                avgSpeed = Math.round(mbS * 100.0) / 100.0;
            } else if (speedCount > 0) {
                avgSpeed = Math.round((speedSum / speedCount) * 100.0) / 100.0;
            }
            job.setAverageSpeedMbS(avgSpeed);

            if (activeCount == 0 && maxCompletedAt != null && progress >= 100.0) {
                job.setCompletedAt(maxCompletedAt);
                job.setEstimatedCompletionAt(maxCompletedAt);
            } else if (avgSpeed != null && avgSpeed > 0 && totalBytes > copiedBytes) {
                long remainingBytes = totalBytes - copiedBytes;
                double remainingSec = (double) remainingBytes / (avgSpeed * 1024.0 * 1024.0);
                job.setEstimatedCompletionAt(Instant.now().plusSeconds((long) Math.ceil(remainingSec)));
            }
        } else if ("BOOTSTRAPPING".equalsIgnoreCase(job.getStatus())) {
            job.setStartedAt(job.getCreatedAt());
            double progress = job.getTotalTables() > 0 ? Math.min(100.0, ((double) job.getReplicatedTables() / job.getTotalTables()) * 100.0) : 0.0;
            job.setProgressPercent(progress);
        }

        if (job.isEnableHdfsStreaming()) {
            boolean online = agentRegistry != null && agentRegistry.hasLiveStreamerForCluster(job.getSourceClusterId());
            job.setCompanionStreamerOnline(online);
        }
    }
}
