package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.AgentResponseDto;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrActionResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyStopRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrReverseRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrStatusResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.TopologyResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobRunEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.DrSnapshotEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.DrSnapshotRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyRollbackRequest;

@Service
public class DisasterRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(DisasterRecoveryService.class);

    public static class EmergencySnapshot {
        private String clusterId;
        private String operator;
        private String reason;
        private Instant timestamp;
        private boolean networkFenced;
        private Map<String, Long> previousDcLimits = new HashMap<>();
        private List<String> stoppedHdfsJobIds = new ArrayList<>();
        private Map<String, Boolean> previousHdfsScheduled = new HashMap<>();
        private List<String> pausedHmsJobIds = new ArrayList<>();

        public EmergencySnapshot() {}

        @JsonCreator
        public EmergencySnapshot(
                @JsonProperty("clusterId") String clusterId,
                @JsonProperty("operator") String operator,
                @JsonProperty("reason") String reason,
                @JsonProperty("timestamp") Instant timestamp,
                @JsonProperty("networkFenced") boolean networkFenced) {
            this.clusterId = clusterId;
            this.operator = operator;
            this.reason = reason;
            this.timestamp = timestamp;
            this.networkFenced = networkFenced;
        }

        public String getClusterId() { return clusterId; }
        public void setClusterId(String clusterId) { this.clusterId = clusterId; }
        public String getOperator() { return operator; }
        public void setOperator(String operator) { this.operator = operator; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
        public Instant getTimestamp() { return timestamp; }
        public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
        public boolean isNetworkFenced() { return networkFenced; }
        public void setNetworkFenced(boolean networkFenced) { this.networkFenced = networkFenced; }
        public Map<String, Long> getPreviousDcLimits() { return previousDcLimits; }
        public void setPreviousDcLimits(Map<String, Long> previousDcLimits) { this.previousDcLimits = previousDcLimits; }
        public List<String> getStoppedHdfsJobIds() { return stoppedHdfsJobIds; }
        public void setStoppedHdfsJobIds(List<String> stoppedHdfsJobIds) { this.stoppedHdfsJobIds = stoppedHdfsJobIds; }
        public Map<String, Boolean> getPreviousHdfsScheduled() { return previousHdfsScheduled; }
        public void setPreviousHdfsScheduled(Map<String, Boolean> previousHdfsScheduled) { this.previousHdfsScheduled = previousHdfsScheduled; }
        public List<String> getPausedHmsJobIds() { return pausedHmsJobIds; }
        public void setPausedHmsJobIds(List<String> pausedHmsJobIds) { this.pausedHmsJobIds = pausedHmsJobIds; }
    }

    private final Map<String, EmergencySnapshot> activeSnapshots = new ConcurrentHashMap<>();

    private final JobRepository jobRepository;
    private final JobRunRepository jobRunRepository;
    private final TaskRepository taskRepository;
    private final HmsReplicationJobRepository hmsReplicationJobRepository;
    private final AgentRegistry agentRegistry;
    private final TopologyRegistry topologyRegistry;
    private final TokenBucketThrottler throttler;
    private final HmsCoordinatorService hmsCoordinatorService;
    private final DrSnapshotRepository drSnapshotRepository;
    private final DistributedLockService lockService;
    private final org.apache.hadoop.explorer.replicator.orchestrator.repository.BandwidthLimitRepository limitRepository;
    private final ObjectMapper objectMapper;

    public DisasterRecoveryService(
            JobRepository jobRepository,
            JobRunRepository jobRunRepository,
            TaskRepository taskRepository,
            HmsReplicationJobRepository hmsReplicationJobRepository,
            AgentRegistry agentRegistry,
            TopologyRegistry topologyRegistry,
            TokenBucketThrottler throttler,
            HmsCoordinatorService hmsCoordinatorService
    ) {
        this(jobRepository, jobRunRepository, taskRepository, hmsReplicationJobRepository,
                agentRegistry, topologyRegistry, throttler, hmsCoordinatorService, null, null, null);
    }

    public DisasterRecoveryService(
            JobRepository jobRepository,
            JobRunRepository jobRunRepository,
            TaskRepository taskRepository,
            HmsReplicationJobRepository hmsReplicationJobRepository,
            AgentRegistry agentRegistry,
            TopologyRegistry topologyRegistry,
            TokenBucketThrottler throttler,
            HmsCoordinatorService hmsCoordinatorService,
            DrSnapshotRepository drSnapshotRepository
    ) {
        this(jobRepository, jobRunRepository, taskRepository, hmsReplicationJobRepository,
                agentRegistry, topologyRegistry, throttler, hmsCoordinatorService, drSnapshotRepository, null, null);
    }

    public DisasterRecoveryService(
            JobRepository jobRepository,
            JobRunRepository jobRunRepository,
            TaskRepository taskRepository,
            HmsReplicationJobRepository hmsReplicationJobRepository,
            AgentRegistry agentRegistry,
            TopologyRegistry topologyRegistry,
            TokenBucketThrottler throttler,
            HmsCoordinatorService hmsCoordinatorService,
            DrSnapshotRepository drSnapshotRepository,
            DistributedLockService lockService
    ) {
        this(jobRepository, jobRunRepository, taskRepository, hmsReplicationJobRepository,
                agentRegistry, topologyRegistry, throttler, hmsCoordinatorService,
                drSnapshotRepository, lockService, null);
    }

    @Autowired
    public DisasterRecoveryService(
            JobRepository jobRepository,
            JobRunRepository jobRunRepository,
            TaskRepository taskRepository,
            HmsReplicationJobRepository hmsReplicationJobRepository,
            AgentRegistry agentRegistry,
            TopologyRegistry topologyRegistry,
            TokenBucketThrottler throttler,
            HmsCoordinatorService hmsCoordinatorService,
            @Autowired(required = false) DrSnapshotRepository drSnapshotRepository,
            @Autowired(required = false) DistributedLockService lockService,
            @Autowired(required = false) org.apache.hadoop.explorer.replicator.orchestrator.repository.BandwidthLimitRepository limitRepository
    ) {
        this.jobRepository = jobRepository;
        this.jobRunRepository = jobRunRepository;
        this.taskRepository = taskRepository;
        this.hmsReplicationJobRepository = hmsReplicationJobRepository;
        this.agentRegistry = agentRegistry;
        this.topologyRegistry = topologyRegistry;
        this.throttler = throttler;
        this.hmsCoordinatorService = hmsCoordinatorService;
        this.drSnapshotRepository = drSnapshotRepository;
        this.lockService = lockService;
        this.limitRepository = limitRepository;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }


    /**
     * Установка лимита канала между ЦОД с сохранением в БД.
     *
     * <p>Запись только в память своего узла означала, что за балансировщиком ограждение и
     * смена полосы применялись лишь на том узле, который принял запрос: остальные продолжали
     * выдавать агентам токены по прежнему лимиту, а после перезапуска узла ограждение
     * исчезало совсем. Остальные узлы подхватывают запись в своем цикле координации.
     */
    private void applyDcLimit(String srcDc, String dstDc, long bytesPerSec) {
        topologyRegistry.setDcLimit(srcDc, dstDc, bytesPerSec);
        if (limitRepository == null) {
            return;
        }
        try {
            String key = "dc:" + srcDc + "->" + dstDc;
            limitRepository.save(new org.apache.hadoop.explorer.replicator.orchestrator.entity.BandwidthLimitEntity(
                    key, bytesPerSec, java.time.Instant.now()));
        } catch (Exception e) {
            log.warn("Не удалось сохранить лимит канала {} -> {} в БД: {}", srcDc, dstDc, e.getMessage());
        }
    }


    /** Снимок ограждения из БД: нужен узлу, который сам ограждение не выставлял. */
    private EmergencySnapshot loadSnapshotFromDatabase(String snapshotKey) {
        try {
            return drSnapshotRepository.findById(snapshotKey)
                    .map(DrSnapshotEntity::getSnapshotJson)
                    .filter(json -> json != null && !json.isBlank())
                    .map(json -> {
                        try {
                            return objectMapper.readValue(json, EmergencySnapshot.class);
                        } catch (Exception e) {
                            log.warn("Не удалось разобрать снимок ограждения '{}': {}", snapshotKey, e.getMessage());
                            return null;
                        }
                    })
                    .orElse(null);
        } catch (Exception e) {
            log.warn("Не удалось прочитать снимок ограждения '{}' из БД: {}", snapshotKey, e.getMessage());
            return null;
        }
    }

    /** Сброс локального кэша снимков: имитация узла, не создававшего ограждение. */
    void forgetLocalSnapshotsForTesting() {
        activeSnapshots.clear();
    }

    @PostConstruct
    public void loadPersistedSnapshots() {
        if (drSnapshotRepository == null) return;
        try {
            List<DrSnapshotEntity> entities = drSnapshotRepository.findAll();
            for (DrSnapshotEntity entity : entities) {
                if (entity.getSnapshotJson() != null && !entity.getSnapshotJson().isBlank()) {
                    EmergencySnapshot snapshot = objectMapper.readValue(entity.getSnapshotJson(), EmergencySnapshot.class);
                    activeSnapshots.put(entity.getId(), snapshot);
                    if (snapshot.isNetworkFenced() && snapshot.getClusterId() != null) {
                        String clusterId = snapshot.getClusterId();
                        for (var dc : topologyRegistry.getDatacenters()) {
                            if (!matchesDc(dc.getId(), clusterId)) {
                                applyDcLimit(clusterId, dc.getId(), 0L);
                                log.warn("[Disaster Recovery Fencing Restored] Восстановлена сетевая блокировка {} ➔ {} (0 МБ/с)", clusterId, dc.getId());
                            }
                        }
                    }
                }
            }
            if (!entities.isEmpty()) {
                log.info("[Disaster Recovery] Восстановлено {} активных DR-снимков из базы данных", entities.size());
            }
        } catch (Exception e) {
            log.warn("[Disaster Recovery] Не удалось восстановить DR-снимки из БД: {}", e.getMessage());
        }
    }

    /**
     * Изоляция кластеров без обхода таблиц заданий.
     *
     * <p>{@link #getDrStatus()} собирает полную сводку DR и для этого читает задания — рендер
     * метрик вызывал его по разу на кластер плюс дважды на скрейп, то есть таблицы заданий
     * вычитывались целиком на каждом интервале Prometheus. Признак изоляции так дорого не
     * стоит: он складывается из нулевого лимита полосы к соседнему ЦОД и активного снимка
     * ограждения, а то и другое известно без заданий.
     *
     * @return изоляция по каждому кластеру топологии (или по ЦОД, если кластеры не заданы)
     */
    @Transactional(readOnly = true)
    public Map<String, Boolean> getClusterFencingStatus() {
        Map<String, Boolean> fencing = new LinkedHashMap<>();
        for (var cluster : topologyRegistry.getClusters()) {
            if (cluster.getId() != null && !cluster.getId().isBlank()) {
                fencing.put(cluster.getId(), isClusterFenced(cluster.getId()));
            }
        }
        if (fencing.isEmpty()) {
            for (var dc : topologyRegistry.getDatacenters()) {
                if (dc.getId() != null && !dc.getId().isBlank()) {
                    fencing.put(dc.getId(), isDcFenced(dc.getId()));
                }
            }
        }
        return fencing;
    }

    /**
     * Изолирован ли кластер: по снимку ограждения на нем самом либо по изоляции его ЦОД.
     */
    @Transactional(readOnly = true)
    public boolean isClusterFenced(String clusterId) {
        if (clusterId == null || clusterId.isBlank()) {
            return false;
        }
        boolean snapshotOnCluster = activeSnapshots.values().stream()
                .anyMatch(s -> s.getClusterId() != null && matchesCluster(s.getClusterId(), clusterId));
        if (snapshotOnCluster) {
            return true;
        }
        String dcId = topologyRegistry.getClusters().stream()
                .filter(c -> c.getId() != null && c.getId().equalsIgnoreCase(clusterId))
                .map(org.apache.hadoop.explorer.replicator.model.ClusterDto::getDcId)
                .filter(d -> d != null && !d.isBlank())
                .findFirst()
                .orElse(clusterId);
        return isDcFenced(dcId);
    }

    private boolean isDcFenced(String dcId) {
        if (dcId == null || dcId.isBlank()) {
            return false;
        }
        if (activeSnapshots.containsKey(dcId.toLowerCase())
                || activeSnapshots.values().stream()
                        .anyMatch(s -> s.getClusterId() != null && matchesDc(s.getClusterId(), dcId))) {
            return true;
        }
        for (var otherDc : topologyRegistry.getDatacenters()) {
            if (otherDc.getId() == null || otherDc.getId().equalsIgnoreCase(dcId)) {
                continue;
            }
            Optional<Long> limit = topologyRegistry.getDcLimit(dcId, otherDc.getId());
            if (limit.isPresent() && limit.get() == 0L) {
                return true;
            }
        }
        return false;
    }

    /**
     * Формирование сводного статуса всех дата-центров, кластеров, маршрутов и готовности к Failover.
     */
    @Transactional(readOnly = true)
    public DrStatusResponse getDrStatus() {
        List<AgentResponseDto> agents = agentRegistry.getAgentDtos();
        List<ReplicatorProperties.DatacenterConfig> dcs = topologyRegistry.getDatacenters();
        List<JobEntity> allJobs = jobRepository.findAll().stream()
                .filter(j -> j.getParentJobId() == null)
                .toList();
        List<HmsReplicationJobEntity> allHmsJobs = hmsReplicationJobRepository.findAll();

        // 1. Статус Дата-Центров
        List<DrStatusResponse.DrDcStatus> dcStatuses = new ArrayList<>();
        boolean primaryOnline = false;
        boolean standbyOnline = false;

        for (var dc : dcs) {
            String dcId = dc.getId();
            List<AgentResponseDto> dcAgents = agents.stream()
                    .filter(a -> matchesDc(a.clusterId(), dcId))
                    .toList();

            int onlineCount = (int) dcAgents.stream().filter(a -> "online".equalsIgnoreCase(a.status())).count();
            int totalCount = dcAgents.size();

            String status = onlineCount > 0 ? "ONLINE" : (totalCount > 0 ? "OFFLINE" : "UNKNOWN");
            if (onlineCount > 0 && onlineCount < totalCount) {
                status = "DEGRADED";
            }

            // Роль: Первый ДЦ в списке считается Primary по умолчанию, остальные — Standby (или Promoted)
            boolean isFirstDc = !dcs.isEmpty() && dcs.get(0).getId().equalsIgnoreCase(dcId);
            boolean isPrimary = isFirstDc || dcId.toLowerCase().contains("primary");
            boolean isStandby = (!isFirstDc && dcs.size() > 1 && dcs.get(1).getId().equalsIgnoreCase(dcId))
                    || dcId.toLowerCase().contains("standby") || dcId.toLowerCase().contains("dr");

            if (isPrimary && onlineCount > 0) primaryOnline = true;
            if (isStandby && onlineCount > 0) standbyOnline = true;

            // Проверяем направление активных задач: если из ДЦ идут активные задачи, значит ДЦ исполняет роль источника
            long outJobsFromDc = allJobs.stream()
                    .filter(j -> matchesDc(j.getSourceClusterId(), dcId) && "RUNNING".equalsIgnoreCase(j.getStatus()))
                    .count();

            String role;
            if (isPrimary) {
                role = (outJobsFromDc > 0 || totalCount == 0) ? "PRIMARY" : "STANDBY";
            } else {
                role = outJobsFromDc > 0 ? "PROMOTED_PRIMARY" : "STANDBY";
            }

            // Проверяем лимиты пропускной способности (Fencing)
            long limitBytes = 100L * 1024 * 1024;
            boolean isFenced = false;
            for (var otherDc : dcs) {
                if (!otherDc.getId().equalsIgnoreCase(dcId)) {
                    Optional<Long> optLimit = topologyRegistry.getDcLimit(dcId, otherDc.getId());
                    if (optLimit.isPresent()) {
                        long l = optLimit.get();
                        if (l == 0) {
                            isFenced = true;
                        } else {
                            limitBytes = l;
                        }
                    }
                }
            }
            boolean hasSnapshot = activeSnapshots.containsKey(dcId.toLowerCase())
                    || activeSnapshots.values().stream().anyMatch(s -> s.getClusterId() != null && matchesDc(s.getClusterId(), dcId));
            if (hasSnapshot) {
                isFenced = true;
                limitBytes = 0;
            }

            double limitMbS = Math.round((limitBytes / (1024.0 * 1024.0)) * 10.0) / 10.0;

            dcStatuses.add(new DrStatusResponse.DrDcStatus(
                    dcId,
                    dc.getName() != null ? dc.getName() : dcId.toUpperCase(),
                    status,
                    role,
                    onlineCount,
                    totalCount,
                    limitMbS,
                    isFenced
            ));
        }

        // 2. Статус Кластеров
        List<DrStatusResponse.DrClusterStatus> clusterStatuses = new ArrayList<>();
        for (var c : topologyRegistry.getClusters()) {
            String cId = c.getId();
            int active = 0, queued = 0, failed = 0, completed = 0;
            for (var j : allJobs) {
                if (matchesCluster(j.getSourceClusterId(), cId)) {
                    String st = j.getStatus() != null ? j.getStatus().toUpperCase() : "";
                    switch (st) {
                        case "RUNNING", "STREAMING" -> active++;
                        case "QUEUED", "ANALYZING" -> queued++;
                        case "FAILED" -> failed++;
                        case "COMPLETED" -> completed++;
                    }
                }
            }

            List<AgentResponseDto> clusterAgents = agents.stream()
                    .filter(a -> matchesCluster(a.clusterId(), cId))
                    .toList();
            boolean isOnline = clusterAgents.stream().anyMatch(a -> "online".equalsIgnoreCase(a.status()));

            boolean isClusterFenced = activeSnapshots.containsKey(cId.toLowerCase())
                    || activeSnapshots.values().stream().anyMatch(s -> s.getClusterId() != null && matchesCluster(s.getClusterId(), cId))
                    || dcStatuses.stream().anyMatch(dc -> dc.id().equalsIgnoreCase(c.getDcId()) && dc.isFenced());

            clusterStatuses.add(new DrStatusResponse.DrClusterStatus(
                    cId,
                    c.getName() != null ? c.getName() : cId,
                    c.getDcId(),
                    isOnline ? "ONLINE" : "OFFLINE",
                    active,
                    queued,
                    failed,
                    completed,
                    isClusterFenced
            ));
        }

        // 3. Маршруты HDFS
        List<DrStatusResponse.DrRouteItem> hdfsRoutes = new ArrayList<>();
        long totalUnreplicatedBytes = 0;
        int activeJobsCount = 0;
        int frozenJobsCount = 0;
        int failedJobsCount = 0;

        for (JobEntity j : allJobs) {
            String status = j.getStatus() != null ? j.getStatus().toUpperCase() : "UNKNOWN";
            if ("RUNNING".equalsIgnoreCase(status) || "STREAMING".equalsIgnoreCase(status)) activeJobsCount++;
            if ("STOPPED".equalsIgnoreCase(status) || "CANCELLED".equalsIgnoreCase(status)) frozenJobsCount++;
            if ("FAILED".equalsIgnoreCase(status)) failedJobsCount++;

            long lag = Math.max(0, j.getTotalBytes() - j.getCopiedBytes());
            totalUnreplicatedBytes += lag;

            // Поиск зеркальной обратной задачи
            Optional<JobEntity> reverse = allJobs.stream()
                    .filter(rj -> !rj.getId().equals(j.getId())
                            && matchesCluster(rj.getSourceClusterId(), j.getTargetClusterId())
                            && matchesCluster(rj.getTargetClusterId(), j.getSourceClusterId())
                            && matchesReversePath(rj, j))
                    .findFirst();

            boolean isReverseReplica = j.getId().startsWith("rev-")
                    || (j.getMessage() != null && j.getMessage().toLowerCase().contains("обратная репликация"));

            hdfsRoutes.add(new DrStatusResponse.DrRouteItem(
                    j.getId(),
                    "HDFS",
                    j.getSourcePath() + " ➔ " + j.getTargetPath(),
                    j.getSourceClusterId(),
                    j.getTargetClusterId(),
                    j.getSourcePath(),
                    j.getTargetPath(),
                    status,
                    j.isScheduled(),
                    j.getCronExpression(),
                    lag,
                    reverse.isPresent(),
                    reverse.map(JobEntity::getId).orElse(null),
                    reverse.map(JobEntity::getStatus).orElse(null),
                    isReverseReplica,
                    j.getMessage()
            ));
        }

        // 4. Маршруты HMS
        List<DrStatusResponse.DrRouteItem> hmsRoutes = new ArrayList<>();
        long totalUnreplicatedEvents = 0;

        for (HmsReplicationJobEntity h : allHmsJobs) {
            String status = h.getStatus() != null ? h.getStatus().toUpperCase() : "UNKNOWN";
            if ("ACTIVE".equalsIgnoreCase(status) || "BOOTSTRAPPING".equalsIgnoreCase(status)) activeJobsCount++;
            if ("PAUSED".equalsIgnoreCase(status)) frozenJobsCount++;
            if ("ERROR".equalsIgnoreCase(status)) failedJobsCount++;

            long eventLag = h.getEventLag() != null ? h.getEventLag() : 0L;
            totalUnreplicatedEvents += eventLag;

            Optional<HmsReplicationJobEntity> reverseHms = allHmsJobs.stream()
                    .filter(rh -> !rh.getId().equals(h.getId())
                            && matchesCluster(rh.getSourceClusterId(), h.getTargetClusterId())
                            && matchesCluster(rh.getTargetClusterId(), h.getSourceClusterId())
                            && Objects.equals(rh.getSourceDbName(), h.getTargetDbName()))
                    .findFirst();

            boolean isReverseReplica = h.getId().startsWith("rev-")
                    || (h.getMessage() != null && h.getMessage().toLowerCase().contains("обратная репликация"));

            hmsRoutes.add(new DrStatusResponse.DrRouteItem(
                    h.getId(),
                    "HMS",
                    "HMS: " + h.getSourceDbName() + " ➔ " + h.getTargetDbName(),
                    h.getSourceClusterId(),
                    h.getTargetClusterId(),
                    h.getSourceDbName(),
                    h.getTargetDbName(),
                    status,
                    true,
                    "CDC Event Stream",
                    eventLag,
                    reverseHms.isPresent(),
                    reverseHms.map(HmsReplicationJobEntity::getId).orElse(null),
                    reverseHms.map(HmsReplicationJobEntity::getStatus).orElse(null),
                    isReverseReplica,
                    h.getMessage()
            ));
        }

        // Определение активного направления
        String firstDcId = !dcs.isEmpty() ? dcs.get(0).getId() : null;
        String secondDcId = dcs.size() > 1 ? dcs.get(1).getId() : null;

        String activeSrcDc = firstDcId;
        String activeDstDc = secondDcId;
        if (secondDcId != null) {
            long secondDcSourceCount = allJobs.stream()
                    .filter(j -> matchesDc(j.getSourceClusterId(), secondDcId) && ("RUNNING".equalsIgnoreCase(j.getStatus()) || "QUEUED".equalsIgnoreCase(j.getStatus())))
                    .count();
            if (secondDcSourceCount > 0) {
                activeSrcDc = secondDcId;
                activeDstDc = firstDcId;
            }
        }

        boolean anyZeroLimit = topologyRegistry.getDcLimitsList().stream().anyMatch(l -> l.limitBytesPerSec() == 0L);
        boolean hasSnapshot = !activeSnapshots.isEmpty();
        boolean killSwitchActive = hasSnapshot || anyZeroLimit;
        String fencedClusterId = activeSnapshots.keySet().stream().findFirst()
                .orElseGet(() -> topologyRegistry.getDcLimitsList().stream()
                        .filter(l -> l.limitBytesPerSec() == 0L)
                        .map(TopologyResponse.DcLimitDto::sourceDc)
                        .findFirst()
                        .orElse(null));
        String lastReason = activeSnapshots.values().stream().findFirst().map(EmergencySnapshot::getReason).orElse(null);

        DrStatusResponse.DrSummary summary = new DrStatusResponse.DrSummary(
                activeSrcDc,
                activeDstDc,
                primaryOnline,
                standbyOnline,
                activeJobsCount,
                frozenJobsCount,
                failedJobsCount,
                totalUnreplicatedBytes,
                totalUnreplicatedEvents,
                killSwitchActive,
                fencedClusterId,
                lastReason
        );

        return new DrStatusResponse(dcStatuses, clusterStatuses, summary, hdfsRoutes, hmsRoutes);
    }

    /**
     * Экстренная остановка (Kill-Switch) всех репликаций из указанного кластера-источника.
     * Защищает целевой ЦОД от Split-Brain и блокирует очереди.
     */
    @Transactional
    public DrActionResponse emergencyStop(DrEmergencyStopRequest req, String username) {
        String clusterId = req != null && req.clusterId() != null ? req.clusterId() : "default";
        if (lockService != null) {
            String lockKey = "dr_emergency_" + clusterId.toLowerCase();
            final DrActionResponse[] result = new DrActionResponse[1];
            lockService.runWithLock(lockKey, java.time.Duration.ofSeconds(15), epoch -> {
                result[0] = doEmergencyStop(req, username);
            });
            if (result[0] != null) {
                return result[0];
            } else {
                return DrActionResponse.error(
                        "Аварийный останов для кластера '" + clusterId +
                                "' уже выполняется другим инстансом Orchestrator (блокировка удерживается)"
                );
            }
        } else {
            return doEmergencyStop(req, username);
        }
    }

    private DrActionResponse doEmergencyStop(DrEmergencyStopRequest req, String username) {
        String clusterId = req.clusterId();
        String operator = username != null ? username : "operator";
        String note = req.reason() != null && !req.reason().isBlank() ? ": " + req.reason() : "";
        Instant now = Instant.now();

        log.warn("[Disaster Recovery] АВАРИЙНЫЙ ОСТАНОВ (Kill-Switch) для кластера '{}' оператором '{}'{}",
                clusterId, operator, note);

        // 1. Создаем и сохраняем снимок состояния (Snapshot) для возможности точного отката
        EmergencySnapshot snapshot = new EmergencySnapshot(
                clusterId, operator, req.reason(), now, Boolean.TRUE.equals(req.fenceNetwork())
        );

        // Сохраняем лимиты пропускной способности каналов до ограждения
        for (var limit : topologyRegistry.getDcLimitsList()) {
            if (matchesDc(limit.sourceDc(), clusterId) || matchesDc(limit.targetDc(), clusterId)) {
                snapshot.getPreviousDcLimits().put(limit.sourceDc() + "->" + limit.targetDc(), limit.limitBytesPerSec());
            }
        }

        int stoppedHdfs = 0;
        List<JobEntity> jobs = jobRepository.findByNormalizedSourceClusterId(clusterId);
        for (JobEntity j : jobs) {
            if (matchesCluster(j.getSourceClusterId(), clusterId)) {
                String status = j.getStatus() != null ? j.getStatus().toUpperCase() : "";
                if ("RUNNING".equals(status) || "QUEUED".equals(status) || "ANALYZING".equals(status) || "STREAMING".equals(status)) {
                    snapshot.getStoppedHdfsJobIds().add(j.getId());
                    snapshot.getPreviousHdfsScheduled().put(j.getId(), j.isScheduled());

                    j.setStatus("STOPPED");
                    j.setScheduled(false); // Отключаем планировщик
                    j.setCompletedAt(now);
                    j.setMessage("Экстренная остановка (Kill-Switch) оператором " + operator + note);

                    if (j.getActiveRunId() != null) {
                        jobRunRepository.findById(j.getActiveRunId()).ifPresent(run -> {
                            run.setStatus("STOPPED");
                            run.setCompletedAt(now);
                            run.setMessage("Остановлено аварийным Kill-Switch");
                            run.calculateMetrics();
                            jobRunRepository.save(run);
                        });
                        j.setActiveRunId(null);
                    }
                    jobRepository.save(j);
                    stoppedHdfs++;
                } else if (j.isScheduled()) {
                    snapshot.getStoppedHdfsJobIds().add(j.getId());
                    snapshot.getPreviousHdfsScheduled().put(j.getId(), true);

                    // Если задача ждет по расписанию, снимаем расписание
                    j.setScheduled(false);
                    j.setMessage("Расписание деактивировано аварийным Kill-Switch");
                    jobRepository.save(j);
                    stoppedHdfs++;
                }
            }
        }

        int stoppedHms = 0;
        List<HmsReplicationJobEntity> hmsJobs = hmsReplicationJobRepository.findByNormalizedSourceClusterId(clusterId);
        for (HmsReplicationJobEntity h : hmsJobs) {
            if (matchesCluster(h.getSourceClusterId(), clusterId)) {
                if (!"PAUSED".equalsIgnoreCase(h.getStatus())) {
                    snapshot.getPausedHmsJobIds().add(h.getId());

                    h.setStatus("PAUSED");
                    h.setMessage("Приостановлено аварийным Kill-Switch оператором " + operator + note);
                    hmsReplicationJobRepository.save(h);
                    stoppedHms++;
                }
            }
        }

        // Сетевое ограждение (Fencing): блокировка лимита канала шейпера
        if (Boolean.TRUE.equals(req.fenceNetwork())) {
            for (var dc : topologyRegistry.getDatacenters()) {
                if (!matchesDc(dc.getId(), clusterId)) {
                    applyDcLimit(clusterId, dc.getId(), 0L);
                    log.warn("[Disaster Recovery Fencing] Канал полосы {} ➔ {} перекрыт (0 МБ/с)", clusterId, dc.getId());
                }
            }
        }

        // Регистрируем снимок для отката
        activeSnapshots.put(clusterId.toLowerCase(), snapshot);
        if (drSnapshotRepository != null) {
            try {
                String json = objectMapper.writeValueAsString(snapshot);
                drSnapshotRepository.save(new DrSnapshotEntity(
                        clusterId.toLowerCase(),
                        clusterId,
                        operator,
                        req.reason(),
                        snapshot.getTimestamp(),
                        snapshot.isNetworkFenced(),
                        json
                ));
            } catch (Exception e) {
                log.warn("[Disaster Recovery] Не удалось сохранить DR-снимок в БД: {}", e.getMessage());
            }
        }

        String msg = String.format("Аварийный останов выполнен: остановлено %d HDFS задач и %d HMS задач для кластера '%s'",
                stoppedHdfs, stoppedHms, clusterId);
        return DrActionResponse.stopSuccess(msg, stoppedHdfs, stoppedHms);
    }

    /**
     * Откат аварийного останова (Kill-Switch Rollback / Unfence):
     * Восстанавливает лимиты каналов шейпера, возвращает схемы HMS в ACTIVE и реактивирует расписания HDFS задач.
     */
    @Transactional
    public DrActionResponse rollbackEmergencyStop(DrEmergencyRollbackRequest req, String username) {
        String clusterId = req != null && req.clusterId() != null ? req.clusterId() : "default";
        if (lockService != null) {
            String lockKey = "dr_emergency_" + clusterId.toLowerCase();
            final DrActionResponse[] result = new DrActionResponse[1];
            lockService.runWithLock(lockKey, java.time.Duration.ofSeconds(15), epoch -> {
                result[0] = doRollbackEmergencyStop(req, username);
            });
            if (result[0] != null) {
                return result[0];
            } else {
                return DrActionResponse.error(
                        "Откат аварийного останова для кластера '" + clusterId +
                                "' уже выполняется другим инстансом Orchestrator (блокировка удерживается)"
                );
            }
        } else {
            return doRollbackEmergencyStop(req, username);
        }
    }

    private DrActionResponse doRollbackEmergencyStop(DrEmergencyRollbackRequest req, String username) {
        String clusterId = req.clusterId();
        String operator = username != null ? username : "operator";
        String snapshotKey = clusterId.toLowerCase();
        EmergencySnapshot snapshot = activeSnapshots.remove(snapshotKey);

        // Узел, не создававший ограждение, не держит снимок в памяти: берем его из БД.
        if (snapshot == null && drSnapshotRepository != null) {
            snapshot = loadSnapshotFromDatabase(snapshotKey);
        }

        // Снимка по запрошенному кластеру нет — это ошибка запроса. Прежний fallback брал
        // «первый снимок из мапы» и снимал ограждение с чужого кластера: откат по опечатке
        // оживлял репликацию в реально упавший ЦОД.
        if (snapshot == null) {
            log.warn("[Disaster Recovery] Откат отклонен: для кластера '{}' нет активного ограждения", clusterId);
            return DrActionResponse.error("Для кластера '" + clusterId + "' нет активного аварийного ограждения");
        }

        if (drSnapshotRepository != null) {
            try {
                drSnapshotRepository.deleteById(snapshotKey);
            } catch (Exception e) {
                log.warn("Не удалось удалить снимок ограждения '{}' из БД: {}", snapshotKey, e.getMessage());
            }
        }

        int restoredLimits = 0;
        int resumedHms = 0;
        int resumedHdfs = 0;

        // 1. Снятие сетевого ограждения (Unfence Network)
        if (Boolean.TRUE.equals(req.restoreNetwork())) {
            long defaultLimit = 100L * 1024 * 1024; // 100 МБ/с по умолчанию
            if (snapshot != null && !snapshot.getPreviousDcLimits().isEmpty()) {
                for (var entry : snapshot.getPreviousDcLimits().entrySet()) {
                    String[] parts = entry.getKey().split("->");
                    if (parts.length == 2) {
                        long lim = entry.getValue() > 0 ? entry.getValue() : defaultLimit;
                        applyDcLimit(parts[0], parts[1], lim);
                        restoredLimits++;
                    }
                }
            } else {
                // Восстанавливаем дефолтные лимиты для всех датацентров
                for (var srcDc : topologyRegistry.getDatacenters()) {
                    for (var dstDc : topologyRegistry.getDatacenters()) {
                        if (!srcDc.getId().equalsIgnoreCase(dstDc.getId())) {
                            applyDcLimit(srcDc.getId(), dstDc.getId(), defaultLimit);
                            restoredLimits++;
                        }
                    }
                }
            }
            log.info("[Disaster Recovery Rollback] Сетевое ограждение для кластера '{}' снято (обновлено {} каналов)", clusterId, restoredLimits);
        }

        // 2. Возобновление схем HMS
        if (Boolean.TRUE.equals(req.resumeHms())) {
            List<HmsReplicationJobEntity> hmsJobs = hmsReplicationJobRepository.findByNormalizedSourceClusterId(clusterId);
            for (HmsReplicationJobEntity h : hmsJobs) {
                if (matchesCluster(h.getSourceClusterId(), clusterId)) {
                    boolean wasPausedBySnapshot = snapshot != null && snapshot.getPausedHmsJobIds().contains(h.getId());
                    if (wasPausedBySnapshot || "PAUSED".equalsIgnoreCase(h.getStatus())) {
                        h.setStatus("ACTIVE");
                        h.setMessage("Возобновлено после отката Kill-Switch оператором " + operator);
                        hmsReplicationJobRepository.save(h);
                        resumedHms++;
                    }
                }
            }
            log.info("[Disaster Recovery Rollback] Возобновлено {} схем HMS для кластера '{}'", resumedHms, clusterId);
        }

        // 3. Восстановление расписаний и состояния HDFS задач
        if (Boolean.TRUE.equals(req.resumeHdfs())) {
            List<JobEntity> jobs = jobRepository.findByNormalizedSourceClusterId(clusterId);
            for (JobEntity j : jobs) {
                if (matchesCluster(j.getSourceClusterId(), clusterId)) {
                    boolean wasStoppedBySnapshot = snapshot != null && snapshot.getStoppedHdfsJobIds().contains(j.getId());
                    if (wasStoppedBySnapshot || "STOPPED".equalsIgnoreCase(j.getStatus())) {
                        boolean shouldSchedule = snapshot != null
                                ? Boolean.TRUE.equals(snapshot.getPreviousHdfsScheduled().get(j.getId()))
                                : (j.getCronExpression() != null && !j.getCronExpression().isBlank());

                        j.setStatus("QUEUED");
                        j.setScheduled(shouldSchedule);
                        j.setMessage("Возобновлено после отката Kill-Switch оператором " + operator);
                        jobRepository.save(j);
                        resumedHdfs++;
                    }
                }
            }
            log.info("[Disaster Recovery Rollback] Возобновлено {} HDFS задач для кластера '{}'", resumedHdfs, clusterId);
        }

        String msg = String.format("Откат Kill-Switch выполнен успешно: снято сетевое ограждение (%d каналов), возобновлено %d схем HMS и %d HDFS задач",
                restoredLimits, resumedHms, resumedHdfs);
        return DrActionResponse.stopSuccess(msg, resumedHdfs, resumedHms);
    }

    /**
     * Создание и запуск обратных правил репликации (Reverse Replication) из target в source.
     */
    @Transactional
    public DrActionResponse reverseReplication(DrReverseRequest req, String username) {
        String fromClusterId = req != null && req.fromClusterId() != null ? req.fromClusterId() : "src";
        String toClusterId = req != null && req.toClusterId() != null ? req.toClusterId() : "dst";
        if (lockService != null) {
            String lockKey = "dr_reverse_" + fromClusterId.toLowerCase() + "_" + toClusterId.toLowerCase();
            final DrActionResponse[] result = new DrActionResponse[1];
            lockService.runWithLock(lockKey, java.time.Duration.ofSeconds(20), epoch -> {
                result[0] = doReverseReplication(req, username);
            });
            if (result[0] != null) {
                return result[0];
            } else {
                return DrActionResponse.error(
                        "Операция обратной репликации между " + fromClusterId + " и " + toClusterId +
                                " уже выполняется другим инстансом Orchestrator (блокировка удерживается)"
                );
            }
        } else {
            return doReverseReplication(req, username);
        }
    }

    private DrActionResponse doReverseReplication(DrReverseRequest req, String username) {
        String fromClusterId = req.fromClusterId();
        String toClusterId = req.toClusterId();
        String operator = username != null ? username : "dr_operator";

        log.info("[Disaster Recovery] Запуск инверсии репликации (Reverse Replication): {} ➔ {} оператором '{}'",
                fromClusterId, toClusterId, operator);

        int reversedHdfs = 0;
        int reversedHms = 0;
        List<String> createdIds = new ArrayList<>();
        Instant now = Instant.now();

        // 1. Инверсия задач HDFS
        if (Boolean.TRUE.equals(req.includeHdfs())) {
            // Маршрут отбирается запросом; сравнение кластеров остается и в коде —
            // нормализация в SQL и в matchesCluster должна давать одно и то же
            List<JobEntity> existingJobs = jobRepository.findByNormalizedRoute(toClusterId, fromClusterId);
            // Зеркала живут на обратном маршруте, и проверка существования смотрит именно туда:
            // иначе повторная инверсия создает второе зеркало того же задания
            List<JobEntity> existingReverseJobs = jobRepository.findByNormalizedRoute(fromClusterId, toClusterId);
            // Ищем задачи, которые изначально шли toClusterId ➔ fromClusterId (прямые)
            List<JobEntity> directJobs = existingJobs.stream()
                    .filter(j -> j.getParentJobId() == null
                            && matchesCluster(j.getSourceClusterId(), toClusterId)
                            && matchesCluster(j.getTargetClusterId(), fromClusterId))
                    .toList();

            for (JobEntity direct : directJobs) {
                boolean wasScheduled = direct.isScheduled();

                // Fencing: гарантированная остановка прямой задачи во избежание встречной циклической записи
                if ("RUNNING".equalsIgnoreCase(direct.getStatus())
                        || "QUEUED".equalsIgnoreCase(direct.getStatus())
                        || "ACTIVE".equalsIgnoreCase(direct.getStatus())
                        || direct.isScheduled()) {
                    log.info("[Disaster Recovery Fencing] Остановка прямой HDFS задачи {} ({}) перед созданием реверса",
                            direct.getId(), direct.getStatus());
                    direct.setStatus("STOPPED");
                    direct.setScheduled(false);
                    direct.setUpdatedAt(now);
                    jobRepository.save(direct);
                }

                // Проверяем, существует ли уже обратная задача
                boolean alreadyExists = existingReverseJobs.stream().anyMatch(ej ->
                        !ej.getId().equals(direct.getId())
                                && matchesCluster(ej.getSourceClusterId(), fromClusterId)
                                && matchesCluster(ej.getTargetClusterId(), toClusterId)
                                && matchesReversePath(ej, direct)
                );

                if (!alreadyExists) {
                    JobEntity revJob = new JobEntity();
                    revJob.setId("rev-" + UUID.randomUUID().toString().substring(0, 8));
                    revJob.setSourcePath(direct.getTargetPath());
                    revJob.setTargetPath(direct.getSourcePath());
                    revJob.setSourceClusterId(fromClusterId);
                    revJob.setTargetClusterId(toClusterId);
                    revJob.setTotalBytes(direct.getTotalBytes());
                    revJob.setCopiedBytes(0L);
                    revJob.setStatus(Boolean.TRUE.equals(req.autoStart()) ? "QUEUED" : "SCHEDULED");
                    revJob.setScheduled(wasScheduled);
                    revJob.setCronExpression(direct.getCronExpression());
                    revJob.setExecutionPrincipal(direct.getExecutionPrincipal());
                    revJob.setRunAsServiceAccount(direct.isRunAsServiceAccount());
                    revJob.setCreatedBy(operator);
                    revJob.setMessage("Обратная репликация (Reverse Failback) из " + fromClusterId + " в " + toClusterId);
                    revJob.setCreatedAt(now);
                    revJob.setUpdatedAt(now);
                    jobRepository.save(revJob);

                    // Создаем запись начального прогона
                    JobRunEntity run = new JobRunEntity();
                    run.setId(UUID.randomUUID().toString());
                    run.setJobId(revJob.getId());
                    run.setRunNumber(1);
                    run.setTriggerType("MANUAL");
                    run.setStatus(revJob.getStatus());
                    run.setTotalBytes(revJob.getTotalBytes());
                    run.setCopiedBytes(0L);
                    run.setStartedAt(now);
                    run.setMessage("Запуск обратной репликации дельты");
                    run.setTriggeredBy(operator);
                    run.setCreatedAt(now);
                    jobRunRepository.save(run);
                    revJob.setActiveRunId(run.getId());
                    jobRepository.save(revJob);

                    createdIds.add(revJob.getId());
                    reversedHdfs++;
                }
            }
        }

        // 2. Инверсия задач HMS
        if (Boolean.TRUE.equals(req.includeHms())) {
            List<HmsReplicationJobEntity> existingHms =
                    hmsReplicationJobRepository.findByNormalizedRoute(toClusterId, fromClusterId);
            List<HmsReplicationJobEntity> existingReverseHms =
                    hmsReplicationJobRepository.findByNormalizedRoute(fromClusterId, toClusterId);
            List<HmsReplicationJobEntity> directHms = existingHms.stream()
                    .filter(h -> matchesCluster(h.getSourceClusterId(), toClusterId)
                            && matchesCluster(h.getTargetClusterId(), fromClusterId))
                    .toList();

            for (HmsReplicationJobEntity direct : directHms) {
                // Fencing: остановка прямой HMS задачи во избежание встречной перезаписи схем
                if ("RUNNING".equalsIgnoreCase(direct.getStatus()) || "ACTIVE".equalsIgnoreCase(direct.getStatus()) || "QUEUED".equalsIgnoreCase(direct.getStatus())) {
                    log.info("[Disaster Recovery Fencing] Остановка прямой HMS задачи {} ({}) перед созданием реверса",
                            direct.getId(), direct.getStatus());
                    direct.setStatus("STOPPED");
                    direct.setUpdatedAt(now);
                    hmsReplicationJobRepository.save(direct);
                }

                boolean alreadyExists = existingReverseHms.stream().anyMatch(eh ->
                        matchesCluster(eh.getSourceClusterId(), fromClusterId)
                                && matchesCluster(eh.getTargetClusterId(), toClusterId)
                                && Objects.equals(eh.getSourceDbName(), direct.getTargetDbName())
                );

                if (!alreadyExists) {
                    HmsReplicationJobEntity createdHms = hmsCoordinatorService.createAndStartReplication(
                            fromClusterId,
                            toClusterId,
                            direct.getTargetDbName(),
                            direct.getSourceDbName(),
                            direct.getTableIncludePattern(),
                            operator,
                            direct.isDropExtraneousTables(),
                            direct.isDropExtraneousPartitions(),
                            direct.getExecutionPrincipal()
                    );
                    createdIds.add("hms-" + createdHms.getId());
                    reversedHms++;
                }
            }
        }

        // Снятие сетевого ограждения (Fencing): возвращаем дефолтный лимит 100 МБ/с для нового направления
        applyDcLimit(fromClusterId, toClusterId, 100L * 1024 * 1024);

        String msg = String.format("Обратная репликация настроена (%s ➔ %s): создано %d HDFS задач и %d HMS задач",
                fromClusterId, toClusterId, reversedHdfs, reversedHms);
        return DrActionResponse.reverseSuccess(msg, reversedHdfs, reversedHms, createdIds);
    }

    /**
     * Разворот одной точечной HDFS задачи.
     */
    @Transactional
    public DrActionResponse reverseSingleHdfsJob(String jobId, String username) {
        JobEntity direct = jobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Задача с ID " + jobId + " не найдена"));

        String operator = username != null ? username : "dr_operator";
        Instant now = Instant.now();

        JobEntity revJob = new JobEntity();
        revJob.setId("rev-" + UUID.randomUUID().toString().substring(0, 8));
        revJob.setSourceClusterId(direct.getTargetClusterId());
        revJob.setTargetClusterId(direct.getSourceClusterId());
        revJob.setSourcePath(direct.getTargetPath());
        revJob.setTargetPath(direct.getSourcePath());
        revJob.setTotalBytes(direct.getTotalBytes());
        revJob.setCopiedBytes(0L);
        revJob.setStatus("QUEUED");
        revJob.setScheduled(direct.isScheduled());
        revJob.setCronExpression(direct.getCronExpression());
        revJob.setExecutionPrincipal(direct.getExecutionPrincipal());
        revJob.setRunAsServiceAccount(direct.isRunAsServiceAccount());
        revJob.setCreatedBy(operator);
        revJob.setMessage("Обратная репликация для задачи " + direct.getId());
        revJob.setCreatedAt(now);
        revJob.setUpdatedAt(now);
        jobRepository.save(revJob);

        JobRunEntity run = new JobRunEntity();
        run.setId(UUID.randomUUID().toString());
        run.setJobId(revJob.getId());
        run.setRunNumber(1);
        run.setTriggerType("MANUAL");
        run.setStatus("QUEUED");
        run.setTotalBytes(revJob.getTotalBytes());
        run.setCopiedBytes(0L);
        run.setStartedAt(now);
        run.setMessage("Запуск обратной репликации дельты");
        run.setTriggeredBy(operator);
        run.setCreatedAt(now);
        jobRunRepository.save(run);
        revJob.setActiveRunId(run.getId());
        jobRepository.save(revJob);

        return DrActionResponse.reverseSuccess("Обратная задача создана: " + revJob.getId(), 1, 0, List.of(revJob.getId()));
    }

    /**
     * Отзыв (отмена) обратной репликации: удаление созданной обратной задачи и восстановление прямого маршрута.
     */
    @Transactional
    public DrActionResponse undoReverse(String jobId, String username) {
        String operator = username != null ? username : "dr_operator";
        log.info("[Disaster Recovery] Запрос отзыва обратной задачи для '{}' оператором '{}'", jobId, operator);

        // 1. Поиск среди HDFS задач
        Optional<JobEntity> optJob = jobRepository.findById(jobId);
        if (optJob.isPresent()) {
            JobEntity job = optJob.get();
            String targetToDeleteId = null;

            if (job.getId().startsWith("rev-") || (job.getMessage() != null && job.getMessage().toLowerCase().contains("обратная репликация"))) {
                targetToDeleteId = job.getId();
            } else {
                List<JobEntity> allJobs = jobRepository.findByNormalizedRoute(
                        job.getTargetClusterId(), job.getSourceClusterId());
                Optional<JobEntity> reverse = allJobs.stream()
                        .filter(rj -> !rj.getId().equals(job.getId())
                                && matchesCluster(rj.getSourceClusterId(), job.getTargetClusterId())
                                && matchesCluster(rj.getTargetClusterId(), job.getSourceClusterId())
                                && matchesReversePath(rj, job))
                        .findFirst();

                if (reverse.isPresent()) {
                    targetToDeleteId = reverse.get().getId();
                }
            }

            if (targetToDeleteId != null) {
                taskRepository.deleteByJobId(targetToDeleteId);
                jobRunRepository.deleteByJobId(targetToDeleteId);
                jobRepository.deleteById(targetToDeleteId);

                String msg = String.format("Обратная задача '%s' успешно отозвана и удалена. Прямой маршрут разблокирован.", targetToDeleteId);
                log.info("[Disaster Recovery] {}", msg);
                return DrActionResponse.reverseSuccess(msg, 0, 0, List.of(targetToDeleteId));
            } else {
                throw new IllegalArgumentException("Для задачи '" + jobId + "' не найдена обратная задача для отзыва");
            }
        }

        // 2. Поиск среди HMS задач
        Optional<HmsReplicationJobEntity> optHms = hmsReplicationJobRepository.findById(jobId);
        if (optHms.isPresent()) {
            HmsReplicationJobEntity hmsJob = optHms.get();
            String targetToDeleteHmsId = null;

            if (hmsJob.getId().startsWith("rev-") || (hmsJob.getMessage() != null && hmsJob.getMessage().toLowerCase().contains("обратная репликация"))) {
                targetToDeleteHmsId = hmsJob.getId();
            } else {
                List<HmsReplicationJobEntity> allHms = hmsReplicationJobRepository.findByNormalizedRoute(
                        hmsJob.getTargetClusterId(), hmsJob.getSourceClusterId());
                Optional<HmsReplicationJobEntity> reverseHms = allHms.stream()
                        .filter(rh -> !rh.getId().equals(hmsJob.getId())
                                && matchesCluster(rh.getSourceClusterId(), hmsJob.getTargetClusterId())
                                && matchesCluster(rh.getTargetClusterId(), hmsJob.getSourceClusterId())
                                && Objects.equals(rh.getSourceDbName(), hmsJob.getTargetDbName()))
                        .findFirst();

                if (reverseHms.isPresent()) {
                    targetToDeleteHmsId = reverseHms.get().getId();
                }
            }

            if (targetToDeleteHmsId != null) {
                hmsCoordinatorService.deleteReplicationJob(targetToDeleteHmsId);
                String msg = String.format("Обратная HMS-задача '%s' успешно отозвана и удалена.", targetToDeleteHmsId);
                log.info("[Disaster Recovery] {}", msg);
                return DrActionResponse.reverseSuccess(msg, 0, 0, List.of(targetToDeleteHmsId));
            } else {
                throw new IllegalArgumentException("Для HMS-задачи '" + jobId + "' не найдена обратная задача для отзыва");
            }
        }

        throw new IllegalArgumentException("Задача с ID '" + jobId + "' не найдена");
    }

    /**
     * Является ли задача зеркалом прямой: требуется инверсия обоих путей.
     *
     * <p>Прежний ранний возврат по совпадению одного пути вырождал условие в «совпал хотя бы
     * один путь по суффиксу»: произвольная чужая задача выглядела как уже существующий реверс,
     * и при failover обратная репликация не создавалась вовсе.
     */
    boolean matchesReversePath(JobEntity rj, JobEntity direct) {
        if (rj == null || direct == null) return false;
        // Основной режим: пути инвертированы (source <-> target)
        if (isSamePath(rj.getSourcePath(), direct.getTargetPath())
                && isSamePath(rj.getTargetPath(), direct.getSourcePath())) {
            return true;
        }
        // Пути совпадают на обоих кластерах: зеркальность определяется кластерами,
        // которые проверяются вызывающей стороной
        return isSamePath(rj.getSourcePath(), direct.getSourcePath())
                && isSamePath(rj.getTargetPath(), direct.getTargetPath());
    }

    private static boolean matchesCluster(String c1, String c2) {
        if (c1 == null || c2 == null) return false;
        if (c1.equalsIgnoreCase(c2)) return true;
        String c1n = c1.toLowerCase().replace("-", "").replace("_", "");
        String c2n = c2.toLowerCase().replace("-", "").replace("_", "");
        return c1n.equals(c2n);
    }

    /**
     * Принадлежность кластера (или самого идентификатора ЦОД) указанному ЦОД.
     *
     * <p>Сопоставление строгое: либо идентификаторы совпадают, либо кластер явно объявлен
     * в топологии с этим {@code dcId}. Сравнение подстрокой считало `dc10` тем же самым
     * ЦОД, что и `dc1`, из-за чего ограждение Kill-Switch пропускало соседний датацентр,
     * а агенты похожих кластеров попадали в чужую статистику.
     */
    boolean matchesDc(String clusterOrDc, String dc) {
        if (clusterOrDc == null || dc == null) return false;
        if (matchesCluster(clusterOrDc, dc)) {
            return true;
        }
        if (topologyRegistry == null) {
            return false;
        }
        return topologyRegistry.getClusters().stream()
                .filter(cluster -> matchesCluster(cluster.getId(), clusterOrDc))
                .anyMatch(cluster -> matchesCluster(cluster.getDcId(), dc));
    }

    /**
     * Равенство путей репликации.
     *
     * <p>Только полное совпадение (с точностью до завершающего слэша). Сравнение по суффиксу
     * считало `/sales` тем же путем, что `/warehouse/sales`.
     */
    private static boolean isSamePath(String p1, String p2) {
        if (p1 == null || p2 == null) return false;
        return normalizePath(p1).equalsIgnoreCase(normalizePath(p2));
    }

    private static String normalizePath(String path) {
        String trimmed = path.trim();
        while (trimmed.length() > 1 && trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
