package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.StreamingLeaseEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.StreamingLeaseRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Координатор высокой доступности Active-Standby для агентов HDFS Inotify стриминга.
 * Управляет распределенным лизингом, сменой эпох (Epoch Fencing) и аудитом резервирования.
 */
@Service
public class StreamingLeaseCoordinator {

    private static final Logger log = LoggerFactory.getLogger(StreamingLeaseCoordinator.class);
    private static final Duration DEFAULT_LEASE_DURATION = Duration.ofSeconds(10);

    private final StreamingLeaseRepository leaseRepository;
    private final JobRepository jobRepository;
    private final AgentRegistry agentRegistry;
    private final ReplicatorProperties properties;

    public StreamingLeaseCoordinator(
            StreamingLeaseRepository leaseRepository,
            JobRepository jobRepository,
            AgentRegistry agentRegistry,
            ReplicatorProperties properties
    ) {
        this.leaseRepository = leaseRepository;
        this.jobRepository = jobRepository;
        this.agentRegistry = agentRegistry;
        this.properties = properties != null ? properties : new ReplicatorProperties();
    }

    /**
     * Атомарное продление или перехват аренды стримера кластера.
     */
    @Transactional
    public StreamingLeaseRenewResponse renewLease(StreamingLeaseRenewRequest req) {
        String clusterId = req.clusterId() != null ? req.clusterId() : "default";
        String agentId = req.agentId();

        if (!properties.getStreaming().isEnabled()) {
            return new StreamingLeaseRenewResponse(
                    "DISABLED",
                    0L,
                    "none",
                    Instant.now(),
                    0,
                    false,
                    0L
            );
        }

        // Проверяем, что агент имеет режим streamer, если в кластере есть выделенные стримеры
        boolean hasDedicatedStreamers = agentRegistry.getAgents().stream()
                .anyMatch(a -> "streamer".equalsIgnoreCase(a.getMode()) && matchesCluster(a.getClusterId(), clusterId));
        boolean isAgentStreamer = agentRegistry.getAgents().stream()
                .filter(a -> a.getAgentId().equalsIgnoreCase(agentId))
                .findFirst()
                .map(a -> "streamer".equalsIgnoreCase(a.getMode()))
                .orElse(true);

        if (hasDedicatedStreamers && !isAgentStreamer) {
            log.warn("[Streamer HA] Агент '{}' с режимом, отличным от 'streamer', отклонен от лизинга кластера '{}', так как зарегистрированы специализированные стримеры",
                    agentId, clusterId);
            StreamingLeaseEntity currentLease = leaseRepository.findById(clusterId).orElse(null);
            return new StreamingLeaseRenewResponse(
                    "STANDBY",
                    currentLease != null ? currentLease.getEpoch() : 0L,
                    currentLease != null ? currentLease.getActiveAgentId() : "none",
                    currentLease != null ? currentLease.getExpiresAt() : Instant.now(),
                    (int) agentRegistry.getAgents().stream()
                            .filter(a -> "streamer".equalsIgnoreCase(a.getMode()) && matchesCluster(a.getClusterId(), clusterId))
                            .count(),
                    false,
                    0L
            );
        }

        Instant now = Instant.now();
        Instant expiresAt = now.plus(DEFAULT_LEASE_DURATION);

        // 1. Попытка атомарного продления или перехвата протухшей аренды
        int updated = leaseRepository.tryAcquireOrRenewLease(clusterId, agentId, expiresAt, now);
        if (updated == 0 && !leaseRepository.existsById(clusterId)) {
            // Первая инициализация лизинга для кластера
            try {
                StreamingLeaseEntity initial = new StreamingLeaseEntity(clusterId, agentId, 1L, expiresAt, now);
                leaseRepository.saveAndFlush(initial);
                log.info("[Streamer HA] Создана начальная аренда для кластера '{}': активный агент '{}', эпоха 1", clusterId, agentId);
            } catch (Exception e) {
                log.debug("[Streamer HA] Конкурентная инициализация лизинга кластера {}: {}", clusterId, e.getMessage());
            }
        }

        // 2. Читаем актуальное состояние лизинга
        StreamingLeaseEntity lease = leaseRepository.findById(clusterId).orElseGet(() ->
                new StreamingLeaseEntity(clusterId, agentId, 1L, expiresAt, now)
        );

        boolean isActive = agentId.equalsIgnoreCase(lease.getActiveAgentId()) && lease.getExpiresAt().isAfter(now);
        String status = isActive ? "ACTIVE" : "STANDBY";

        // 3. Подсчет количества зарегистрированных стримеров для кластера
        int streamersCount = (int) agentRegistry.getAgents().stream()
                .filter(a -> "streamer".equalsIgnoreCase(a.getMode()) && matchesCluster(a.getClusterId(), clusterId))
                .count();

        boolean redundancyWarning = streamersCount < 2;
        if (redundancyWarning) {
            log.warn("[Streamer HA Warning] Для кластера '{}' обнаружено всего {} стример(ов). Отсутствует резерв (NO_REDUNDANCY)!",
                    clusterId, streamersCount);
        }

        // 4. Поиск максимального lastProcessedTxid для задач стриминга кластера
        long maxTxid = 0L;
        List<JobEntity> jobs = jobRepository.findAll();
        for (JobEntity j : jobs) {
            if ("STREAMING_INOTIFY".equalsIgnoreCase(j.getSyncMode()) && matchesCluster(j.getSourceClusterId(), clusterId)) {
                if (j.getLastProcessedTxid() != null && j.getLastProcessedTxid() > maxTxid) {
                    maxTxid = j.getLastProcessedTxid();
                }
            }
        }

        return new StreamingLeaseRenewResponse(
                status,
                lease.getEpoch(),
                lease.getActiveAgentId(),
                lease.getExpiresAt(),
                streamersCount,
                redundancyWarning,
                maxTxid
        );
    }

    /**
     * Получение текущего статуса лизинга кластера (для мониторинга и UI).
     */
    @Transactional(readOnly = true)
    public Optional<StreamingLeaseRenewResponse> getLeaseStatus(String clusterId) {
        String targetCluster = clusterId != null ? clusterId : "default";
        Optional<StreamingLeaseEntity> leaseOpt = leaseRepository.findById(targetCluster);
        if (leaseOpt.isEmpty()) {
            return Optional.empty();
        }

        StreamingLeaseEntity lease = leaseOpt.get();
        Instant now = Instant.now();
        boolean isActive = lease.getExpiresAt().isAfter(now);

        int streamersCount = (int) agentRegistry.getAgents().stream()
                .filter(a -> "streamer".equalsIgnoreCase(a.getMode()) && matchesCluster(a.getClusterId(), targetCluster))
                .count();

        long maxTxid = 0L;
        for (JobEntity j : jobRepository.findAll()) {
            if ("STREAMING_INOTIFY".equalsIgnoreCase(j.getSyncMode()) && matchesCluster(j.getSourceClusterId(), targetCluster)) {
                if (j.getLastProcessedTxid() != null && j.getLastProcessedTxid() > maxTxid) {
                    maxTxid = j.getLastProcessedTxid();
                }
            }
        }

        return Optional.of(new StreamingLeaseRenewResponse(
                isActive ? "ACTIVE" : "EXPIRED",
                lease.getEpoch(),
                lease.getActiveAgentId(),
                lease.getExpiresAt(),
                streamersCount,
                streamersCount < 2,
                maxTxid
        ));
    }

    /**
     * Получение статуса лизинга для всех кластеров, где запущены стримеры (DC1, DC2, ...).
     */
    @Transactional(readOnly = true)
    public Map<String, StreamingLeaseRenewResponse> getAllLeaseStatuses() {
        Map<String, StreamingLeaseRenewResponse> result = new HashMap<>();
        List<StreamingLeaseEntity> allLeases = leaseRepository.findAll();
        for (StreamingLeaseEntity lease : allLeases) {
            getLeaseStatus(lease.getClusterId()).ifPresent(resp -> result.put(lease.getClusterId(), resp));
        }
        return result;
    }

    private static boolean matchesCluster(String c1, String c2) {
        if (c1 == null || c2 == null) return true;
        return c1.equalsIgnoreCase(c2) || c1.contains(c2) || c2.contains(c1);
    }
}
