package org.apache.hadoop.explorer.replicator.orchestrator.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.BandwidthLimitEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.OrchestratorNodeEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.BandwidthLimitRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.OrchestratorNodeRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.InetAddress;
import java.time.Instant;
import java.util.List;

/**
 * Координатор Active-Active пула Orchestrator.
 * 
 * <p>Обеспечивает:
 * 1. Регистрацию узлов Orchestrator и отслеживание активных инстансов через БД (Heartbeat).
 * 2. Пропорциональное разделение квот Token Bucket (localLimit = configuredLimit / activeNodes).
 * 3. Распределенное сохранение и синхронизацию динамических лимитов пропускной способности (Bandwidth Limits).
 */
@Service
public class OrchestratorClusterCoordinator {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorClusterCoordinator.class);

    private final OrchestratorNodeRepository nodeRepository;
    private final BandwidthLimitRepository limitRepository;
    private final TokenBucketThrottler throttler;
    private final TopologyRegistry topologyRegistry;
    private final org.springframework.transaction.support.TransactionTemplate txTemplate;
    private final String nodeId;
    private final String hostName;

    @Autowired
    public OrchestratorClusterCoordinator(
            @Autowired(required = false) OrchestratorNodeRepository nodeRepository,
            @Autowired(required = false) BandwidthLimitRepository limitRepository,
            TokenBucketThrottler throttler,
            TopologyRegistry topologyRegistry,
            @Autowired(required = false) DistributedLockService lockService,
            @Autowired(required = false) org.springframework.transaction.PlatformTransactionManager transactionManager
    ) {
        this.nodeRepository = nodeRepository;
        this.limitRepository = limitRepository;
        this.throttler = throttler;
        this.topologyRegistry = topologyRegistry;
        this.txTemplate = transactionManager != null ? new org.springframework.transaction.support.TransactionTemplate(transactionManager) : null;
        this.nodeId = lockService != null ? lockService.getNodeId() : "orch-standalone-" + System.currentTimeMillis();
        String host = "localhost";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception ignored) {
        }
        this.hostName = host;
    }

    public OrchestratorClusterCoordinator(
            OrchestratorNodeRepository nodeRepository,
            BandwidthLimitRepository limitRepository,
            TokenBucketThrottler throttler,
            TopologyRegistry topologyRegistry,
            DistributedLockService lockService
    ) {
        this(nodeRepository, limitRepository, throttler, topologyRegistry, lockService, null);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHostName() {
        return hostName;
    }

    @PostConstruct
    public void init() {
        if (nodeRepository != null) {
            try {
                sendHeartbeatAndRebalance();
                syncLimitsFromDatabase();
            } catch (Exception e) {
                log.warn("Не удалось инициализировать координацию Active-Active Orchestrator: {}", e.getMessage());
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        if (nodeRepository != null) {
            try {
                nodeRepository.deleteById(nodeId);
                log.info("Узел Orchestrator '{}' дерегистрирован при остановке", nodeId);
            } catch (Exception e) {
                log.warn("Ошибка при дерегистрации узла '{}': {}", nodeId, e.getMessage());
            }
        }
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void scheduledCoordination() {
        if (nodeRepository != null) {
            try {
                if (txTemplate != null) {
                    txTemplate.executeWithoutResult(status -> sendHeartbeatAndRebalance());
                } else {
                    sendHeartbeatAndRebalance();
                }
                syncLimitsFromDatabase();
            } catch (Exception e) {
                log.warn("Ошибка цикла координации Active-Active Orchestrator: {}", e.getMessage());
            }
        }
    }

    @Transactional
    public void sendHeartbeatAndRebalance() {
        if (nodeRepository == null) return;
        Instant now = Instant.now();
        nodeRepository.save(new OrchestratorNodeEntity(nodeId, hostName, now, "ACTIVE"));

        Instant staleThreshold = now.minusSeconds(15);
        try {
            nodeRepository.cleanStaleNodes(staleThreshold);
        } catch (Exception e) {
            log.debug("Ошибка очистки устаревших узлов Orchestrator: {}", e.getMessage());
        }

        long activeCount = nodeRepository.countActiveNodes(staleThreshold);
        int nodeCount = Math.max(1, (int) activeCount);
        if (nodeCount != throttler.getActiveNodeCount()) {
            log.info("[Active-Active Orchestrator] Изменение числа активных узлов: {} ➔ {}. Перебалансировка квот Token Bucket",
                    throttler.getActiveNodeCount(), nodeCount);
            throttler.rebalanceQuotas(nodeCount);
        }
    }

    @Transactional(readOnly = true)
    public void syncLimitsFromDatabase() {
        if (limitRepository == null) return;
        List<BandwidthLimitEntity> limits = limitRepository.findAll();
        for (BandwidthLimitEntity limit : limits) {
            applyLimitLocally(limit.getLimitKey(), limit.getLimitBytesPerSec());
        }
    }

    @Transactional
    public void persistAndApplyLimit(String key, long bytesPerSec) {
        applyLimitLocally(key, bytesPerSec);
        if (limitRepository != null) {
            try {
                limitRepository.save(new BandwidthLimitEntity(key, bytesPerSec, Instant.now()));
            } catch (Exception e) {
                log.warn("Ошибка сохранения лимита '{}' в БД: {}", key, e.getMessage());
            }
        }
    }

    private void applyLimitLocally(String key, long bytesPerSec) {
        if ("global".equalsIgnoreCase(key)) {
            throttler.setGlobalLimit(bytesPerSec);
        } else if (key.startsWith("dc:")) {
            String route = key.substring(3);
            if (route.contains("->")) {
                String[] parts = route.split("->");
                topologyRegistry.setDcLimit(parts[0], parts[1], bytesPerSec);
            }
        } else if (key.startsWith("hdfs:")) {
            String route = key.substring(5);
            if (route.contains("->")) {
                String[] parts = route.split("->");
                topologyRegistry.setHdfsLimit(parts[0], parts[1], bytesPerSec);
            }
        }
    }
}
