package org.apache.hadoop.explorer.replicator.orchestrator.throttler;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.BandwidthLimitEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.OrchestratorNodeEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.BandwidthLimitRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.OrchestratorNodeRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.DistributedLockService;
import org.apache.hadoop.explorer.replicator.orchestrator.service.OrchestratorClusterCoordinator;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TokenBucketClusterScalingTest {

    private ReplicatorProperties properties;
    private TopologyRegistry topologyRegistry;
    private TokenBucketThrottler throttler;

    @BeforeEach
    void setUp() {
        properties = new ReplicatorProperties();
        properties.setGlobalLimitBytesPerSec(100L * 1024 * 1024); // 100 МБ/с

        topologyRegistry = mock(TopologyRegistry.class);
        throttler = new TokenBucketThrottler(properties, topologyRegistry);
    }

    @Test
    @DisplayName("RES-06: Локальная квота должна пропорционально делиться на число активных узлов Orchestrator")
    void shouldProportionallyDivideGlobalQuotaAcrossActiveNodes() {
        // По умолчанию 1 узел -> 100% лимита (100 МБ/с)
        assertEquals(1, throttler.getActiveNodeCount());
        assertEquals(100L * 1024 * 1024, throttler.getGlobalConfiguredLimit());
        assertEquals(100L * 1024 * 1024, throttler.getGlobalLimit());

        // 2 активных узла -> 50% лимита (50 МБ/с) на каждый инстанс
        throttler.rebalanceQuotas(2);
        assertEquals(2, throttler.getActiveNodeCount());
        assertEquals(100L * 1024 * 1024, throttler.getGlobalConfiguredLimit(), "Сконфигурированный лимит остается неизменным");
        assertEquals(50L * 1024 * 1024, throttler.getGlobalLimit(), "Эффективный локальный лимит уменьшен в 2 раза");

        // 4 активных узла -> 25% лимита (25 МБ/с)
        throttler.rebalanceQuotas(4);
        assertEquals(4, throttler.getActiveNodeCount());
        assertEquals(25L * 1024 * 1024, throttler.getGlobalLimit());
    }

    @Test
    @DisplayName("RES-06: При сбое соседей квота должна автоматически восстанавливаться до 100%")
    void shouldRestoreFullQuotaWhenNodesFail() {
        // Было 4 узла
        throttler.rebalanceQuotas(4);
        assertEquals(25L * 1024 * 1024, throttler.getGlobalLimit());

        // 3 узла упали, остался 1 узел
        throttler.rebalanceQuotas(1);
        assertEquals(1, throttler.getActiveNodeCount());
        assertEquals(100L * 1024 * 1024, throttler.getGlobalLimit());
    }

    @Test
    @DisplayName("RES-06: Квоты DC-DC и HDFS-HDFS бакетов также должны пропорционально масштабироваться")
    void shouldScaleDcAndHdfsBucketsProportionally() {
        when(topologyRegistry.getCluster("c1")).thenReturn(java.util.Optional.of(
                new ReplicatorProperties.ClusterConfig("c1", "Cluster 1", "dc1", "hdfs://nn1:8020", "h1", 50051)
        ));
        when(topologyRegistry.getCluster("c2")).thenReturn(java.util.Optional.of(
                new ReplicatorProperties.ClusterConfig("c2", "Cluster 2", "dc2", "hdfs://nn2:8020", "h2", 50051)
        ));
        when(topologyRegistry.getDcLimit("dc1", "dc2")).thenReturn(java.util.Optional.of(60L * 1024 * 1024)); // 60 МБ/с
        when(topologyRegistry.getHdfsLimit("c1", "c2")).thenReturn(java.util.Optional.of(40L * 1024 * 1024)); // 40 МБ/с

        throttler.rebalanceQuotas(2);

        // Инициализируем бакеты через requestTokens
        throttler.requestTokens(1024, "c1", "c2");

        TokenBucketThrottler.SingleBucket dcBucket = throttler.getDcBuckets().get("dc1->dc2");
        assertNotNull(dcBucket);
        assertEquals(60L * 1024 * 1024, dcBucket.getConfiguredLimitBytesPerSec());
        assertEquals(30L * 1024 * 1024, dcBucket.getLimitBytesPerSec(), "Локальный лимит DC уменьшен в 2 раза");

        TokenBucketThrottler.SingleBucket hdfsBucket = throttler.getHdfsBuckets().get("c1->c2");
        assertNotNull(hdfsBucket);
        assertEquals(40L * 1024 * 1024, hdfsBucket.getConfiguredLimitBytesPerSec());
        assertEquals(20L * 1024 * 1024, hdfsBucket.getLimitBytesPerSec(), "Локальный лимит HDFS уменьшен в 2 раза");
    }

    @Test
    @DisplayName("RES-06: OrchestratorClusterCoordinator должен регистрировать heartbeat, определять активные узлы и синхронизировать лимиты")
    void shouldCoordinateNodesAndLimitsThroughCoordinator() {
        OrchestratorNodeRepository nodeRepo = mock(OrchestratorNodeRepository.class);
        BandwidthLimitRepository limitRepo = mock(BandwidthLimitRepository.class);
        DistributedLockService lockService = mock(DistributedLockService.class);
        when(lockService.getNodeId()).thenReturn("orch-test-node-1");

        when(nodeRepo.countActiveNodes(any())).thenReturn(3L);
        when(limitRepo.findAll()).thenReturn(List.of(
                new BandwidthLimitEntity("global", 150L * 1024 * 1024, Instant.now()),
                new BandwidthLimitEntity("dc:dc1->dc2", 80L * 1024 * 1024, Instant.now())
        ));

        OrchestratorClusterCoordinator coordinator = new OrchestratorClusterCoordinator(
                nodeRepo, limitRepo, throttler, topologyRegistry, lockService
        );

        coordinator.sendHeartbeatAndRebalance();

        // Проверяем сохранение heartbeat
        verify(nodeRepo).save(argThat(entity -> "orch-test-node-1".equals(entity.getNodeId())));
        verify(nodeRepo).cleanStaleNodes(any());

        // Проверяем, что throttler перебалансировал квоты на 3 узла
        assertEquals(3, throttler.getActiveNodeCount());

        coordinator.syncLimitsFromDatabase();

        // Проверяем применение обновленного глобального лимита (150 МБ / 3 узла = 50 МБ/с)
        assertEquals(150L * 1024 * 1024, throttler.getGlobalConfiguredLimit());
        assertEquals(50L * 1024 * 1024, throttler.getGlobalLimit());

        // Проверяем применение DC лимита в топологию
        verify(topologyRegistry).setDcLimit("dc1", "dc2", 80L * 1024 * 1024);
    }
}
