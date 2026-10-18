package org.apache.hadoop.explorer.replicator.sender;

import io.grpc.ManagedChannel;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.TokenRequest;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ReplicationSenderOptimizationTest {

    @Test
    @DisplayName("P1: getOrCreateChannel возвращает одинаковый ManagedChannel из пула и удаляет при evictChannel")
    void testChannelPoolReuseAndEviction() throws Exception {
        OrchestratorGrpcClient orchestratorClient = mock(OrchestratorGrpcClient.class);
        HadoopFsManager fsManager = mock(HadoopFsManager.class);
        LocalBandwidthLimiter bandwidthLimiter = mock(LocalBandwidthLimiter.class);

        try (ReplicationSender sender = new ReplicationSender(
                "worker-test-1",
                orchestratorClient,
                fsManager,
                bandwidthLimiter,
                64 * 1024
        )) {
            String target = "localhost:50051";

            ManagedChannel ch1 = sender.getOrCreateChannel(target);
            ManagedChannel ch2 = sender.getOrCreateChannel(target);

            assertNotNull(ch1);
            assertSame(ch1, ch2, "Пул должен возвращать один и тот же ManagedChannel для одинакового адреса");
            assertFalse(ch1.isShutdown());

            // Исключение сбойного канала из пула
            sender.evictChannel(target);
            assertTrue(ch1.isShutdown(), "Исключенный канал должен быть закрыт (shutdown)");

            // Последующее получение создает новый канал
            ManagedChannel ch3 = sender.getOrCreateChannel(target);
            assertNotNull(ch3);
            assertNotSame(ch1, ch3, "После evictChannel должен создаваться свежий экземпляр канала");
            assertFalse(ch3.isShutdown());
        }
    }

    @Test
    @DisplayName("P1: acquireNetworkTokens пакетирует запросы порциями (Hierarchical Token Bucket batching)")
    void testTokenBatchLeaseAcquisition() {
        OrchestratorGrpcClient orchestratorClient = mock(OrchestratorGrpcClient.class);
        when(orchestratorClient.requestNetworkTokens(any(TokenRequest.class))).thenReturn(0.0);

        ConcurrentHashMap<String, AtomicLong> leaseMap = new ConcurrentHashMap<>();

        // 1. Первый запрос на 64 КБ: должен запросить у оркестратора батч (2 МБ)
        ReplicationSender.acquireNetworkTokens(
                leaseMap,
                orchestratorClient,
                "worker-1",
                64 * 1024L,
                "dc1",
                "dc2"
        );

        verify(orchestratorClient, times(1)).requestNetworkTokens(argThat(req ->
                req.getRequestedBytes() == 2 * 1024 * 1024L
                        && "worker-1".equals(req.getWorkerId())
                        && "dc1".equals(req.getSourceClusterId())
        ));

        // 2. Следующие запросы на 64 КБ должны потреблять токены из локального остатка БЕЗ обращения к оркестратору
        for (int i = 0; i < 10; i++) {
            ReplicationSender.acquireNetworkTokens(
                    leaseMap,
                    orchestratorClient,
                    "worker-1",
                    64 * 1024L,
                    "dc1",
                    "dc2"
            );
        }

        // Число вызовов оркестратора должно остаться равным 1
        verify(orchestratorClient, times(1)).requestNetworkTokens(any(TokenRequest.class));

        // В пуле остатка должно остаться: 2MB - 11 * 64KB
        String routeKey = "dc1->dc2";
        long remaining = leaseMap.get(routeKey).get();
        assertEquals(2 * 1024 * 1024L - (11 * 64 * 1024L), remaining);
    }
}
