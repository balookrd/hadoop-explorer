package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.StreamingLeaseRenewRequest;
import org.apache.hadoop.explorer.replicator.model.StreamingLeaseRenewResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Self-fencing активного inotify-стримера при потере связи с оркестратором.
 *
 * <p>Ключевая гарантия (SPLIT-BRAIN): если аренда не продлевается дольше предельного таймаута,
 * стример обязан сам сложить полномочия. Иначе сетевая изоляция дает двух активных стримеров,
 * которые дублируют задачи репликации.
 *
 * <p>Тонкость, из-за которой защита не работала: gRPC-клиент перехватывает исключение
 * продления и возвращает {@code null}, поэтому ветка {@code catch} в цикле аренды —
 * единственное место, где вызывался фенсинг — никогда не исполнялась.
 */
@DisplayName("Inotify streamer: self-fencing при потере аренды")
class StreamerSelfFencingTest {

    private OrchestratorGrpcClient orchestratorClient;
    private HdfsInotifyListener listener;

    @BeforeEach
    void setUp() {
        ReplicatorAgentConfig config = new ReplicatorAgentConfig();
        config.setClusterId("dc1");
        config.setAgentId("streamer-dc1-1");

        orchestratorClient = mock(OrchestratorGrpcClient.class);
        HadoopFsManager fsManager = mock(HadoopFsManager.class);
        InotifyBatchProcessor processor = mock(InotifyBatchProcessor.class);
        listener = new HdfsInotifyListener(config, orchestratorClient, fsManager, processor);
    }

    @Test
    @DisplayName("Клиент вернул null (связь потеряна) — активный стример отгораживает себя")
    void testNullLeaseResponseFencesActiveStreamer() {
        when(orchestratorClient.renewStreamingLease(any(StreamingLeaseRenewRequest.class))).thenReturn(null);
        // Аренда не продлевалась дольше предельного таймаута (10 с)
        listener.setActiveForTesting(true, System.currentTimeMillis() - 20_000L);

        listener.renewLeaseOnce();

        assertFalse(listener.isActive(),
                "Стример остался активным без продления аренды — это split-brain с дублированием задач");
    }

    @Test
    @DisplayName("Недавняя успешная аренда не отгораживает стример при единичном сбое")
    void testRecentLeaseKeepsStreamerActive() {
        when(orchestratorClient.renewStreamingLease(any(StreamingLeaseRenewRequest.class))).thenReturn(null);
        listener.setActiveForTesting(true, System.currentTimeMillis());

        listener.renewLeaseOnce();

        assertTrue(listener.isActive(),
                "Единичный сбой продления в пределах таймаута не должен снимать активную роль");
    }

    @Test
    @DisplayName("Успешный ответ ACTIVE возвращает роль и обновляет эпоху")
    void testActiveResponseRestoresRole() {
        StreamingLeaseRenewResponse response = new StreamingLeaseRenewResponse();
        response.setStatus("ACTIVE");
        response.setEpoch(7L);
        response.setLastCommittedTxid(500L);
        response.setActiveAgentId("streamer-dc1-1");
        when(orchestratorClient.renewStreamingLease(any(StreamingLeaseRenewRequest.class))).thenReturn(response);
        listener.setActiveForTesting(false, System.currentTimeMillis() - 20_000L);

        listener.renewLeaseOnce();

        assertTrue(listener.isActive(), "При статусе ACTIVE стример должен принять роль");
    }
}
