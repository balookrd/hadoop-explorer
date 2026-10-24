package org.apache.hadoop.explorer.replicator.orchestrator.registry;

import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Вычисление состояния агентов без мутации на горячем пути.
 *
 * <p>{@code refreshHealth()} вызывался из каждого геттера реестра: полный проход по всем
 * агентам с записью статуса. В цикле забора задач это давало десятки тысяч итераций на один
 * вызов, а поля {@code AgentEntry} при этом менялись из heartbeat-потоков параллельно с
 * чтением — без синхронизации и без {@code volatile}.
 */
@DisplayName("Состояние агентов: ленивое вычисление и потокобезопасность")
class AgentHealthEvaluationTest {

    private static final String SECRET = "test-agent-secret-0123456789";

    private ReplicatorProperties properties;
    private AgentRegistry registry;

    @BeforeEach
    void setUp() {
        properties = new ReplicatorProperties();
        properties.setAgentSecret(SECRET);
        properties.setEnforceClusterWhitelist(false);
        properties.setAgentHeartbeatTimeoutSeconds(15);
        properties.setAgentOfflineTimeoutSeconds(45);
        registry = new AgentRegistry(properties, new TopologyRegistry(properties));
    }

    private void registerAgent(String agentId, String clusterId) {
        registry.register(new AgentRegisterRequest(
                agentId, clusterId, "all", "node-" + agentId + ":50051", "node-" + agentId, 100.0), SECRET);
    }

    @Test
    @DisplayName("Агент с истекшим heartbeat не выдается как живой")
    void testStaleAgentIsNotReturnedAsLive() {
        registerAgent("agent-stale", "dc1");
        AgentRegistry.AgentEntry entry = registry.getAgents().stream()
                .filter(a -> "agent-stale".equals(a.getAgentId()))
                .findFirst().orElseThrow();

        // Heartbeat не приходил дольше порога offline
        entry.setLastHeartbeat(Instant.now().minusSeconds(120));

        assertTrue(registry.getLiveHmsAgentForCluster("dc1").isEmpty(),
                "Агент без heartbeat дольше порога не может считаться живым");
    }

    @Test
    @DisplayName("Свежий heartbeat возвращает агента в живые")
    void testFreshHeartbeatMakesAgentLive() {
        registerAgent("agent-fresh", "dc1");
        AgentRegistry.AgentEntry entry = registry.getAgents().stream()
                .filter(a -> "agent-fresh".equals(a.getAgentId()))
                .findFirst().orElseThrow();
        entry.setLastHeartbeat(Instant.now().minusSeconds(120));
        assertTrue(registry.getLiveHmsAgentForCluster("dc1").isEmpty());

        entry.setLastHeartbeat(Instant.now());

        assertTrue(registry.getLiveHmsAgentForCluster("dc1").isPresent(),
                "После свежего heartbeat агент снова доступен для выдачи задач");
    }

    @Test
    @DisplayName("Чтение реестра во время heartbeat не падает и не теряет агентов")
    void testConcurrentHeartbeatAndReadsAreSafe() throws Exception {
        for (int i = 0; i < 20; i++) {
            registerAgent("agent-" + i, "dc1");
        }

        List<Throwable> failures = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(2);

        Thread writer = new Thread(() -> {
            try {
                for (int round = 0; round < 500; round++) {
                    for (int i = 0; i < 20; i++) {
                        registry.heartbeat(new org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest(
                                "agent-" + i, "dc1", "node-agent-" + i + ":50051", 0), SECRET);
                    }
                }
            } catch (Throwable t) {
                failures.add(t);
            } finally {
                done.countDown();
            }
        }, "heartbeat-writer");

        Thread reader = new Thread(() -> {
            try {
                for (int round = 0; round < 500; round++) {
                    assertEquals(20, registry.getAgents().size());
                    registry.getLiveHmsAgentForCluster("dc1");
                }
            } catch (Throwable t) {
                failures.add(t);
            } finally {
                done.countDown();
            }
        }, "registry-reader");

        writer.start();
        reader.start();
        assertTrue(done.await(60, TimeUnit.SECONDS), "Потоки должны завершиться");

        assertTrue(failures.isEmpty(),
                "Конкурентное чтение реестра упало: " + (failures.isEmpty() ? "" : failures.get(0)));
        assertEquals(20, registry.getAgents().size());
    }
}
