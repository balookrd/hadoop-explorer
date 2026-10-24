package org.apache.hadoop.explorer.replicator.orchestrator.registry;

import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.AgentRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Определение живости агента узлом, который не принимает его heartbeat.
 *
 * <p>Реестр агентов — локальная карта узла: heartbeat приходит на тот узел, который выбрал
 * балансировщик, а в БД уходит write-behind. Планировщик failover под распределенной
 * блокировкой может выполняться на другом узле, где об агенте вообще нет записи в памяти, —
 * и тогда живые агенты считаются офлайновыми, а их активные задачи массово переназначаются
 * с дублированием передач. Решение о живости должно опираться на БД.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Active-active: живость агента по данным БД")
class AgentLivenessAcrossNodesTest {

    private static final String SECRET = "test-orchestrator-agent-secret-0123456789";

    @Autowired
    private AgentRegistry registry;

    @Autowired
    private AgentRepository agentRepository;

    @Autowired
    private ReplicatorProperties properties;

    @BeforeEach
    void setUp() {
        registry.clear();
        agentRepository.deleteAll();
    }

    /** Второй узел кластера: своя память реестра, та же база. */
    private AgentRegistry secondNode() {
        return new AgentRegistry(properties, new TopologyRegistry(properties), agentRepository);
    }

    @Test
    @DisplayName("Агент, зарегистрированный на другом узле, считается живым")
    void testAgentRegisteredElsewhereIsLive() {
        registry.register(new AgentRegisterRequest(
                "agent-dc1-1", "dc1", "all", "node-dc1:50051", "node-dc1", 100.0), SECRET);

        AgentRegistry otherNode = secondNode();
        otherNode.clear(); // узел еще не получал heartbeat этого агента

        assertTrue(otherNode.isAgentLive("agent-dc1-1"),
                "Узел без локальной записи обязан узнать о живом агенте из БД, "
                        + "иначе его задачи будут переназначены как брошенные");
    }

    @Test
    @DisplayName("Агент с устаревшим heartbeat в БД живым не считается")
    void testStaleAgentInDatabaseIsNotLive() {
        registry.register(new AgentRegisterRequest(
                "agent-dc1-2", "dc1", "all", "node-dc1:50051", "node-dc1", 100.0), SECRET);

        var entity = agentRepository.findById("agent-dc1-2").orElseThrow();
        entity.setLastHeartbeat(Instant.now().minusSeconds(600));
        agentRepository.save(entity);

        AgentRegistry otherNode = secondNode();
        otherNode.clear();

        assertFalse(otherNode.isAgentLive("agent-dc1-2"),
                "Давно замолчавший агент не должен считаться живым ни на одном узле");
    }

    @Test
    @DisplayName("Неизвестный агент живым не считается")
    void testUnknownAgentIsNotLive() {
        assertFalse(secondNode().isAgentLive("agent-never-seen"));
        assertFalse(registry.isAgentLive(null));
    }

    @Test
    @DisplayName("Локальный свежий heartbeat имеет приоритет над записью в БД")
    void testLocalHeartbeatWinsOverStaleDatabaseRow() {
        registry.register(new AgentRegisterRequest(
                "agent-dc1-3", "dc1", "all", "node-dc1:50051", "node-dc1", 100.0), SECRET);

        // В БД запись отстала (write-behind), локально heartbeat только что пришел
        var entity = agentRepository.findById("agent-dc1-3").orElseThrow();
        entity.setLastHeartbeat(Instant.now().minusSeconds(600));
        agentRepository.save(entity);

        assertTrue(registry.isAgentLive("agent-dc1-3"),
                "Узел, принимающий heartbeat, не должен считать агента мертвым из-за отставания БД");
    }
}
