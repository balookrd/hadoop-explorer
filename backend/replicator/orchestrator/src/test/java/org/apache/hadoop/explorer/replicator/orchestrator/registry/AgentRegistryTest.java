package org.apache.hadoop.explorer.replicator.orchestrator.registry;

import org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AgentRegistryTest {

    private final ReplicatorProperties props;
    private final AgentRegistry registry;

    AgentRegistryTest() {
        this.props = new ReplicatorProperties();
        this.props.setAgentSecret("secret-123");
        this.registry = new AgentRegistry(props, new TopologyRegistry(props));
    }

    @Test
    @DisplayName("Должен успешно регистрировать агента и принимать heartbeat")
    void shouldRegisterAndHeartbeat() {
        AgentRegisterRequest reg = new AgentRegisterRequest("agent-1", "dc1", "all", "node1:50051", "node1", 100.0);
        boolean registered = registry.register(reg, "secret-123");
        assertTrue(registered);

        assertEquals(1, registry.getAgents().size());
        assertEquals(AgentRegistry.AgentStatus.ONLINE, registry.getAgents().get(0).getStatus());

        // Heartbeat
        AgentHeartbeatRequest hb = new AgentHeartbeatRequest("agent-1", "dc1", "node1:50051", 2);
        boolean heartbeated = registry.heartbeat(hb, "secret-123");
        assertTrue(heartbeated);

        // Unregister
        boolean unregistered = registry.unregister("agent-1", "secret-123");
        assertTrue(unregistered);
        assertEquals(0, registry.getAgents().size());
    }

    @Test
    @DisplayName("Должен отклонять регистрацию с неверным секретом")
    void shouldRejectInvalidSecret() {
        AgentRegisterRequest reg = new AgentRegisterRequest("agent-bad", "dc1", "all", "node2:50051", "node2", 100.0);
        assertThrows(SecurityException.class, () -> registry.register(reg, "wrong-secret"));
    }

    @Test
    @DisplayName("Должен блокировать SSRF попытки через облачные метаданные")
    void shouldBlockSsrfCloudMetadata() {
        assertThrows(SecurityException.class, () -> registry.validateGrpcAddress("169.254.169.254:50051"));
        assertThrows(SecurityException.class, () -> registry.validateGrpcAddress("metadata.google.internal:50051"));
    }
}
