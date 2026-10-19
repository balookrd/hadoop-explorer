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

    @Test
    @DisplayName("Должен блокировать расширенные SSRF адреса (Alibaba, IPv6 link-local, инъекции)")
    void shouldBlockSsrfExtended() {
        assertThrows(SecurityException.class, () -> registry.validateGrpcAddress("169.254.1.1:50051"));
        assertThrows(SecurityException.class, () -> registry.validateGrpcAddress("100.100.100.200:50051"));
        assertThrows(SecurityException.class, () -> registry.validateGrpcAddress("fe80::1:50051"));
        assertThrows(IllegalArgumentException.class, () -> registry.validateGrpcAddress("bad host$:50051"));
        assertThrows(IllegalArgumentException.class, () -> registry.validateGrpcAddress("node1:70000"));
        assertThrows(IllegalArgumentException.class, () -> registry.validateGrpcAddress("node1:-1"));
    }

    @Test
    @DisplayName("Должен блокировать смену кластера для активного агента (Cluster Hijacking)")
    void shouldBlockClusterHijacking() {
        AgentRegisterRequest reg1 = new AgentRegisterRequest("agent-hijack", "dc-source", "all", "node1:50051", "node1", 100.0);
        assertTrue(registry.register(reg1, "secret-123"));

        // Попытка зарегистрировать тот же agentId в другом кластере
        AgentRegisterRequest reg2 = new AgentRegisterRequest("agent-hijack", "dc-target", "all", "node1:50051", "node1", 100.0);
        SecurityException exReg = assertThrows(SecurityException.class, () -> registry.register(reg2, "secret-123"));
        assertTrue(exReg.getMessage().contains("Попытка смены кластера"));

        // Попытка смены кластера через heartbeat
        AgentHeartbeatRequest hb = new AgentHeartbeatRequest("agent-hijack", "dc-target", "node1:50051", 1);
        SecurityException exHb = assertThrows(SecurityException.class, () -> registry.heartbeat(hb, "secret-123"));
        assertTrue(exHb.getMessage().contains("Попытка смены кластера"));
    }

    @Test
    @DisplayName("Должен блокировать регистрацию в неразрешенном кластере при enforceClusterWhitelist")
    void shouldEnforceClusterWhitelistWhenEnabled() {
        ReplicatorProperties customProps = new ReplicatorProperties();
        customProps.setAgentSecret("secret-123");
        customProps.setEnforceClusterWhitelist(true);

        ReplicatorProperties.ClusterConfig allowedCluster = new ReplicatorProperties.ClusterConfig();
        allowedCluster.setId("prod-hdfs");
        allowedCluster.setName("Production HDFS");
        customProps.setClusters(java.util.List.of(allowedCluster));

        TopologyRegistry customTopology = new TopologyRegistry(customProps);
        AgentRegistry customRegistry = new AgentRegistry(customProps, customTopology);

        // Регистрация в разрешенном кластере
        AgentRegisterRequest valid = new AgentRegisterRequest("valid-agent", "prod-hdfs", "all", "node1:50051", "node1", 100.0);
        assertTrue(customRegistry.register(valid, "secret-123"));

        // Регистрация в неизвестном кластере
        AgentRegisterRequest invalid = new AgentRegisterRequest("rogue-agent", "unknown-cluster", "all", "node2:50051", "node2", 100.0);
        SecurityException ex = assertThrows(SecurityException.class, () -> customRegistry.register(invalid, "secret-123"));
        assertTrue(ex.getMessage().contains("enforce-cluster-whitelist"));
    }

    @Test
    @DisplayName("Должен блокировать SSRF адреса, переданные в heartbeat")
    void shouldBlockSsrfInHeartbeat() {
        AgentHeartbeatRequest hb = new AgentHeartbeatRequest("hb-agent", "dc1", "169.254.169.254:50051", 0);
        assertThrows(SecurityException.class, () -> registry.heartbeat(hb, "secret-123"));
    }
}
