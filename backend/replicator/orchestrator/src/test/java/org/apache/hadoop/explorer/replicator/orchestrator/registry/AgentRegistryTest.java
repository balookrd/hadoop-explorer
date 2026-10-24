package org.apache.hadoop.explorer.replicator.orchestrator.registry;

import org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.AgentEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.AgentRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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

    @Test
    @DisplayName("Должен защищать статический gRPC адрес кластера от перезаписи динамическим агентом")
    void shouldProtectStaticClusterGrpcAddressFromHijacking() {
        ReplicatorProperties customProps = new ReplicatorProperties();
        customProps.setAgentSecret("secret-123");

        ReplicatorProperties.ClusterConfig allowedCluster = new ReplicatorProperties.ClusterConfig();
        allowedCluster.setId("prod-hdfs");
        allowedCluster.setName("Production HDFS");
        allowedCluster.setGrpcHost("gateway.prod.corp");
        allowedCluster.setGrpcPort(50051);
        customProps.setClusters(java.util.List.of(allowedCluster));

        TopologyRegistry customTopology = new TopologyRegistry(customProps);
        AgentRegistry customRegistry = new AgentRegistry(customProps, customTopology);

        AgentRegisterRequest agentReg = new AgentRegisterRequest("node-1", "prod-hdfs", "all", "node-1:50051", "node-1", 100.0);
        assertTrue(customRegistry.register(agentReg, "secret-123"));

        assertEquals("gateway.prod.corp:50051", customTopology.getClusterGrpcAddress("prod-hdfs"));
    }

    @Test
    @DisplayName("SEC-13: Должен блокировать Loopback адреса при allowLoopbackAgents=false")
    void shouldBlockLoopbackWhenDisallowed() {
        ReplicatorProperties customProps = new ReplicatorProperties();
        customProps.setAgentSecret("secret-123");
        customProps.setAllowLoopbackAgents(false);

        AgentRegistry secureRegistry = new AgentRegistry(customProps, new TopologyRegistry(customProps));

        SecurityException ex1 = assertThrows(SecurityException.class, () -> secureRegistry.validateGrpcAddress("127.0.0.1:50051"));
        assertTrue(ex1.getMessage().contains("Loopback"));

        SecurityException ex2 = assertThrows(SecurityException.class, () -> secureRegistry.validateGrpcAddress("localhost:50051"));
        assertTrue(ex2.getMessage().contains("Loopback"));

        SecurityException ex3 = assertThrows(SecurityException.class, () -> secureRegistry.validateGrpcAddress("[::1]:50051"));
        assertTrue(ex3.getMessage().contains("Loopback"));
    }

    @Test
    @DisplayName("SEC-13: Должен блокировать некорректный синтаксис FQDN и недопустимые IP адреса")
    void shouldBlockInvalidFqdnAndIpSyntax() {
        assertThrows(SecurityException.class, () -> registry.validateGrpcAddress("malicious..host:50051"));
        assertThrows(SecurityException.class, () -> registry.validateGrpcAddress(".malicious.host:50051"));
        assertThrows(SecurityException.class, () -> registry.validateGrpcAddress("malicious.host.:50051"));
        assertThrows(SecurityException.class, () -> registry.validateGrpcAddress("999.999.999.999:50051"));
    }

    @Test
    @DisplayName("SEC-12 & SEC-16: Проверка строгого режима аутентификации агентов (enforceAgentSecret)")
    void shouldEnforceAgentSecretModes() {
        ReplicatorProperties enforcedProps = new ReplicatorProperties();
        enforcedProps.setAgentSecret("custom-super-secret-key-67890");
        enforcedProps.setEnforceAgentSecret(true);

        AgentRegistry enforcedRegistry = new AgentRegistry(enforcedProps, new TopologyRegistry(enforcedProps));

        // В строгом режиме отсутствие заголовка вызывает отказ
        SecurityException exMissing = assertThrows(SecurityException.class, () ->
                enforcedRegistry.register(new AgentRegisterRequest("agent-x", "dc1", "all", "node1:50051", "node1", 50.0), null));
        assertTrue(exMissing.getMessage().contains("Отсутствует X-Agent-Secret"));

        // Неверный токен отклоняется
        SecurityException exInvalid = assertThrows(SecurityException.class, () ->
                enforcedRegistry.register(new AgentRegisterRequest("agent-x", "dc1", "all", "node1:50051", "node1", 50.0), "bad-token"));
        assertTrue(exInvalid.getMessage().contains("Неверный X-Agent-Secret"));

        // Корректный токен успешно проходит регистрацию
        assertTrue(enforcedRegistry.register(
                new AgentRegisterRequest("agent-x", "dc1", "all", "node1:50051", "node1", 50.0), "custom-super-secret-key-67890"));
    }

    @Test
    @DisplayName("RES-01: Должен сохранять агентов в AgentRepository и восстанавливать при старте")
    void shouldPersistAndRestoreAgentsFromRepository() {
        AgentRepository mockRepo = mock(AgentRepository.class);
        AgentRegistry repoRegistry = new AgentRegistry(props, new TopologyRegistry(props), mockRepo);

        // Регистрация -> save
        AgentRegisterRequest reg = new AgentRegisterRequest("agent-db-1", "dc1", "all", "node1:50051", "node1", 100.0);
        assertTrue(repoRegistry.register(reg, "secret-123"));
        verify(mockRepo, times(1)).save(any(AgentEntity.class));

        // Heartbeat -> пометка dirty (Write-Behind) и отложенный пакетный сброс в БД
        AgentHeartbeatRequest hb = new AgentHeartbeatRequest("agent-db-1", "dc1", "node1:50051", 3);
        assertTrue(repoRegistry.heartbeat(hb, "secret-123"));
        verify(mockRepo, times(1)).save(any(AgentEntity.class)); // save не должен вызываться синхронно на heartbeat
        int flushed = repoRegistry.flushDirtyAgentsToDatabase();
        assertEquals(1, flushed, "Должен быть сброшен 1 dirty-агент");
        verify(mockRepo, times(1)).saveAll(any()); // сброс происходит через пакетный saveAll

        // Unregister -> deleteById
        assertTrue(repoRegistry.unregister("agent-db-1", "secret-123"));
        verify(mockRepo, times(1)).deleteById("agent-db-1");

        // Восстановление из БД при инициализации
        AgentEntity savedEntity = new AgentEntity();
        savedEntity.setAgentId("agent-restored");
        savedEntity.setClusterId("dc-restored");
        savedEntity.setHost("node-restored");
        savedEntity.setGrpcPort(50051);
        savedEntity.setGrpcAddress("node-restored:50051");
        savedEntity.setMode("all");
        savedEntity.setStatus("ONLINE");
        when(mockRepo.findAll()).thenReturn(List.of(savedEntity));

        AgentRegistry newRegistry = new AgentRegistry(props, new TopologyRegistry(props), mockRepo);
        newRegistry.initSecurityValidation(); // вызывает loadAgentsFromDatabase

        assertEquals(1, newRegistry.getAgents().size());
        assertEquals("agent-restored", newRegistry.getAgents().get(0).getAgentId());
        assertEquals("node-restored:50051", newRegistry.getAgents().get(0).getGrpcAddress());
    }

    @Test
    @DisplayName("Должен корректно определять наличие и количество живых streamer-агентов для кластера")
    void shouldDetectLiveStreamersForCluster() {
        assertFalse(registry.hasLiveStreamerForCluster("dc1"));
        assertEquals(0, registry.countLiveStreamers("dc1"));

        // Регистрируем обычного агента (mode=all)
        AgentRegisterRequest normalAgent = new AgentRegisterRequest("agent-all", "dc1", "all", "node1:50051", "node1", 100.0);
        assertTrue(registry.register(normalAgent, "secret-123"));
        assertFalse(registry.hasLiveStreamerForCluster("dc1"), "Агент с mode=all не должен считаться выделенным streamer-агентом");

        // Регистрируем стример (mode=streamer)
        AgentRegisterRequest streamerAgent = new AgentRegisterRequest("streamer-1", "dc1", "streamer", "streamer1:50051", "streamer1", 100.0);
        assertTrue(registry.register(streamerAgent, "secret-123"));
        assertTrue(registry.hasLiveStreamerForCluster("dc1"));
        assertEquals(1, registry.countLiveStreamers("dc1"));

        // Для другого кластера dc2 стримеров нет
        assertFalse(registry.hasLiveStreamerForCluster("dc2"));
        assertEquals(0, registry.countLiveStreamers("dc2"));

        // После unregister стримера count становится 0
        assertTrue(registry.unregister("streamer-1", "secret-123"));
        assertFalse(registry.hasLiveStreamerForCluster("dc1"));
        assertEquals(0, registry.countLiveStreamers("dc1"));
    }
}

