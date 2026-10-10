package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.model.ClaimTasksRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.StreamingLeaseEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.StreamingLeaseRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class StreamerSecurityAndHaTest {

    @Autowired
    private TaskService taskService;

    @Autowired
    private AgentRegistry agentRegistry;

    @Autowired
    private StreamingLeaseRepository leaseRepository;

    @Autowired
    private StreamingLeaseCoordinator leaseCoordinator;

    @Autowired
    private ReplicatorProperties properties;

    @BeforeEach
    void setUp() {
        agentRegistry.clear();
        leaseRepository.deleteAll();
        properties.getStreaming().setEnabled(true);
    }

    @Test
    @DisplayName("Изоляция безопасности: Агент в режиме streamer не имеет права забирать задачи передачи данных (403 Forbidden)")
    void shouldDenyClaimTasksForStreamerAgent() {
        // Регистрируем стример под суперпользователем
        agentRegistry.register(
                new AgentRegisterRequest("streamer-node-1", "dc1", "streamer", "streamer-node-1:50051", null, 0.0),
                properties.getAgentSecret()
        );

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () ->
                taskService.claimTasks(new ClaimTasksRequest("streamer-node-1", "dc1", 5))
        );

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        assertTrue(ex.getReason().contains("изолирован и не имеет права"),
                "Причина должна содержать предупреждение об изоляции безопасности: " + ex.getReason());
    }

    @Test
    @DisplayName("Active-Standby HA: Только один стример активен, второй в Standby; при сбое первого второй захватывает аренду со сменой эпохи")
    void shouldElectSingleActiveStreamerAndFailover() {
        // 1. Первый стример продлевает аренду -> становится ACTIVE
        StreamingLeaseRenewResponse resp1 = leaseCoordinator.renewLease(
                new StreamingLeaseRenewRequest("dc1", "streamer-node-1")
        );
        assertEquals("ACTIVE", resp1.status());
        assertEquals("streamer-node-1", resp1.activeAgentId());
        assertEquals(1L, resp1.epoch());

        // 2. Второй стример продлевает аренду -> становится STANDBY
        StreamingLeaseRenewResponse resp2 = leaseCoordinator.renewLease(
                new StreamingLeaseRenewRequest("dc1", "streamer-node-2")
        );
        assertEquals("STANDBY", resp2.status());
        assertEquals("streamer-node-1", resp2.activeAgentId());
        assertEquals(1L, resp2.epoch());

        // 3. Эмулируем падение первого стримера (аренда протухает)
        StreamingLeaseEntity lease = leaseRepository.findById("dc1").orElseThrow();
        lease.setExpiresAt(Instant.now().minusSeconds(1));
        leaseRepository.saveAndFlush(lease);

        // 4. Второй стример повторно делает запрос -> перехватывает лидерство с переходом в ACTIVE и инкрементом epoch
        StreamingLeaseRenewResponse failoverResp = leaseCoordinator.renewLease(
                new StreamingLeaseRenewRequest("dc1", "streamer-node-2")
        );
        assertEquals("ACTIVE", failoverResp.status());
        assertEquals("streamer-node-2", failoverResp.activeAgentId());
        assertEquals(2L, failoverResp.epoch(), "При failover эпоха должна инкрементироваться (Epoch Fencing)");
    }

    @Test
    @DisplayName("Аудит отказоустойчивости: При наличии только 1 стримера выставляется флаг redundancyWarning (NO_REDUNDANCY)")
    void shouldWarnWhenNoRedundancy() {
        // Зарегистрирован только один стример
        agentRegistry.register(
                new AgentRegisterRequest("streamer-node-1", "dc1", "streamer", "streamer-node-1:50051", null, 0.0),
                properties.getAgentSecret()
        );

        StreamingLeaseRenewResponse resp1 = leaseCoordinator.renewLease(
                new StreamingLeaseRenewRequest("dc1", "streamer-node-1")
        );
        assertEquals(1, resp1.registeredStreamersCount());
        assertTrue(resp1.redundancyWarning(), "Должно быть предупреждение об отсутствии резерва при ровно 1 стримере");

        // Регистрируем второй резервный стример
        agentRegistry.register(
                new AgentRegisterRequest("streamer-node-2", "dc1", "streamer", "streamer-node-2:50051", null, 0.0),
                properties.getAgentSecret()
        );

        StreamingLeaseRenewResponse resp2 = leaseCoordinator.renewLease(
                new StreamingLeaseRenewRequest("dc1", "streamer-node-1")
        );
        assertEquals(2, resp2.registeredStreamersCount());
        assertFalse(resp2.redundancyWarning(), "Предупреждение должно сняться при наличии 2+ стримеров");
    }
}
