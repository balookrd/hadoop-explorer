package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Отказ агента CDC-задачи.
 *
 * <p>Без живых агентов CDC-задача уходила в WAITING_FOR_AGENTS, а при их возвращении — в
 * QUEUED, то есть на полный повторный bootstrap: перезапуск агента-источника на стенде CDC
 * под нагрузкой отправил 26 из 30 задач переносить базы заново.
 */
@DisplayName("HMS: отказ агента CDC-задачи")
class HmsCdcFailoverTest {

    private static HmsReplicationJobEntity job(String status) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId("hms-1");
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName("sales");
        job.setTargetDbName("sales");
        job.setStatus(status);
        job.setAssignedAgentId("agent-dc1");
        job.setLastProcessedEventId(9115L);
        return job;
    }

    private static HmsCoordinatorService coordinatorWithoutLiveAgents() {
        AgentRegistry registry = mock(AgentRegistry.class);
        when(registry.getLiveHmsAgentForCluster(anyString())).thenReturn(Optional.empty());
        return new HmsCoordinatorService(mock(HmsReplicationJobRepository.class), mock(HmsEventLogRepository.class),
                registry, mock(JobService.class));
    }

    @Test
    @DisplayName("Без живых агентов CDC-задача остается ACTIVE со своей позицией")
    void activeJobKeepsCdcPositionWithoutAgents() {
        HmsReplicationJobEntity job = job("ACTIVE");

        coordinatorWithoutLiveAgents().failoverHmsJob(job, "agent-dc1", "истекла аренда");

        assertEquals("ACTIVE", job.getStatus(), "возврат агентов не должен запускать bootstrap заново");
        assertEquals(9115L, job.getLastProcessedEventId());
        assertNull(job.getAssignedAgentId(), "задача свободна для любого агента кластера");
    }

    @Test
    @DisplayName("Bootstrap без живых агентов по-прежнему ждет агентов")
    void bootstrappingJobWaitsForAgents() {
        HmsReplicationJobEntity job = job("BOOTSTRAPPING");

        coordinatorWithoutLiveAgents().failoverHmsJob(job, "agent-dc1", "истекла аренда");

        assertEquals("WAITING_FOR_AGENTS", job.getStatus());
    }
}
