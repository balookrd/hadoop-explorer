package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrReverseRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Выборки заданий в сценариях аварийного переключения.
 *
 * <p>Инверсия репликации, отзыв обратной задачи и эвакуация задач отключившегося агента читали
 * свои таблицы целиком и отбирали нужное в памяти. Это худший момент для лишней памяти:
 * переключение выполняется, когда один из ЦОД уже недоступен.
 */
@DisplayName("DR: выборки не читают таблицы целиком")
class DrJobScanTest {

    private JobRepository jobRepository;
    private HmsReplicationJobRepository hmsJobRepository;
    private DisasterRecoveryService drService;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        hmsJobRepository = mock(HmsReplicationJobRepository.class);
        TopologyRegistry topologyRegistry = mock(TopologyRegistry.class);

        when(topologyRegistry.getDatacenters()).thenReturn(List.of(
                new ReplicatorProperties.DatacenterConfig("dc1", "Дата-Центр 1", "zone-a"),
                new ReplicatorProperties.DatacenterConfig("dc2", "Дата-Центр 2", "zone-b")
        ));
        when(topologyRegistry.getClusters()).thenReturn(List.of());

        drService = new DisasterRecoveryService(
                jobRepository,
                mock(JobRunRepository.class),
                mock(TaskRepository.class),
                hmsJobRepository,
                mock(AgentRegistry.class),
                topologyRegistry,
                mock(TokenBucketThrottler.class),
                mock(HmsCoordinatorService.class)
        );
    }

    private JobEntity job(String id, String source, String target) {
        JobEntity job = new JobEntity();
        job.setId(id);
        job.setSourceClusterId(source);
        job.setTargetClusterId(target);
        job.setSourcePath("/data/" + id);
        job.setTargetPath("/backup/" + id);
        job.setStatus("STOPPED");
        return job;
    }

    @Test
    @DisplayName("Инверсия репликации выбирает задания маршрута запросом")
    void reverseReplicationShouldQueryRoute() {
        // Запрос reverse(from=dc2, to=dc1) ищет прямые задания маршрута dc1 ➔ dc2
        when(jobRepository.findByNormalizedRoute("dc1", "dc2"))
                .thenReturn(List.of(job("job-direct", "dc1", "dc2")));
        when(hmsJobRepository.findByNormalizedRoute("dc1", "dc2")).thenReturn(List.of());
        when(jobRepository.save(org.mockito.ArgumentMatchers.any(JobEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        var response = drService.reverseReplication(new DrReverseRequest("dc2", "dc1", true, true, true), "sre");

        assertEquals(1, response.reversedHdfsJobs(), "Прямое задание маршрута должно быть развернуто");
        verify(jobRepository).findByNormalizedRoute("dc1", "dc2");
        verify(jobRepository, never()).findAll();
        verify(hmsJobRepository, never()).findAll();
    }

    @Test
    @DisplayName("Отзыв обратной задачи ищет зеркало по обратному маршруту")
    void undoReverseShouldQueryMirrorRoute() {
        JobEntity direct = job("job-direct-1", "dc1", "dc2");
        JobEntity mirror = job("rev-77", "dc2", "dc1");
        mirror.setSourcePath(direct.getTargetPath());
        mirror.setTargetPath(direct.getSourcePath());

        when(jobRepository.findById("job-direct-1")).thenReturn(Optional.of(direct));
        when(jobRepository.findByNormalizedRoute("dc2", "dc1")).thenReturn(List.of(mirror));
        when(hmsJobRepository.findById(anyString())).thenReturn(Optional.empty());

        drService.undoReverse("job-direct-1", "sre");

        verify(jobRepository).findByNormalizedRoute("dc2", "dc1");
        verify(jobRepository, never()).findAll();
    }

    @Test
    @DisplayName("Эвакуация задач HMS выбирает задачи отключившегося агента запросом")
    void hmsFailoverShouldQueryByAgent() {
        HmsReplicationJobRepository hmsRepository = mock(HmsReplicationJobRepository.class);
        HmsReplicationJobEntity assigned = new HmsReplicationJobEntity();
        assigned.setId("hms-1");
        assigned.setSourceClusterId("dc1");
        assigned.setTargetClusterId("dc2");
        assigned.setSourceDbName("analytics");
        assigned.setTargetDbName("analytics_replica");
        assigned.setStatus("ACTIVE");
        assigned.setAssignedAgentId("agent-dead");
        when(hmsRepository.findByAssignedAgentId("agent-dead")).thenReturn(List.of(assigned));

        HmsCoordinatorService coordinator = new HmsCoordinatorService(
                hmsRepository, mock(HmsEventLogRepository.class),
                mock(AgentRegistry.class), mock(JobService.class));

        int evacuated = coordinator.failoverHmsJobsForAgent("agent-dead", "агент отключился");

        assertEquals(1, evacuated);
        verify(hmsRepository).findByAssignedAgentId("agent-dead");
        verify(hmsRepository, never()).findAll();
    }
}
