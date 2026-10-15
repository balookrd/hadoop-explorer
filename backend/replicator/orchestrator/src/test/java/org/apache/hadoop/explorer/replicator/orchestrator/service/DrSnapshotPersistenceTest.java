package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyRollbackRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyStopRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrStatusResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.DrSnapshotEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.*;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DrSnapshotPersistenceTest {

    private JobRepository jobRepository;
    private JobRunRepository jobRunRepository;
    private TaskRepository taskRepository;
    private HmsReplicationJobRepository hmsReplicationJobRepository;
    private AgentRegistry agentRegistry;
    private TopologyRegistry topologyRegistry;
    private TokenBucketThrottler throttler;
    private HmsCoordinatorService hmsCoordinatorService;
    private DrSnapshotRepository drSnapshotRepository;

    private DisasterRecoveryService drService;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        jobRunRepository = mock(JobRunRepository.class);
        taskRepository = mock(TaskRepository.class);
        hmsReplicationJobRepository = mock(HmsReplicationJobRepository.class);
        agentRegistry = mock(AgentRegistry.class);
        topologyRegistry = mock(TopologyRegistry.class);
        throttler = mock(TokenBucketThrottler.class);
        hmsCoordinatorService = mock(HmsCoordinatorService.class);
        drSnapshotRepository = mock(DrSnapshotRepository.class);

        when(topologyRegistry.getDatacenters()).thenReturn(List.of(
                new ReplicatorProperties.DatacenterConfig("dc1", "DC1", "zone-a"),
                new ReplicatorProperties.DatacenterConfig("dc2", "DC2", "zone-b")
        ));
        when(topologyRegistry.getClusters()).thenReturn(Collections.emptyList());

        drService = new DisasterRecoveryService(
                jobRepository,
                jobRunRepository,
                taskRepository,
                hmsReplicationJobRepository,
                agentRegistry,
                topologyRegistry,
                throttler,
                hmsCoordinatorService,
                drSnapshotRepository
        );
    }

    @Test
    @DisplayName("P1: emergencyStopCluster сохраняет снимок в БД, а rollbackEmergencyStop удаляет его")
    void shouldPersistAndRemoveSnapshotInDatabase() {
        when(jobRepository.findAll()).thenReturn(Collections.emptyList());
        when(hmsReplicationJobRepository.findAll()).thenReturn(Collections.emptyList());

        // 1. Аварийный останов
        DrEmergencyStopRequest stopReq = new DrEmergencyStopRequest("dc1", "DataCenter fire", true);
        drService.emergencyStop(stopReq, "admin");

        ArgumentCaptor<DrSnapshotEntity> captor = ArgumentCaptor.forClass(DrSnapshotEntity.class);
        verify(drSnapshotRepository, times(1)).save(captor.capture());

        DrSnapshotEntity saved = captor.getValue();
        assertEquals("dc1", saved.getId());
        assertEquals("dc1", saved.getClusterId());
        assertEquals("admin", saved.getOperator());
        assertTrue(saved.isNetworkFenced());
        assertNotNull(saved.getSnapshotJson());
        assertTrue(saved.getSnapshotJson().contains("\"clusterId\":\"dc1\""));

        // 2. Откат останова
        DrEmergencyRollbackRequest rollReq = new DrEmergencyRollbackRequest("dc1", true, true, true);
        drService.rollbackEmergencyStop(rollReq, "admin");

        verify(drSnapshotRepository, atLeastOnce()).deleteById("dc1");
    }

    @Test
    @DisplayName("P1: loadPersistedSnapshots восстанавливает активные снимки из БД при старте сервиса")
    void shouldRestoreSnapshotsOnStartup() {
        String json = "{\"clusterId\":\"dc2\",\"operator\":\"ops\",\"reason\":\"fencing test\",\"timestamp\":\"2026-10-15T20:00:00Z\",\"networkFenced\":true,\"previousDcLimits\":{},\"stoppedHdfsJobIds\":[],\"previousHdfsScheduled\":{},\"pausedHmsJobIds\":[]}";
        DrSnapshotEntity entity = new DrSnapshotEntity("dc2", "dc2", "ops", "fencing test", java.time.Instant.now(), true, json);

        when(drSnapshotRepository.findAll()).thenReturn(List.of(entity));

        // Вызываем инициализацию из БД
        drService.loadPersistedSnapshots();

        // Проверяем, что снимок попал в статус DR
        var status = drService.getDrStatus();
        assertNotNull(status);
        boolean dc2Fenced = status.datacenters().stream()
                .filter(d -> "dc2".equalsIgnoreCase(d.id()))
                .anyMatch(DrStatusResponse.DrDcStatus::isFenced);
        assertTrue(dc2Fenced, "Восстановленный снимок из БД должен помечать дата-центр dc2 как огражденный (isFenced)");
    }
}
