package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.AgentResponseDto;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrActionResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyStopRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrReverseRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrStatusResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobRunEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DisasterRecoveryServiceTest {

    private JobRepository jobRepository;
    private JobRunRepository jobRunRepository;
    private HmsReplicationJobRepository hmsReplicationJobRepository;
    private AgentRegistry agentRegistry;
    private TopologyRegistry topologyRegistry;
    private TokenBucketThrottler throttler;
    private HmsCoordinatorService hmsCoordinatorService;

    private DisasterRecoveryService drService;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        jobRunRepository = mock(JobRunRepository.class);
        hmsReplicationJobRepository = mock(HmsReplicationJobRepository.class);
        agentRegistry = mock(AgentRegistry.class);
        topologyRegistry = mock(TopologyRegistry.class);
        throttler = mock(TokenBucketThrottler.class);
        hmsCoordinatorService = mock(HmsCoordinatorService.class);

        // Настройка дефолтной топологии
        when(topologyRegistry.getDatacenters()).thenReturn(List.of(
                new ReplicatorProperties.DatacenterConfig("dc1", "Дата-Центр 1 (Primary DC)", "zone-a"),
                new ReplicatorProperties.DatacenterConfig("dc2", "Дата-Центр 2 (Disaster Recovery)", "zone-b")
        ));
        when(topologyRegistry.getClusters()).thenReturn(List.of(
                new org.apache.hadoop.explorer.replicator.model.ClusterDto() {{
                    setId("dc1"); setName("HDFS DC1 Production"); setDcId("dc1");
                }},
                new org.apache.hadoop.explorer.replicator.model.ClusterDto() {{
                    setId("dc2"); setName("HDFS DC2 Disaster Recovery"); setDcId("dc2");
                }}
        ));

        drService = new DisasterRecoveryService(
                jobRepository,
                jobRunRepository,
                hmsReplicationJobRepository,
                agentRegistry,
                topologyRegistry,
                throttler,
                hmsCoordinatorService
        );
    }

    @Test
    @DisplayName("Должен возвращать сводный DR-статус датацентров, кластеров и маршрутов")
    void shouldReturnDrStatus() {
        // DC1 оффлайн (0 онлайн агентов), DC2 онлайн (2 агента)
        when(agentRegistry.getAgentDtos()).thenReturn(List.of(
                new AgentResponseDto("agent-dc1-1", "dc1", "host1:50051", "offline", "all", 0, null, 100, "now"),
                new AgentResponseDto("agent-dc2-1", "dc2", "host2:50051", "online", "all", 1, null, 2, "now"),
                new AgentResponseDto("agent-dc2-2", "dc2", "host3:50051", "online", "all", 0, null, 2, "now")
        ));

        JobEntity job1 = new JobEntity();
        job1.setId("job-1");
        job1.setSourceClusterId("dc1");
        job1.setTargetClusterId("dc2");
        job1.setSourcePath("/data/sales");
        job1.setTargetPath("/backup/sales");
        job1.setStatus("RUNNING");
        job1.setTotalBytes(1000L);
        job1.setCopiedBytes(400L);

        when(jobRepository.findAll()).thenReturn(List.of(job1));
        when(hmsReplicationJobRepository.findAll()).thenReturn(List.of());

        DrStatusResponse status = drService.getDrStatus();

        assertNotNull(status);
        assertEquals(2, status.datacenters().size());

        DrStatusResponse.DrDcStatus dc1 = status.datacenters().stream().filter(d -> d.id().equals("dc1")).findFirst().orElseThrow();
        assertEquals("OFFLINE", dc1.status());
        assertEquals(0, dc1.onlineAgents());

        DrStatusResponse.DrDcStatus dc2 = status.datacenters().stream().filter(d -> d.id().equals("dc2")).findFirst().orElseThrow();
        assertEquals("ONLINE", dc2.status());
        assertEquals(2, dc2.onlineAgents());

        assertEquals(1, status.hdfsRoutes().size());
        assertEquals(600L, status.hdfsRoutes().get(0).lagBytesOrEvents());
        assertEquals(600L, status.summary().unreplicatedBytes());
    }

    @Test
    @DisplayName("Должен выполнять экстренную остановку (Kill-Switch) и сетевое ограждение для DC1")
    void shouldExecuteEmergencyStop() {
        JobEntity activeJob = new JobEntity();
        activeJob.setId("job-active");
        activeJob.setSourceClusterId("dc1");
        activeJob.setTargetClusterId("dc2");
        activeJob.setStatus("RUNNING");
        activeJob.setScheduled(true);
        activeJob.setActiveRunId("run-1");

        JobRunEntity activeRun = new JobRunEntity();
        activeRun.setId("run-1");
        activeRun.setStatus("RUNNING");

        HmsReplicationJobEntity hmsJob = new HmsReplicationJobEntity();
        hmsJob.setId("hms-1");
        hmsJob.setSourceClusterId("dc1");
        hmsJob.setTargetClusterId("dc2");
        hmsJob.setStatus("ACTIVE");

        when(jobRepository.findAll()).thenReturn(List.of(activeJob));
        when(jobRunRepository.findById("run-1")).thenReturn(Optional.of(activeRun));
        when(hmsReplicationJobRepository.findAll()).thenReturn(List.of(hmsJob));

        DrEmergencyStopRequest req = new DrEmergencyStopRequest("dc1", "Авария питания в ЦОД1", true);
        DrActionResponse resp = drService.emergencyStop(req, "admin_user");

        assertTrue(resp.success());
        assertEquals(1, resp.stoppedHdfsJobs());
        assertEquals(1, resp.stoppedHmsJobs());

        assertEquals("STOPPED", activeJob.getStatus());
        assertFalse(activeJob.isScheduled(), "Планировщик должен быть отключен");
        assertNull(activeJob.getActiveRunId());
        assertTrue(activeJob.getMessage().contains("Авария питания в ЦОД1"));

        assertEquals("STOPPED", activeRun.getStatus());
        assertEquals("PAUSED", hmsJob.getStatus());

        // Проверяем сетевое ограждение
        verify(topologyRegistry, atLeastOnce()).setDcLimit(eq("dc1"), eq("dc2"), eq(0L));
    }

    @Test
    @DisplayName("Должен корректно инвертировать репликацию (Reverse Replication: DC2 ➔ DC1)")
    void shouldExecuteReverseReplication() {
        JobEntity directJob = new JobEntity();
        directJob.setId("job-direct");
        directJob.setSourceClusterId("dc1");
        directJob.setTargetClusterId("dc2");
        directJob.setSourcePath("/data/production/events");
        directJob.setTargetPath("/backup/mirror/events");
        directJob.setStatus("STOPPED");
        directJob.setTotalBytes(5000L);
        directJob.setScheduled(true);
        directJob.setCronExpression("*/10 * * * *");
        directJob.setExecutionPrincipal("writer@REALM.LOCAL");
        directJob.setRunAsServiceAccount(true);

        when(jobRepository.findAll()).thenReturn(List.of(directJob));
        when(hmsReplicationJobRepository.findAll()).thenReturn(List.of());

        DrReverseRequest req = new DrReverseRequest("dc2", "dc1", true, true, true);
        DrActionResponse resp = drService.reverseReplication(req, "sre_engineer");

        assertTrue(resp.success());
        assertEquals(1, resp.reversedHdfsJobs());
        assertEquals(0, resp.reversedHmsJobs());
        assertEquals(1, resp.createdJobIds().size());

        ArgumentCaptor<JobEntity> jobCaptor = ArgumentCaptor.forClass(JobEntity.class);
        verify(jobRepository, atLeastOnce()).save(jobCaptor.capture());

        JobEntity createdReverseJob = jobCaptor.getAllValues().stream()
                .filter(j -> j.getId().startsWith("rev-"))
                .findFirst()
                .orElseThrow();

        // Проверяем инверсию путей и кластеров
        assertEquals("dc2", createdReverseJob.getSourceClusterId());
        assertEquals("dc1", createdReverseJob.getTargetClusterId());
        assertEquals("/backup/mirror/events", createdReverseJob.getSourcePath());
        assertEquals("/data/production/events", createdReverseJob.getTargetPath());
        assertEquals("QUEUED", createdReverseJob.getStatus());
        assertTrue(createdReverseJob.isScheduled());
        assertEquals("*/10 * * * *", createdReverseJob.getCronExpression());

        // Проверяем снятие сетевого ограждения
        verify(topologyRegistry, atLeastOnce()).setDcLimit(eq("dc2"), eq("dc1"), eq(100L * 1024 * 1024));
    }

    @Test
    @DisplayName("Должен разворачивать точечную HDFS задачу в обратную сторону")
    void shouldReverseSingleHdfsJob() {
        JobEntity job = new JobEntity();
        job.setId("job-single");
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourcePath("/data/warehouse/raw");
        job.setTargetPath("/backup/warehouse/raw");
        job.setTotalBytes(12345L);

        when(jobRepository.findById("job-single")).thenReturn(Optional.of(job));

        DrActionResponse resp = drService.reverseSingleHdfsJob("job-single", "admin");

        assertTrue(resp.success());
        assertEquals(1, resp.reversedHdfsJobs());

        ArgumentCaptor<JobEntity> captor = ArgumentCaptor.forClass(JobEntity.class);
        verify(jobRepository, atLeastOnce()).save(captor.capture());

        JobEntity rev = captor.getValue();
        assertEquals("dc2", rev.getSourceClusterId());
        assertEquals("dc1", rev.getTargetClusterId());
        assertEquals("/backup/warehouse/raw", rev.getSourcePath());
        assertEquals("/data/warehouse/raw", rev.getTargetPath());
    }
}
