package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
public class HmsSubjobIsolationTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @Autowired
    private HmsEventLogRepository hmsEventLogRepository;

    @Test
    @DisplayName("HMS подзадачи исключены из общего списка регламентных задач HDFS, но доступны по parentJobId")
    void testHmsSubjobExclusionFromStandardList() {
        String parentHmsJobId = "hms-job-sales-" + UUID.randomUUID();

        // 1. Создаем регламентную HDFS задачу
        CreateJobRequest standardReq = new CreateJobRequest(
                "/data/sales/daily",
                "/backup/sales/daily",
                "dc1",
                "dc2",
                1024L,
                "hdfs@REALM.LOCAL",
                true,
                false,
                null,
                20,
                "STANDARD",
                null
        );
        JobResponse stdJob = jobService.createJob(standardReq, "admin");

        // 2. Создаем служебную подзадачу метастора
        CreateJobRequest subjobReq = new CreateJobRequest(
                "/warehouse/sales/dt=2026-10-09",
                "/backup/warehouse/sales/dt=2026-10-09",
                "dc1",
                "dc2",
                2048L,
                "hdfs@REALM.LOCAL",
                true,
                false,
                null,
                20,
                "HMS_SUBJOB",
                parentHmsJobId
        );
        JobResponse subJob = jobService.createJob(subjobReq, "system_operator");

        // 3. Проверяем, что в регламентном списке HDFS видны только STANDARD задачи
        List<JobResponse> standardJobs = jobService.listJobs(null, "admin", true, false);
        boolean containsStd = standardJobs.stream().anyMatch(j -> j.id().equals(stdJob.id()));
        boolean containsSubjob = standardJobs.stream().anyMatch(j -> j.id().equals(subJob.id()));

        assertTrue(containsStd, "Регламентная задача HDFS должна быть видна в общем списке");
        assertFalse(containsSubjob, "Служебная подзадача HMS_SUBJOB не должна быть видна в регламентном списке HDFS");

        // 4. Проверяем, что подзадача доступна через listSubjobsByParentId
        List<JobResponse> parentSubjobs = jobService.listSubjobsByParentId(parentHmsJobId);
        assertEquals(1, parentSubjobs.size());
        assertEquals(subJob.id(), parentSubjobs.get(0).id());
        assertEquals("HMS_SUBJOB", parentSubjobs.get(0).jobType());
        assertEquals(parentHmsJobId, parentSubjobs.get(0).parentJobId());
    }

    @Test
    @DisplayName("Проверка сохранения и выборки HmsReplicationJobEntity и HmsEventLogEntity")
    void testHmsEntitiesPersistence() {
        String jobId = "hms-sync-" + UUID.randomUUID();

        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId(jobId);
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName("analytics");
        job.setTargetDbName("analytics_dr");
        job.setStatus("BOOTSTRAPPING");
        job.setBootstrapEventId(1050L);
        job.setLastProcessedEventId(1050L);
        hmsJobRepository.save(job);

        assertTrue(hmsJobRepository.findById(jobId).isPresent());
        HmsReplicationJobEntity loaded = hmsJobRepository.findById(jobId).get();
        assertEquals("BOOTSTRAPPING", loaded.getStatus());
        assertEquals("analytics", loaded.getSourceDbName());

        HmsEventLogEntity event = new HmsEventLogEntity();
        event.setId(UUID.randomUUID().toString());
        event.setHmsJobId(jobId);
        event.setEventId(1051L);
        event.setEventType("ADD_PARTITION");
        event.setTableName("events");
        event.setPartitionName("dt=2026-10-09");
        event.setSourceUri("hdfs://ns-hot:8020/warehouse/analytics/events/dt=2026-10-09");
        event.setTargetUri("hdfs://ns-hot-dc2:8020/warehouse/analytics_dr/events/dt=2026-10-09");
        event.setStatus("PENDING_DATA");
        hmsEventLogRepository.save(event);

        assertEquals(1, hmsEventLogRepository.countByHmsJobId(jobId));
        List<HmsEventLogEntity> pending = hmsEventLogRepository.findByHmsJobIdAndStatus(jobId, "PENDING_DATA");
        assertEquals(1, pending.size());
        assertEquals("ADD_PARTITION", pending.get(0).getEventType());
    }
}
