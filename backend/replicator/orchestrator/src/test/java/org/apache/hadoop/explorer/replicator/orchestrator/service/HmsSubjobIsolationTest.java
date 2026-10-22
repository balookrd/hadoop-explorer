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

    @Autowired
    private org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService coordinatorService;

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

    @Test
    @DisplayName("Проверка расчета общей скорости, стартового времени и ETA первичной синхронизации по сабтаскам")
    void testHmsBootstrapSubtasksMetricsEnrichment() {
        String parentHmsJobId = "hms-job-bootstrap-" + UUID.randomUUID();

        HmsReplicationJobEntity hmsJob = new HmsReplicationJobEntity();
        hmsJob.setId(parentHmsJobId);
        hmsJob.setSourceClusterId("dc1");
        hmsJob.setTargetClusterId("dc2");
        hmsJob.setSourceDbName("analytics");
        hmsJob.setTargetDbName("analytics_backup");
        hmsJob.setStatus("BOOTSTRAPPING");
        hmsJob.setTotalTables(3);
        hmsJob.setReplicatedTables(1);
        hmsJobRepository.save(hmsJob);

        java.time.Instant now = java.time.Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        java.time.Instant start1 = now.minusSeconds(20);
        java.time.Instant start2 = now.minusSeconds(10);

        // Сабтаска 1 (завершена, 10 МБ)
        CreateJobRequest sub1Req = new CreateJobRequest(
                "/tmp/tbl1", "/tmp/tbl1_dst", "dc1", "dc2",
                10485760L, "hdfs@REALM.LOCAL", true, false,
                null, 20, "HMS_SUBJOB", parentHmsJobId
        );
        JobResponse sub1 = jobService.createJob(sub1Req, "system");
        var ent1 = jobRepository.findById(sub1.id()).get();
        ent1.setStartedAt(start1);
        ent1.setCompletedAt(now.minusSeconds(5));
        ent1.setStatus("COMPLETED");
        ent1.setCopiedBytes(10485760L);
        jobRepository.save(ent1);

        // Сабтаска 2 (выполняется, 20 МБ, скопировано 10 МБ)
        CreateJobRequest sub2Req = new CreateJobRequest(
                "/tmp/tbl2", "/tmp/tbl2_dst", "dc1", "dc2",
                20971520L, "hdfs@REALM.LOCAL", true, false,
                null, 20, "HMS_SUBJOB", parentHmsJobId
        );
        JobResponse sub2 = jobService.createJob(sub2Req, "system");
        var ent2 = jobRepository.findById(sub2.id()).get();
        ent2.setStartedAt(start2);
        ent2.setStatus("RUNNING");
        ent2.setCopiedBytes(10485760L);
        jobRepository.save(ent2);

        // Обогащаем задачу метриками
        coordinatorService.enrichWithSubtasksMetrics(hmsJob);

        assertNotNull(hmsJob.getStartedAt(), "Стартовое время должно быть рассчитано");
        assertEquals(start1.getEpochSecond(), hmsJob.getStartedAt().getEpochSecond(), "Стартовое время должно быть минимальным временем среди сабтасок");
        assertEquals(31457280L, hmsJob.getTotalBytes(), "Общий объем байт должен суммироваться по сабтаскам");
        assertEquals(20971520L, hmsJob.getCopiedBytes(), "Скопированный объем байт должен суммироваться");
        assertEquals(2, hmsJob.getSubtasksCount());
        assertEquals(1, hmsJob.getActiveSubtasksCount());

        assertNotNull(hmsJob.getAverageSpeedMbS(), "Общая средняя скорость должна быть вычислена");
        assertTrue(hmsJob.getAverageSpeedMbS() > 0, "Скорость репликации должна быть больше 0");

        assertNotNull(hmsJob.getEstimatedCompletionAt(), "Расчетное время окончания (ETA) должно быть вычислено");
        assertTrue(hmsJob.getEstimatedCompletionAt().isAfter(now), "ETA должно быть в будущем");
    }

    @Test
    @DisplayName("Дедупликация: findReusableSubjob возвращает существующую подзадачу вместо создания дубликата")
    void testFindReusableSubjobDeduplication() {
        String parentHmsJobId = "hms-job-dedup-" + UUID.randomUUID();
        String src = "hdfs://dc1:8020/warehouse/db/tbl";
        String dst = "hdfs://dc2:8020/warehouse/db/tbl";

        // Создаем первую подзадачу
        var req = new CreateJobRequest(
                src, dst, "dc1", "dc2", 1024L, "admin", true, false, null, 7, "HMS_SUBJOB", parentHmsJobId
        );
        JobResponse created = jobService.createJob(req, "system_agent");
        assertNotNull(created);

        // Повторный запрос должен найти уже существующую подзадачу
        var found = jobService.findReusableSubjob(parentHmsJobId, src, dst);
        assertTrue(found.isPresent(), "Должна быть найдена существующая активная подзадача");
        assertEquals(created.id(), found.get().id(), "ID подзадачи должен совпадать (дедупликация)");
    }
}
