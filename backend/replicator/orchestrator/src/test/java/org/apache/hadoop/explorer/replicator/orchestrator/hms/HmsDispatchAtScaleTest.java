package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
import org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.AgentEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.AgentRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Раздача HMS-задач и учет подзадач при сотнях агентов и больших таблицах.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("HMS: раздача задач и подзадачи в масштабе")
class HmsDispatchAtScaleTest {

    @Autowired
    private HmsCoordinatorService coordinatorService;
    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private JobService jobService;
    @Autowired
    private AgentRegistry agentRegistry;
    @Autowired
    private AgentRepository agentRepository;
    @Autowired
    private ReplicatorProperties properties;

    @BeforeEach
    void setUp() {
        hmsJobRepository.deleteAll();
        jobRepository.deleteAll();
        agentRepository.deleteAll();
        agentRegistry.clear();
        register("agent-src-1", "dc1");
        register("agent-src-2", "dc1");
        register("agent-dst-1", "dc2");
    }

    private void register(String agentId, String clusterId) {
        agentRegistry.register(new AgentRegisterRequest(agentId, clusterId, "all",
                "10.0.0." + Math.abs(agentId.hashCode() % 200) + ":50051", null, 100.0), properties.getAgentSecret());
    }

    private HmsReplicationJobEntity queuedJob(String id) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId(id);
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName(id);
        job.setTargetDbName(id);
        job.setStatus("QUEUED");
        job.setCreatedBy("tester");
        return hmsJobRepository.save(job);
    }

    @Test
    @DisplayName("Квота bootstrap: первый агент не забирает все задачи кластера")
    void firstPollerDoesNotTakeEverything() {
        int quota = properties.getHmsMaxBootstrapsPerAgent();
        for (int i = 0; i < quota * 2; i++) {
            queuedJob("db_" + i);
        }

        List<HmsPendingJobDto> first = coordinatorService.getPendingJobsForCluster("dc1", "agent-src-1");
        List<HmsPendingJobDto> second = coordinatorService.getPendingJobsForCluster("dc1", "agent-src-2");

        assertEquals(quota, first.size(), "первый агент берет не больше квоты");
        assertEquals(quota, second.size(), "остаток достается второму агенту");
        assertTrue(first.stream().noneMatch(f -> second.stream().anyMatch(s -> s.id().equals(f.id()))),
                "одна задача не выдается двум агентам");
    }

    @Test
    @DisplayName("Пауза ограждает: отчет агента не возвращает задачу в ACTIVE")
    void pausedJobRejectsAgentReport() {
        HmsReplicationJobEntity job = queuedJob("paused_db");
        coordinatorService.getPendingJobsForCluster("dc1", "agent-src-1");
        job = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertTrue(job.pause());
        job.setAssignedAgentId("agent-src-1");
        hmsJobRepository.save(job);

        boolean accepted = coordinatorService.updateJobProgress(job.getId(), new HmsProgressReportRequest(
                "ACTIVE", 1, 1, 0, 0, 10L, null, 0L, "Синхронизировано", List.of(), "agent-src-1"));

        assertFalse(accepted);
        assertEquals("PAUSED", hmsJobRepository.findById(job.getId()).orElseThrow().getStatus());
    }

    @Test
    @DisplayName("Рестарт узла не сбрасывает bootstrap с живой арендой")
    void restartKeepsBootstrapWithLiveLease() {
        HmsReplicationJobEntity live = queuedJob("live_db");
        live.setStatus("BOOTSTRAPPING");
        live.setAssignedAgentId("agent-src-1");
        live.setLeaseExpiresAt(Instant.now().plusSeconds(60));
        hmsJobRepository.save(live);

        coordinatorService.recoverInterruptedJobsOnStartup();

        HmsReplicationJobEntity reloaded = hmsJobRepository.findById("live_db").orElseThrow();
        assertEquals("agent-src-1", reloaded.getAssignedAgentId(), "живую аренду трогать нельзя");
        assertEquals("BOOTSTRAPPING", reloaded.getStatus());
    }

    @Test
    @DisplayName("HA: агент, шлющий heartbeat соседнему узлу, виден и на этом")
    void peerAgentIsVisibleAfterSync() {
        AgentEntity peer = new AgentEntity();
        peer.setAgentId("agent-on-peer-node");
        peer.setClusterId("dc3");
        peer.setMode("all");
        peer.setHost("10.0.3.1");
        peer.setGrpcPort(50051);
        peer.setGrpcAddress("10.0.3.1:50051");
        peer.setLastHeartbeat(Instant.now());
        agentRepository.save(peer);
        assertTrue(agentRegistry.getLiveHmsAgentForCluster("dc3").isEmpty(), "до синхронизации узел его не знает");

        agentRegistry.syncPeersFromDatabase();

        assertTrue(agentRegistry.getLiveHmsAgentForCluster("dc3").isPresent());
    }

    @Test
    @DisplayName("Подзадачи: сводка агрегатом, страница ограничена, удаление пакетом")
    void subjobsAreAggregatedPagedAndBulkDeleted() {
        for (int i = 0; i < 1500; i++) {
            JobEntity sub = new JobEntity();
            sub.setId("sub-" + i);
            sub.setParentJobId("hms-parent");
            sub.setSourceClusterId("dc1");
            sub.setTargetClusterId("dc2");
            sub.setSourcePath("/src/" + i);
            sub.setTargetPath("/dst/" + i);
            sub.setStatus(i < 10 ? "RUNNING" : "COMPLETED");
            sub.setTotalBytes(100);
            sub.setCopiedBytes(i < 10 ? 50 : 100);
            jobRepository.save(sub);
        }

        Map<String, JobService.SubjobAggregate> agg = jobService.aggregateSubjobsByParentIds(List.of("hms-parent"));
        // Фоновые планировщики контекста могут сменить статус RUNNING у подзадач без агента,
        // поэтому ожидаемое число активных берется из БД, а не из исходных данных
        List<JobEntity> stored = jobRepository.findByParentJobId("hms-parent");
        long expectedActive = stored.stream()
                .filter(j -> "RUNNING".equalsIgnoreCase(j.getStatus()) || "QUEUED".equalsIgnoreCase(j.getStatus()))
                .count();
        assertEquals(1500, agg.get("hms-parent").count());
        assertEquals(stored.stream().mapToLong(JobEntity::getTotalBytes).sum(), agg.get("hms-parent").totalBytes());
        assertEquals(stored.stream().mapToLong(JobEntity::getCopiedBytes).sum(), agg.get("hms-parent").copiedBytes());
        assertEquals(expectedActive, agg.get("hms-parent").active());

        var page = jobService.listSubjobsByParentId("hms-parent", org.springframework.data.domain.PageRequest.of(0, 200));
        assertEquals(200, page.getContent().size());
        assertEquals(1500, page.getTotalElements());

        jobService.deleteSubjobsByParentId("hms-parent");
        assertEquals(0, jobRepository.findByParentJobId("hms-parent").size());
    }

    @Test
    @DisplayName("Вторая живая задача на ту же пару баз отклоняется 409")
    void duplicateRouteIsRejected() {
        coordinatorService.createAndStartReplication("dc1", "dc2", "sales", "sales", "*", "tester");

        var ex = assertThrows(org.springframework.web.server.ResponseStatusException.class, () ->
                coordinatorService.createAndStartReplication("dc1", "dc2", "sales", "sales", "*", "tester"));
        assertEquals(org.springframework.http.HttpStatus.CONFLICT, ex.getStatusCode());
        // В другую базу приемника — можно
        assertDoesNotThrow(() -> coordinatorService.createAndStartReplication("dc1", "dc2", "sales", "sales_copy", "*", "tester"));
    }

    @Test
    @DisplayName("Удаление задачи с идущими подзадачами: подзадачи отменяются и удаляются, задачи нет")
    void deleteStopsJobAndRemovesActiveSubjobs() {
        HmsReplicationJobEntity job = queuedJob("del_db");
        for (int i = 0; i < 50; i++) {
            JobEntity sub = new JobEntity();
            sub.setId("del-sub-" + i);
            sub.setParentJobId(job.getId());
            sub.setSourceClusterId("dc1");
            sub.setTargetClusterId("dc2");
            sub.setSourcePath("/src/" + i);
            sub.setTargetPath("/dst/" + i);
            sub.setStatus("RUNNING");
            jobRepository.save(sub);
        }

        assertTrue(coordinatorService.deleteReplicationJob(job.getId()));

        assertTrue(hmsJobRepository.findById(job.getId()).isEmpty());
        assertEquals(0, jobRepository.findByParentJobId(job.getId()).size());
    }

    @Test
    @DisplayName("Подзадачи данных принимаются только от владельца живой HMS-задачи")
    void subjobsAcceptedOnlyFromOwnerOfLiveJob() {
        HmsReplicationJobEntity job = queuedJob("owned_db");
        job.setAssignedAgentId("agent-src-1");
        job = hmsJobRepository.save(job);

        assertTrue(coordinatorService.acceptsSubjobFrom(job.getId(), "agent-src-1"));
        assertFalse(coordinatorService.acceptsSubjobFrom(job.getId(), "agent-src-2"), "чужой агент");
        assertFalse(coordinatorService.acceptsSubjobFrom("hms-job-deleted-0000", "agent-src-1"), "удаленная задача");
        HmsReplicationJobEntity stopped = hmsJobRepository.findById(job.getId()).orElseThrow();
        stopped.setStatus("STOPPED");
        hmsJobRepository.save(stopped);
        assertFalse(coordinatorService.acceptsSubjobFrom(job.getId(), "agent-src-1"), "остановленная задача");
    }

    @Test
    @DisplayName("CDC без жесткого предела: 100 задач на 10 агентов разбираются все и поровну")
    void cdcJobsAreAllAssignedAndBalanced() {
        for (int a = 3; a <= 10; a++) {
            register("agent-src-" + a, "dc1");
        }
        for (int i = 0; i < 100; i++) {
            HmsReplicationJobEntity job = queuedJob("cdc_db_" + i);
            job.setStatus("ACTIVE");
            hmsJobRepository.save(job);
        }

        // Агенты опрашивают по кругу, как в бою; первый же опрос не должен забрать все
        List<HmsPendingJobDto> firstPoll = coordinatorService.getPendingJobsForCluster("dc1", "agent-src-1");
        assertTrue(firstPoll.size() < 100, "первый агент забрал " + firstPoll.size() + " из 100");
        for (int round = 0; round < 30; round++) {
            for (int a = 1; a <= 10; a++) {
                coordinatorService.getPendingJobsForCluster("dc1", "agent-src-" + a);
            }
        }

        Map<String, Long> perAgent = new java.util.HashMap<>();
        for (HmsReplicationJobEntity job : hmsJobRepository.findAll()) {
            assertNotNull(job.getAssignedAgentId(), "задача " + job.getId() + " осталась без исполнителя");
            perAgent.merge(job.getAssignedAgentId(), 1L, Long::sum);
        }
        assertEquals(10, perAgent.size(), "задачи должны достаться всем агентам: " + perAgent);
        long max = perAgent.values().stream().mapToLong(Long::longValue).max().orElse(0);
        long min = perAgent.values().stream().mapToLong(Long::longValue).min().orElse(0);
        assertTrue(max - min <= 3, "разброс нагрузки " + perAgent);
    }

    @Test
    @DisplayName("CDC-задачи не занимают квоту bootstrap")
    void cdcJobsDoNotConsumeBootstrapQuota() {
        int quota = properties.getHmsMaxBootstrapsPerAgent();
        for (int i = 0; i < quota; i++) {
            HmsReplicationJobEntity cdc = queuedJob("busy_cdc_" + i);
            cdc.setStatus("ACTIVE");
            cdc.setAssignedAgentId("agent-src-1");
            cdc.setLeaseExpiresAt(Instant.now().plusSeconds(60));
            hmsJobRepository.save(cdc);
        }
        // Второй агент тоже занят так же, иначе выравнивание само отдаст bootstrap ему
        HmsReplicationJobEntity fresh = queuedJob("fresh_bootstrap");

        List<HmsPendingJobDto> polled = coordinatorService.getPendingJobsForCluster("dc1", "agent-src-1");

        assertTrue(polled.stream().anyMatch(p -> p.id().equals(fresh.getId())),
                "агент с " + quota + " CDC-задачами все еще может взять bootstrap");
    }

    @Test
    @DisplayName("DLQ replay: отчет пачки, начатой до переигрывания, не затирает откат позиции")
    void dlqReplayIsNotOverwrittenByInFlightBatch() {
        HmsReplicationJobEntity job = queuedJob("replay_db");
        job.setStatus("ACTIVE");
        job.setAssignedAgentId("agent-src-1");
        job.setLeaseExpiresAt(Instant.now().plusSeconds(60));
        job.setLastProcessedEventId(500L);
        hmsJobRepository.save(job);
        coordinatorService.recordDeadLetterEvent(job.getId(), 420L, "ALTER_TABLE", "t", null, null, "poison");
        String dlqId = coordinatorService.getDeadLetterEvents(job.getId(), "NEW", 10).get(0).getId();

        assertTrue(coordinatorService.replayDeadLetterEvent(job.getId(), dlqId));
        // Пачка, начатая до replay, досылает позицию 600
        boolean accepted = coordinatorService.updateJobProgress(job.getId(), new HmsProgressReportRequest(
                "ACTIVE", 1, 1, 0, 0, 600L, null, 0L, "Синхронизировано", List.of(), "agent-src-1"));

        assertFalse(accepted, "отчет устаревшей пачки должен быть отклонен");
        assertEquals(419L, hmsJobRepository.findById(job.getId()).orElseThrow().getLastProcessedEventId());
        List<HmsPendingJobDto> polled = coordinatorService.getPendingJobsForCluster("dc1", "agent-src-1");
        assertEquals(419L, polled.stream().filter(p -> p.id().equals(job.getId())).findFirst().orElseThrow()
                .lastProcessedEventId(), "агент получает задачу уже с откатанной позицией");
    }
}
