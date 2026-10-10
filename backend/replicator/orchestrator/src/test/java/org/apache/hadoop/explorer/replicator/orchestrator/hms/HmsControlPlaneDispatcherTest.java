package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.model.HmsEventReportDto;
import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
import org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
public class HmsControlPlaneDispatcherTest {

    @Autowired
    private HmsCoordinatorService coordinatorService;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @Autowired
    private HmsEventLogRepository eventLogRepository;

    @Autowired
    private AgentRegistry agentRegistry;

    @Autowired
    private org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties properties;

    @BeforeEach
    void setup() {
        hmsJobRepository.deleteAll();
        eventLogRepository.deleteAll();
        agentRegistry.clear();
    }

    @Test
    @DisplayName("При отсутствии агентов задача переходит в статус WAITING_FOR_AGENTS без попыток локального исполнения")
    void testWaitingForAgentsWhenNoAgentsRegistered() {
        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "cluster-alpha",
                "cluster-beta",
                "finance_db",
                "finance_replica",
                "*",
                "operator_1"
        );

        assertNotNull(job.getId());
        assertEquals("WAITING_FOR_AGENTS", job.getStatus());
        assertTrue(job.getMessage().contains("Ожидание подключения агентов"));

        // Опрос задач для кластера не возвращает задачи без доступного целевого агента
        List<HmsPendingJobDto> pending = coordinatorService.getPendingJobsForCluster("cluster-alpha");
        assertTrue(pending.isEmpty());
    }

    @Test
    @DisplayName("Диспетчеризация задачи паре агентов, выдача задания и фиксация прогресса")
    void testDispatchJobWhenAgentsOnlineAndReportProgress() {
        // 1. Регистрация агентов для DC1 и DC2
        agentRegistry.register(new AgentRegisterRequest(
                "agent-src-01", "dc1", "hms", "10.0.1.10:50051", null, 100.0
        ), properties.getAgentSecret());

        agentRegistry.register(new AgentRegisterRequest(
                "agent-dst-01", "dc2", "hms", "10.0.2.20:50051", null, 100.0
        ), properties.getAgentSecret());

        // 2. Создание задачи репликации
        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "dc1",
                "dc2",
                "telemetry",
                "telemetry_replica",
                "events_*",
                "admin",
                true,
                true
        );

        assertEquals("QUEUED", job.getStatus(), "При наличии пары агентов статус должен стать QUEUED");
        assertTrue(job.getMessage().contains("agent-src-01"));
        assertTrue(job.getMessage().contains("10.0.2.20:50051"));

        // 3. Source Agent опрашивает очередь и получает задание с целевым gRPC адресом
        List<HmsPendingJobDto> pending = coordinatorService.getPendingJobsForCluster("dc1");
        assertFalse(pending.isEmpty());
        HmsPendingJobDto task = pending.stream().filter(p -> p.id().equals(job.getId())).findFirst().orElseThrow();
        assertEquals("10.0.2.20:50051", task.targetAgentGrpcAddress());
        assertTrue(task.dropExtraneousTables());
        assertTrue(task.dropExtraneousPartitions());

        // 4. Source Agent шлет отчет о завершении Bootstrap
        HmsEventReportDto dropTableEvent = new HmsEventReportDto(
                100L, "BOOTSTRAP_DROP_EXTRA_TABLE", "old_temp_table", null,
                null, null, null, "DROPPED", "Лишняя таблица удалена при согласовании"
        );
        HmsEventReportDto tableEvent = new HmsEventReportDto(
                100L, "BOOTSTRAP_TABLE", "events_2026", null,
                "hdfs://ns-hot/warehouse/events_2026", "hdfs://ns-target/warehouse/events_2026",
                "subjob-101", "APPLIED", null
        );

        HmsProgressReportRequest report = new HmsProgressReportRequest(
                "ACTIVE",
                5,
                5,
                200,
                200,
                100L,
                100L,
                0L,
                "Bootstrap успешно завершен",
                List.of(dropTableEvent, tableEvent)
        );

        coordinatorService.updateJobProgress(job.getId(), report);

        // 5. Проверка сохранения состояния в БД Control Plane
        HmsReplicationJobEntity updated = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertEquals("ACTIVE", updated.getStatus());
        assertEquals(5, updated.getReplicatedTables());
        assertEquals(200, updated.getReplicatedPartitions());
        assertEquals(100L, updated.getLastProcessedEventId());

        List<HmsEventLogEntity> events = eventLogRepository.findByHmsJobIdOrderByCreatedAtDesc(job.getId(), null);
        assertEquals(2, events.size());
        assertTrue(events.stream().anyMatch(e -> "BOOTSTRAP_DROP_EXTRA_TABLE".equals(e.getEventType())));
        assertTrue(events.stream().anyMatch(e -> "BOOTSTRAP_TABLE".equals(e.getEventType())));
    }

    @Test
    @DisplayName("Автоматический перевод из WAITING_FOR_AGENTS в QUEUED при появлении агентов")
    void testAutoTransitionWhenAgentsAppear() {
        // Задача создана, когда агентов не было
        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "cloud-dc1",
                "cloud-dc2",
                "logs_db",
                "logs_db",
                "*",
                "user_app"
        );
        assertEquals("WAITING_FOR_AGENTS", job.getStatus());

        // Регистрируем агентов
        agentRegistry.register(new AgentRegisterRequest(
                "agent-c1", "cloud-dc1", "all", "192.168.1.1:50051", null, null
        ), properties.getAgentSecret());
        agentRegistry.register(new AgentRegisterRequest(
                "agent-c2", "cloud-dc2", "all", "192.168.2.2:50051", null, null
        ), properties.getAgentSecret());

        // Планировщик проверяет ожидающие задачи
        coordinatorService.checkWaitingJobs();

        HmsReplicationJobEntity refreshed = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertEquals("QUEUED", refreshed.getStatus());
        assertTrue(refreshed.getMessage().contains("192.168.2.2:50051"));
    }

    @Test
    @DisplayName("Эксклюзивный Distributed Lease: задача захватывается одним агентом, второй агент получает пустой список")
    void testDistributedLeaseExclusiveClaim() {
        // Регистрируем два агента источника в dc1 и один в dc2
        agentRegistry.register(new AgentRegisterRequest(
                "agent-src-alpha", "dc1", "all", "10.0.1.1:50051", null, 100.0
        ), properties.getAgentSecret());
        agentRegistry.register(new AgentRegisterRequest(
                "agent-src-beta", "dc1", "all", "10.0.1.2:50051", null, 100.0
        ), properties.getAgentSecret());
        agentRegistry.register(new AgentRegisterRequest(
                "agent-dst-primary", "dc2", "all", "10.0.2.1:50051", null, 100.0
        ), properties.getAgentSecret());

        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "dc1", "dc2", "crm_db", "crm_db", "*", "operator"
        );
        assertEquals("QUEUED", job.getStatus());

        // 1. Первый агент (alpha) запрашивает задачи со своим agentId -> успешно захватывает lease
        List<HmsPendingJobDto> alphaJobs = coordinatorService.getPendingJobsForCluster("dc1", "agent-src-alpha");
        assertEquals(1, alphaJobs.size());
        assertEquals(job.getId(), alphaJobs.get(0).id());
        assertEquals("agent-src-alpha", alphaJobs.get(0).assignedAgentId());

        HmsReplicationJobEntity claimedJob = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertEquals("agent-src-alpha", claimedJob.getAssignedAgentId());
        assertNotNull(claimedJob.getLeaseExpiresAt());

        // 2. Второй агент (beta) запрашивает задачи со своим agentId -> задача заблокирована
        List<HmsPendingJobDto> betaJobs = coordinatorService.getPendingJobsForCluster("dc1", "agent-src-beta");
        assertTrue(betaJobs.isEmpty(), "Второй агент не должен получить задачу, пока удерживается lease");

        // 3. Первый агент шлет прогресс -> lease продлевается
        var initialLease = claimedJob.getLeaseExpiresAt();
        coordinatorService.updateJobProgress(job.getId(), new HmsProgressReportRequest(
                "ACTIVE", 10, 10, 50, 50, 42L, 42L, 0L,
                "CDC в процессе", List.of(), "agent-src-alpha"
        ));

        HmsReplicationJobEntity renewedJob = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertEquals("agent-src-alpha", renewedJob.getAssignedAgentId());
        assertEquals(42L, renewedJob.getLastProcessedEventId());
        assertTrue(!renewedJob.getLeaseExpiresAt().isBefore(initialLease));
    }

    @Test
    @DisplayName("Авто-failover: при падении удерживающего агента задача перехватывается другим агентом пула с сохранением CDC смещения")
    void testAutoFailoverWhenAssignedAgentGoesOffline() {
        agentRegistry.register(new AgentRegisterRequest(
                "agent-failover-1", "dc1", "all", "10.0.1.1:50051", null, 100.0
        ), properties.getAgentSecret());
        agentRegistry.register(new AgentRegisterRequest(
                "agent-failover-2", "dc1", "all", "10.0.1.2:50051", null, 100.0
        ), properties.getAgentSecret());
        agentRegistry.register(new AgentRegisterRequest(
                "agent-dst-main", "dc2", "all", "10.0.2.1:50051", null, 100.0
        ), properties.getAgentSecret());

        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "dc1", "dc2", "orders_db", "orders_db", "*", "operator"
        );

        // Агент 1 забирает задачу и начинает стримить CDC до события 1500
        coordinatorService.getPendingJobsForCluster("dc1", "agent-failover-1");
        coordinatorService.updateJobProgress(job.getId(), new HmsProgressReportRequest(
                "ACTIVE", 5, 5, 100, 100, 1500L, 1000L, 0L,
                "CDC поток активен", List.of(), "agent-failover-1"
        ));

        // Эмулируем падение агента 1: сдвигаем heartbeat далеко в прошлое
        var agent1Entry = agentRegistry.getAgent("agent-failover-1").orElseThrow();
        agent1Entry.setLastHeartbeat(Instant.now().minusSeconds(100));

        // Шедулер авто-failover обнаруживает брошенную задачу
        coordinatorService.checkAndFailoverOrphanedHmsJobs();

        HmsReplicationJobEntity afterFailover = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertNull(afterFailover.getAssignedAgentId(), "Назначение должно быть сброшено для авто-failover");
        assertEquals("agent-failover-1", afterFailover.getLastFailedAgentId());
        assertEquals(1500L, afterFailover.getLastProcessedEventId(), "CDC offset должен сохраниться");

        // Агент 2 запрашивает задачи -> мгновенно перехватывает задачу и продолжает с 1500L
        List<HmsPendingJobDto> agent2Jobs = coordinatorService.getPendingJobsForCluster("dc1", "agent-failover-2");
        assertEquals(1, agent2Jobs.size());
        assertEquals(job.getId(), agent2Jobs.get(0).id());
        assertEquals("agent-failover-2", agent2Jobs.get(0).assignedAgentId());
        assertEquals(1500L, agent2Jobs.get(0).lastProcessedEventId());
    }

    @Test
    @DisplayName("Переключение Target-агента: при сбое приемника Source-агент получает адрес резервного приемника")
    void testTargetAgentFailoverSwitch() {
        agentRegistry.register(new AgentRegisterRequest(
                "agent-src-node", "dc1", "all", "10.0.1.1:50051", null, 100.0
        ), properties.getAgentSecret());
        agentRegistry.register(new AgentRegisterRequest(
                "agent-dst-nodeA", "dc2", "all", "10.0.2.10:50051", null, 100.0
        ), properties.getAgentSecret());

        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "dc1", "dc2", "logs_db", "logs_db", "*", "operator"
        );

        List<HmsPendingJobDto> initial = coordinatorService.getPendingJobsForCluster("dc1", "agent-src-node");
        assertEquals("10.0.2.10:50051", initial.get(0).targetAgentGrpcAddress());

        // Регистрируем второй целевой агент и переводим nodeA в OFFLINE
        agentRegistry.register(new AgentRegisterRequest(
                "agent-dst-nodeB", "dc2", "all", "10.0.2.20:50051", null, 100.0
        ), properties.getAgentSecret());
        agentRegistry.getAgent("agent-dst-nodeA").orElseThrow().setLastHeartbeat(Instant.now().minusSeconds(100));

        // При очередном цикле опроса Source-агент получает адрес резервного приемника (nodeB)
        List<HmsPendingJobDto> switched = coordinatorService.getPendingJobsForCluster("dc1", "agent-src-node");
        assertEquals("10.0.2.20:50051", switched.get(0).targetAgentGrpcAddress());
    }
}
