package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.ClusterLockEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.ClusterLockRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class StreamingFeatureFlagTest {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private ClusterLockRepository lockRepository;

    private ReplicatorProperties properties;
    private JobService jobService;
    private DistributedLockService lockService;

    @BeforeEach
    void setUp() {
        properties = new ReplicatorProperties();
        jobService = new JobService(jobRepository, jobRunRepository, taskRepository, properties);
        lockService = new DistributedLockService(lockRepository);
    }

    @Test
    @DisplayName("При streaming.enabled=false создание задачи со syncMode=STREAMING_INOTIFY должно отклоняться")
    void shouldRejectStreamingJobWhenDisabled() {
        properties.getStreaming().setEnabled(false);

        CreateJobRequest req = new CreateJobRequest(
            "/data/prod/events",
            "/backup/prod/events",
            "dc1",
            "dc2",
            1048576L,
            "de_user@REALM",
            true,
            false,
            null,
            20,
            "STANDARD",
            null,
            "STREAMING_INOTIFY"
        );

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            jobService.createJob(req, "de_user")
        );

        assertTrue(ex.getMessage().contains("hadoop.replicator.streaming.enabled=false"),
            "Сообщение об ошибке должно указывать на отключенный флаг конфигурации: " + ex.getMessage());
    }

    @Test
    @DisplayName("Обычные задачи должны штатно создаваться при streaming.enabled=false (гарантия отсутствия регрессии)")
    void shouldAllowRegularJobsWhenStreamingDisabled() {
        properties.getStreaming().setEnabled(false);

        CreateJobRequest manualReq = new CreateJobRequest(
            "/data/prod/sales",
            "/backup/prod/sales",
            "dc1",
            "dc2",
            2048576L,
            "de_user@REALM",
            true,
            false,
            null
        );

        JobResponse res = jobService.createJob(manualReq, "de_user");
        assertNotNull(res);
        assertEquals("QUEUED", res.status());
        assertEquals("MANUAL", res.syncMode());
    }

    @Test
    @DisplayName("При streaming.enabled=true создание задачи STREAMING_INOTIFY переводит статус в STREAMING")
    void shouldAcceptStreamingJobWhenEnabled() {
        properties.getStreaming().setEnabled(true);

        CreateJobRequest req = new CreateJobRequest(
            "/data/realtime/sensors",
            "/backup/realtime/sensors",
            "dc1",
            "dc2",
            0L,
            "de_user@REALM",
            true,
            false,
            null,
            20,
            "STANDARD",
            null,
            "STREAMING_INOTIFY"
        );

        JobResponse res = jobService.createJob(req, "de_user");
        assertNotNull(res);
        assertEquals("STREAMING", res.status());
        assertEquals("STREAMING_INOTIFY", res.syncMode());
        assertTrue(res.message().contains("активна"));
    }

    @Test
    @DisplayName("При наличии AgentRegistry создание STREAMING_INOTIFY должно отклоняться если на кластере нет streamer-агента")
    void shouldRejectStreamingJobWhenNoStreamerAgentOnCluster() {
        properties.getStreaming().setEnabled(true);
        properties.setAgentSecret("test-agent-secret-0123456789");
        var agentRegistry = new org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry(
                properties,
                new org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry(properties)
        );
        JobService registryAwareJobService = new JobService(jobRepository, jobRunRepository, taskRepository, properties, agentRegistry);

        CreateJobRequest req = new CreateJobRequest(
            "/data/events",
            "/backup/events",
            "dc1",
            "dc2",
            0L,
            "de_user@REALM",
            true,
            false,
            null,
            20,
            "STANDARD",
            null,
            "STREAMING_INOTIFY"
        );

        // 1. Без стримера - отклоняется
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            registryAwareJobService.createJob(req, "de_user")
        );
        assertTrue(ex.getMessage().contains("отсутствует зарегистрированный Inotify-агент"), ex.getMessage());

        // 2. Регистрируем обычного агента (mode=all) - всё равно отклоняется
        agentRegistry.register(
            new org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest("agent-all", "dc1", "all", "node1:50051", "node1", 100.0),
            "test-agent-secret-0123456789"
        );
        assertThrows(IllegalArgumentException.class, () -> registryAwareJobService.createJob(req, "de_user"));

        // 3. Регистрируем streamer-агента (mode=streamer) - создание успешно разрешается
        agentRegistry.register(
            new org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest("streamer-1", "dc1", "streamer", "streamer1:50051", "streamer1", 100.0),
            "test-agent-secret-0123456789"
        );
        JobResponse response = registryAwareJobService.createJob(req, "de_user");
        assertNotNull(response);
        assertEquals("STREAMING", response.status());
        assertTrue(Boolean.TRUE.equals(response.streamerOnline()), "streamerOnline должен быть true");
    }

    @Test
    @DisplayName("DistributedLockService должен атомарно захватывать и освобождать распределенную блокировку")
    void shouldAcquireAndReleaseDistributedLock() {
        String lockName = "test_scheduler_lock";

        boolean locked = lockService.tryLock(lockName, Duration.ofSeconds(10));
        assertTrue(locked, "Первый инстанс должен успешно захватить блокировку");

        // Попытка захвата другим симулированным инстансом
        DistributedLockService secondNode = new DistributedLockService(lockRepository);
        boolean secondLocked = secondNode.tryLock(lockName, Duration.ofSeconds(10));
        assertFalse(secondLocked, "Второй инстанс не должен получить удерживаемую блокировку");

        // Освобождение
        lockService.releaseLock(lockName);

        // Теперь второй инстанс может захватить
        boolean acquiredAfterRelease = secondNode.tryLock(lockName, Duration.ofSeconds(10));
        assertTrue(acquiredAfterRelease, "После освобождения второй инстанс должен захватить блокировку");
    }

    @Test
    @DisplayName("P4: runWithLock должен выполнять действие и корректно освобождать блокировку с heartbeat renewal")
    void shouldExecuteRunWithLockAndRelease() {
        String lockName = "test_run_with_lock";
        java.util.concurrent.atomic.AtomicBoolean actionExecuted = new java.util.concurrent.atomic.AtomicBoolean(false);

        lockService.runWithLock(lockName, Duration.ofSeconds(2), () -> {
            actionExecuted.set(true);
            // Во время выполнения action блокировка удерживается
            DistributedLockService secondNode = new DistributedLockService(lockRepository);
            assertFalse(secondNode.tryLock(lockName, Duration.ofSeconds(2)));
        });

        assertTrue(actionExecuted.get());

        // После завершения runWithLock блокировка должна быть освобождена
        DistributedLockService secondNode = new DistributedLockService(lockRepository);
        assertTrue(secondNode.tryLock(lockName, Duration.ofSeconds(2)));
        secondNode.releaseLock(lockName);
    }
}
