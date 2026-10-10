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
}
