package org.apache.hadoop.explorer.replicator.orchestrator.chaos;

import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.orchestrator.ReplicatorOrchestratorApplication;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.AgentRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.TaskService;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = ReplicatorOrchestratorApplication.class)
@Transactional
@DisplayName("Orchestrator Chaos & Resilience Tests: Worker Death, Failover Cascades and Token Storms")
public class OrchestratorChaosResilienceTest {

    @Autowired
    private TaskService taskService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private AgentRegistry agentRegistry;

    @Autowired
    private AgentRepository agentRepository;

    @Autowired
    private ReplicatorProperties properties;

    private String validSecret;

    @BeforeEach
    void setUp() {
        validSecret = properties.getAgentSecret();
        if (validSecret == null || validSecret.isBlank()) {
            validSecret = "test-agent-secret-key-12345";
            properties.setAgentSecret(validSecret);
        }
        properties.setAgentHeartbeatTimeoutSeconds(2);
        properties.setAgentOfflineTimeoutSeconds(5);
    }

    @Test
    @DisplayName("Chaos: Внезапная смерть воркера во время передачи приводит к авто-эвакуации задачи на резервный узел")
    void testSuddenWorkerDeathTriggersOrphanTaskFailover() {
        String cluster = "dc-chaos";
        String workerA = "worker-chaos-A";
        String workerB = "worker-chaos-B";

        // Регистрируем двух агентов в одном кластере
        agentRegistry.register(new AgentRegisterRequest(workerA, cluster, "all", "10.0.0.1:50051", "host-a", 100.0), validSecret);
        agentRegistry.register(new AgentRegisterRequest(workerB, cluster, "all", "10.0.0.2:50051", "host-b", 100.0), validSecret);

        // Создаем задание
        String jobId = "job-chaos-" + UUID.randomUUID();
        JobEntity job = new JobEntity();
        job.setId(jobId);
        job.setSourceClusterId(cluster);
        job.setTargetClusterId(cluster);
        job.setSourcePath("/data/source");
        job.setTargetPath("/data/target");
        job.setStatus("RUNNING");
        job.setSyncMode("BATCH");
        job.setTotalObjects(1);
        job.setTotalBytes(1024L);
        jobRepository.saveAndFlush(job);

        // Создаем задачу
        TaskCreateItem taskItem = new TaskCreateItem();
        taskItem.setId("task-failover-1");
        taskItem.setSourcePath("/data/source/important.parquet");
        taskItem.setTargetPath("/data/target/important.parquet");
        taskItem.setFileSize(1024L);
        taskItem.setTaskType("FILE");

        BatchCreateTasksRequest batchReq = new BatchCreateTasksRequest();
        batchReq.setTasks(List.of(taskItem));
        taskService.batchCreateTasks(jobId, batchReq);

        // Воркер А забирает задачу
        ClaimTasksRequest claimReqA = new ClaimTasksRequest(workerA, cluster, 5);
        List<TaskItemDto> claimedA = taskService.claimTasks(claimReqA);
        assertEquals(1, claimedA.size(), "Воркер А должен успешно забрать задачу");
        assertEquals("task-failover-1", claimedA.get(0).getId());

        TaskEntity runningTask = taskRepository.findById("task-failover-1").orElseThrow();
        assertEquals("RUNNING", runningTask.getStatus());
        assertEquals(workerA, runningTask.getAssignedAgentId());

        // ХАОС: Воркер А внезапно умирает (не шлет heartbeat более 10 сек -> переходит в OFFLINE)
        assertTrue(agentRegistry.getAgent(workerA).isPresent());
        simulateAgentDeath(workerA, 100);

        // Срабатывает механизм обнаружения зависших/осиротевших задач
        taskService.executeCheckAndFailoverOrphanedTasks();

        // Проверяем, что задача была возвращена в QUEUED с фиксацией сбойного агента
        TaskEntity recoveredTask = taskRepository.findById("task-failover-1").orElseThrow();
        assertEquals("QUEUED", recoveredTask.getStatus(), "Задача должна быть возвращена в статус QUEUED");
        assertNull(recoveredTask.getAssignedAgentId(), "Назначенный агент должен быть сброшен");
        assertEquals(1, recoveredTask.getRetryCount(), "Счетчик повторов должен быть инкрементирован");
        assertEquals(workerA, recoveredTask.getLastFailedAgentId(), "Сбойный воркер должен быть зафиксирован");

        // Если сбойный воркер А попытается снова опросить очередь, задача НЕ должна ему выдаваться при наличии живого воркера B
        List<TaskItemDto> rejectedForA = taskService.claimTasks(claimReqA);
        assertTrue(rejectedForA.isEmpty(), "Сбойный воркер А не должен получать задачу повторно при наличии альтернативы");

        // Живой воркер B опрашивает очередь и успешно подхватывает задачу
        ClaimTasksRequest claimReqB = new ClaimTasksRequest(workerB, cluster, 5);
        List<TaskItemDto> claimedB = taskService.claimTasks(claimReqB);
        assertEquals(1, claimedB.size(), "Резервный воркер B должен перехватить эвакуированную задачу");
        assertEquals("task-failover-1", claimedB.get(0).getId());

        // Воркер B успешно завершает задачу
        CompleteTaskRequest compReq = new CompleteTaskRequest();
        compReq.setTaskId("task-failover-1");
        compReq.setBytesTransferred(1024L);
        compReq.setFilesCount(1);
        boolean completed = taskService.completeTask(compReq);
        assertTrue(completed);

        TaskEntity finalTask = taskRepository.findById("task-failover-1").orElseThrow();
        assertEquals("COMPLETED", finalTask.getStatus());

        JobEntity finalJob = jobRepository.findById(jobId).orElseThrow();
        assertEquals("COMPLETED", finalJob.getStatus(), "Задание должно перейти в статус COMPLETED");
    }

    @Test
    @DisplayName("Chaos: Исчерпание лимита повторов (Circuit Breaker) корректно переводит задачу и задание в FAILED")
    void testMaxRetriesExceededTransitionsJobToFailed() {
        String cluster = "dc-retry";
        String worker = "worker-retry-1";
        agentRegistry.register(new AgentRegisterRequest(worker, cluster, "all", "10.0.0.10:50051", "host-r", 100.0), validSecret);

        String jobId = "job-retry-" + UUID.randomUUID();
        JobEntity job = new JobEntity();
        job.setId(jobId);
        job.setSourcePath("/src/file.bin");
        job.setTargetPath("/tgt/file.bin");
        job.setSourceClusterId(cluster);
        job.setTargetClusterId(cluster);
        job.setStatus("RUNNING");
        job.setSyncMode("BATCH");
        job.setTotalObjects(1);
        job.setTotalBytes(500L);
        jobRepository.saveAndFlush(job);

        TaskCreateItem item = new TaskCreateItem();
        item.setId("task-fatal-retry");
        item.setSourcePath("/src/file.bin");
        item.setTargetPath("/tgt/file.bin");
        item.setFileSize(500L);
        item.setTaskType("FILE");

        BatchCreateTasksRequest batchReq = new BatchCreateTasksRequest();
        batchReq.setTasks(List.of(item));
        taskService.batchCreateTasks(jobId, batchReq);

        // По умолчанию maxRetries = 3. Допустимо 3 повторных попытки (retryCount = 1, 2, 3)
        for (int i = 1; i <= 3; i++) {
            boolean failed = taskService.failTask(new FailTaskRequest("task-fatal-retry", worker, "Network timeout attempt " + i));
            assertTrue(failed);
            TaskEntity task = taskRepository.findById("task-fatal-retry").orElseThrow();
            assertEquals("QUEUED", task.getStatus(), "При попытках <= maxRetries задача должна возвращаться в QUEUED");
            assertEquals(i, task.getRetryCount());
        }

        // 4-я ошибка: лимит повторов (3) исчерпан -> перевод в FAILED
        boolean fatal = taskService.failTask(new FailTaskRequest("task-fatal-retry", worker, "Fatal permanent network partition"));
        assertTrue(fatal);
        TaskEntity task = taskRepository.findById("task-fatal-retry").orElseThrow();
        assertEquals("FAILED", task.getStatus(), "При исчерпании maxRetries задача должна перейти в FAILED");

        JobEntity finalJob = jobRepository.findById(jobId).orElseThrow();
        assertEquals("FAILED", finalJob.getStatus(), "Задание должно быть переведено в FAILED");
        assertEquals(1, finalJob.getFailedObjects());
    }

    @Test
    @DisplayName("Stress: Высококонкурентный шторм запросов токенов (SingleBucket calculateWaitAndConsume)")
    void testConcurrentTokenBucketThrottlerStorm() throws InterruptedException {
        TokenBucketThrottler.SingleBucket bucket = new TokenBucketThrottler.SingleBucket("stress-bucket", 1_000_000.0, 1.0);

        int threads = 20;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger successfulGrants = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        double waitTime = bucket.calculateWaitAndConsume(10_000, System.nanoTime());
                        if (waitTime >= 0.0) {
                            successfulGrants.incrementAndGet();
                        }
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS), "Все потоки должны завершить конкурентные запросы токенов");
        executor.shutdownNow();

        assertEquals(threads * operationsPerThread, successfulGrants.get(), "Все конкурентные запросы должны быть обработаны");
    }

    @Test
    @DisplayName("Stress: Параллельный шторм регистраций и heartbeat агентов (AgentRegistry Concurrency)")
    void testConcurrentAgentHeartbeatStorm() throws InterruptedException {
        int agentCount = 30;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(agentCount);

        for (int i = 0; i < agentCount; i++) {
            final int id = i;
            executor.submit(() -> {
                try {
                    String agentId = "agent-storm-" + id;
                    agentRegistry.register(new AgentRegisterRequest(agentId, "dc-storm", "all", "127.0.0.1:" + (50000 + id), "host-" + id, 100.0), validSecret);
                    AgentHeartbeatRequest hb = new AgentHeartbeatRequest();
                    hb.setAgentId(agentId);
                    hb.setClusterId("dc-storm");
                    hb.setActiveTransfers(id % 4);
                    hb.setTransferredBytes(id * 1024L);
                    hb.setChunkLatencyAvgMs(12.5);
                    agentRegistry.heartbeat(hb, validSecret);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Все потоки регистрации и heartbeat должны выполниться без гонок");
        executor.shutdownNow();

        assertEquals(agentCount, agentRegistry.getAgents().stream().filter(a -> "dc-storm".equals(a.getClusterId())).count());
    }

    /**
     * Агент действительно умер: heartbeat устарел и в памяти узла, и в БД.
     * Решение о живости принимается по БД, поэтому старения одной локальной записи
     * недостаточно — иначе failover судил бы агента по данным одного узла.
     */
    private void simulateAgentDeath(String agentId, long secondsAgo) {
        Instant stale = Instant.now().minusSeconds(secondsAgo);
        agentRegistry.getAgent(agentId).ifPresent(a -> a.setLastHeartbeat(stale));
        agentRepository.findById(agentId).ifPresent(entity -> {
            entity.setLastHeartbeat(stale);
            agentRepository.save(entity);
        });
    }
}
