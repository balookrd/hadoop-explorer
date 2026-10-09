package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.orchestrator.ReplicatorOrchestratorApplication;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = ReplicatorOrchestratorApplication.class)
@Transactional
class DistributedTaskQueueIntegrationTest {

    @Autowired
    private TaskService taskService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private AgentRegistry agentRegistry;

    @Test
    @DisplayName("Оркестратор: полный жизненный цикл распределенного пула пофайловых задач (Batch -> Claim -> Complete -> Finish)")
    void testDistributedTaskPoolLifecycle() {
        String jobId = "job-distributed-test-" + UUID.randomUUID();

        // 1. Создаем родительское задание в очереди
        JobEntity job = new JobEntity();
        job.setId(jobId);
        job.setSourcePath("/data/warehouse/tables");
        job.setTargetPath("/backup/warehouse/tables");
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setStatus("QUEUED");
        jobRepository.saveAndFlush(job);

        // 2. Анализатор регистрирует батч из 3 файлов: 1 актуален (skipped), 2 новых (к передаче)
        TaskCreateItem item1 = new TaskCreateItem("task-1", "/data/warehouse/tables/part-1.parquet", "/backup/warehouse/tables/part-1.parquet", 1000L, true);
        TaskCreateItem item2 = new TaskCreateItem("task-2", "/data/warehouse/tables/part-2.parquet", "/backup/warehouse/tables/part-2.parquet", 2000L, false);
        TaskCreateItem item3 = new TaskCreateItem("task-3", "/data/warehouse/tables/part-3.parquet", "/backup/warehouse/tables/part-3.parquet", 3000L, false);

        BatchCreateTasksRequest batchReq = new BatchCreateTasksRequest(jobId, List.of(item1, item2, item3));
        boolean batchOk = taskService.batchCreateTasks(jobId, batchReq);
        assertTrue(batchOk);

        // Проверяем состояние задания после регистрации пула
        JobEntity afterBatch = jobRepository.findById(jobId).orElseThrow();
        assertEquals("RUNNING", afterBatch.getStatus());
        assertEquals(6000L, afterBatch.getTotalBytes());
        assertEquals(1000L, afterBatch.getCopiedBytes()); // skippedBytes учтены
        assertEquals(3, afterBatch.getTotalObjects());
        assertEquals(0, afterBatch.getTransferredObjects());
        assertEquals(1, afterBatch.getSkippedObjects());
        assertEquals(0, afterBatch.getFailedObjects());

        // 3. Воркер-1 берет в работу 1 задачу
        List<TaskItemDto> claimed1 = taskService.claimTasks(new ClaimTasksRequest("worker-node1", "dc1", 1));
        assertEquals(1, claimed1.size());
        assertEquals("worker-node1", claimed1.get(0).getAssignedAgentId());
        assertEquals("RUNNING", claimed1.get(0).getStatus());

        // 4. Воркер-2 берет следующую задачу
        List<TaskItemDto> claimed2 = taskService.claimTasks(new ClaimTasksRequest("worker-node2", "dc1", 1));
        assertEquals(1, claimed2.size());
        assertEquals("worker-node2", claimed2.get(0).getAssignedAgentId());
        assertNotEquals(claimed1.get(0).getId(), claimed2.get(0).getId(), "Воркеры должны получить разные задачи");

        // Пул QUEUED задач теперь пуст
        List<TaskItemDto> claimedEmpty = taskService.claimTasks(new ClaimTasksRequest("worker-node3", "dc1", 1));
        assertTrue(claimedEmpty.isEmpty());

        // 5. Воркер-1 завершает свою задачу
        TaskItemDto t1 = claimed1.get(0);
        boolean comp1 = taskService.completeTask(new CompleteTaskRequest(t1.getId(), "worker-node1", t1.getFileSize(), "sha256-ok-1"));
        assertTrue(comp1);

        JobEntity midJob = jobRepository.findById(jobId).orElseThrow();
        assertEquals("RUNNING", midJob.getStatus(), "Задание все еще должно быть RUNNING, так как осталась вторая задача");
        assertEquals(1000L + t1.getFileSize(), midJob.getCopiedBytes());
        assertEquals(1, midJob.getTransferredObjects());

        // 6. Воркер-2 завершает последнюю задачу
        TaskItemDto t2 = claimed2.get(0);
        boolean comp2 = taskService.completeTask(new CompleteTaskRequest(t2.getId(), "worker-node2", t2.getFileSize(), "sha256-ok-2"));
        assertTrue(comp2);

        // Все задачи завершены -> Задание перешло в COMPLETED!
        JobEntity finishedJob = jobRepository.findById(jobId).orElseThrow();
        assertEquals("COMPLETED", finishedJob.getStatus());
        assertEquals(6000L, finishedJob.getCopiedBytes());
        assertEquals(3, finishedJob.getTotalObjects());
        assertEquals(2, finishedJob.getTransferredObjects());
        assertEquals(1, finishedJob.getSkippedObjects());
        assertEquals(0, finishedJob.getFailedObjects());
        assertNotNull(finishedJob.getCompletedAt());

        // Проверяем список подзадач
        List<TaskItemDto> allTasks = taskService.getTasksByJob(jobId);
        assertEquals(3, allTasks.size());
    }

    @Test
    @DisplayName("Оркестратор: сбой отдельной подзадачи не завершает родительское задание, пока выполняются другие подзадачи")
    void testPartialFailureWaitsForAllTasks() {
        String jobId = "job-fail-test-" + UUID.randomUUID();

        JobEntity job = new JobEntity();
        job.setId(jobId);
        job.setSourcePath("/data/warehouse/logs");
        job.setTargetPath("/backup/warehouse/logs");
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setStatus("QUEUED");
        jobRepository.saveAndFlush(job);

        // Пул из двух задач на передачу
        TaskCreateItem item1 = new TaskCreateItem("fail-task-1", "/data/warehouse/logs/app1.log", "/backup/warehouse/logs/app1.log", 500L, false);
        TaskCreateItem item2 = new TaskCreateItem("fail-task-2", "/data/warehouse/logs/app2.log", "/backup/warehouse/logs/app2.log", 500L, false);

        taskService.batchCreateTasks(jobId, new BatchCreateTasksRequest(jobId, List.of(item1, item2)));

        // Воркер-1 забирает task-1, Воркер-2 забирает task-2
        List<TaskItemDto> claimed1 = taskService.claimTasks(new ClaimTasksRequest("worker-1", "dc1", 1));
        List<TaskItemDto> claimed2 = taskService.claimTasks(new ClaimTasksRequest("worker-2", "dc1", 1));
        assertEquals(1, claimed1.size());
        assertEquals(1, claimed2.size());

        // Воркер-1 падает с ошибкой gRPC connection reset
        taskService.failTask(new FailTaskRequest(claimed1.get(0).getId(), "worker-1", "gRPC stream broken"));

        // Родительская задача НЕ должна закрыться немедленно в FAILED! Она должна остаться RUNNING
        JobEntity afterOneFailed = jobRepository.findById(jobId).orElseThrow();
        assertEquals("RUNNING", afterOneFailed.getStatus(), "Задача должна оставаться RUNNING, пока task-2 в работе");
        assertEquals(1, afterOneFailed.getFailedObjects());
        assertEquals(0, afterOneFailed.getTransferredObjects());
        assertNull(afterOneFailed.getCompletedAt());

        // Воркер-2 успешно завершает свою задачу
        taskService.completeTask(new CompleteTaskRequest(claimed2.get(0).getId(), "worker-2", 500L, "sha256-ok"));

        // Теперь ВСЕ подзадачи завершены. Так как была ошибка, статус переходит в FAILED
        JobEntity fullyFinished = jobRepository.findById(jobId).orElseThrow();
        assertEquals("FAILED", fullyFinished.getStatus());
        assertEquals(2, fullyFinished.getTotalObjects());
        assertEquals(1, fullyFinished.getTransferredObjects());
        assertEquals(1, fullyFinished.getFailedObjects());
        assertNotNull(fullyFinished.getCompletedAt());
        assertTrue(fullyFinished.getMessage().contains("ошибками"));
    }
}
