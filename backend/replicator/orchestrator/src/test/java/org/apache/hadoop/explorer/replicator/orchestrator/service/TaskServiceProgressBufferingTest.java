package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.model.CompleteTaskRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Тесты для проверки debounced / in-memory агрегации прогресса задач (PERF-06).
 */
class TaskServiceProgressBufferingTest {

    private TaskRepository taskRepository;
    private JobRepository jobRepository;
    private JobRunRepository jobRunRepository;
    private AgentRegistry agentRegistry;
    private TopologyRegistry topologyRegistry;
    private ReplicatorProperties properties;

    private TaskService taskService;

    @BeforeEach
    void setUp() {
        taskRepository = mock(TaskRepository.class);
        jobRepository = mock(JobRepository.class);
        jobRunRepository = mock(JobRunRepository.class);
        agentRegistry = mock(AgentRegistry.class);
        topologyRegistry = mock(TopologyRegistry.class);
        properties = new ReplicatorProperties();

        taskService = new TaskService(
                taskRepository,
                jobRepository,
                jobRunRepository,
                agentRegistry,
                topologyRegistry,
                properties
        );
    }

    @Test
    @DisplayName("JobProgressBuffer: потокобезопасная агрегация под высокой конкуренцией без блокировок")
    void testJobProgressBufferConcurrency() throws InterruptedException {
        JobProgressBuffer buffer = new JobProgressBuffer();
        String jobId = "job-concurrent-test";
        int threads = 16;
        int iterationsPerThread = 10_000;
        long bytesPerOp = 1024L;
        int filesPerOp = 1;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < iterationsPerThread; i++) {
                        buffer.record(jobId, bytesPerOp, filesPerOp);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertTrue(buffer.hasPendingProgress(jobId));

        Map<String, long[]> drained = buffer.drain();
        assertNotNull(drained.get(jobId));
        assertEquals(threads * iterationsPerThread * bytesPerOp, drained.get(jobId)[0]);
        assertEquals(threads * iterationsPerThread * filesPerOp, drained.get(jobId)[1]);

        // После drain буфер должен быть пуст
        assertFalse(buffer.hasPendingProgress(jobId));
        assertTrue(buffer.drain().isEmpty());
    }

    @Test
    @DisplayName("PERF-06: completeTask буферизует инкремент и не обращается к СУБД до flushPendingProgress")
    void testCompleteTaskBufferingAndFlush() {
        String jobId = "job-buffering-1";
        String taskId1 = "task-buf-1";
        String taskId2 = "task-buf-2";

        TaskEntity t1 = new TaskEntity();
        t1.setId(taskId1);
        t1.setJobId(jobId);
        t1.setStatus("RUNNING");
        t1.setFileSize(5000L);

        TaskEntity t2 = new TaskEntity();
        t2.setId(taskId2);
        t2.setJobId(jobId);
        t2.setStatus("RUNNING");
        t2.setFileSize(7000L);

        when(taskRepository.findById(taskId1)).thenReturn(Optional.of(t1));
        when(taskRepository.findById(taskId2)).thenReturn(Optional.of(t2));
        when(taskRepository.existsByJobIdAndStatusIn(eq(jobId), anyCollection())).thenReturn(true);

        // Завершаем две задачи
        taskService.completeTask(new CompleteTaskRequest(taskId1, "worker-1", 5000L, "chk1", 1));
        taskService.completeTask(new CompleteTaskRequest(taskId2, "worker-2", 7000L, "chk2", 1));

        // Прогресс в памяти
        assertTrue(taskService.getProgressBuffer().hasPendingProgress(jobId));

        // В БД ни одного вызова incrementProgress (исключен Row Contention)
        verify(jobRepository, never()).incrementProgress(any(), anyLong(), anyInt(), any());

        // Запускаем периодический сброс
        taskService.flushPendingProgress();

        // Ровно 1 вызов с агрегированной дельтой 12000L и 2 файла!
        verify(jobRepository, times(1)).incrementProgress(eq(jobId), eq(12000L), eq(2), any(Instant.class));

        // Повторный flush ничего не шлет
        taskService.flushPendingProgress();
        verify(jobRepository, times(1)).incrementProgress(any(), anyLong(), anyInt(), any());
    }

    @Test
    @DisplayName("PERF-06: finalizeJobCompletion принудительно сбрасывает остаток буфера перед финализацией")
    void testFinalizeJobCompletionFlushesBufferImmediately() {
        String jobId = "job-finalize-flush";
        String taskId = "task-last-one";

        TaskEntity task = new TaskEntity();
        task.setId(taskId);
        task.setJobId(jobId);
        task.setStatus("RUNNING");
        task.setFileSize(1500L);

        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));

        JobEntity job = new JobEntity();
        job.setId(jobId);
        job.setStatus("RUNNING");
        job.setTotalObjects(1);
        job.setTransferredObjects(0);
        job.setCopiedBytes(0L);
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        // Последняя задача: в очереди больше нет QUEUED или RUNNING
        when(taskRepository.existsByJobIdAndStatusIn(eq(jobId), anyCollection())).thenReturn(false);
        when(taskRepository.countByJobIdAndStatus(eq(jobId), eq("FAILED"))).thenReturn(0L);

        boolean completed = taskService.completeTask(new CompleteTaskRequest(taskId, "worker-1", 1500L, "ok", 1));
        assertTrue(completed);

        // Перед проверкой JobEntity был немедленно вызван incrementProgress
        verify(jobRepository, times(1)).incrementProgress(eq(jobId), eq(1500L), eq(1), any(Instant.class));

        // Буфер для задания пуст
        assertFalse(taskService.getProgressBuffer().hasPendingProgress(jobId));

        // Задание переведено в COMPLETED и сохранено
        assertEquals("COMPLETED", job.getStatus());
        verify(jobRepository, times(1)).save(job);
    }

    @Test
    @DisplayName("PERF-06: completeTasksBatch пакетно обрабатывает список задач и аккумулирует прогресс")
    void testCompleteTasksBatchOptimization() {
        String jobId1 = "job-batch-a";
        String jobId2 = "job-batch-b";

        TaskEntity t1 = new TaskEntity();
        t1.setId("task-a1");
        t1.setJobId(jobId1);
        t1.setStatus("RUNNING");
        t1.setFileSize(100L);

        TaskEntity t2 = new TaskEntity();
        t2.setId("task-a2");
        t2.setJobId(jobId1);
        t2.setStatus("RUNNING");
        t2.setFileSize(200L);

        TaskEntity t3 = new TaskEntity();
        t3.setId("task-b1");
        t3.setJobId(jobId2);
        t3.setStatus("RUNNING");
        t3.setFileSize(300L);

        when(taskRepository.findAllById(anyList())).thenReturn(List.of(t1, t2, t3));
        when(taskRepository.existsByJobIdAndStatusIn(anyString(), anyCollection())).thenReturn(true);

        List<CompleteTaskRequest> requests = List.of(
                new CompleteTaskRequest("task-a1", "worker-1", 100L, "ok1", 1),
                new CompleteTaskRequest("task-a2", "worker-1", 200L, "ok2", 1),
                new CompleteTaskRequest("task-b1", "worker-1", 300L, "ok3", 1)
        );

        int count = taskService.completeTasksBatch(requests);
        assertEquals(3, count);

        // Проверяем batch-сохранение задач
        verify(taskRepository, times(1)).saveAll(anyList());

        // Буфер содержит прогресс для обоих заданий
        assertTrue(taskService.getProgressBuffer().hasPendingProgress(jobId1));
        assertTrue(taskService.getProgressBuffer().hasPendingProgress(jobId2));

        // Сбрасываем дельты
        taskService.flushPendingProgress();

        // По одному атомарному инкременту на каждое затронутое задание
        verify(jobRepository, times(1)).incrementProgress(eq(jobId1), eq(300L), eq(2), any(Instant.class));
        verify(jobRepository, times(1)).incrementProgress(eq(jobId2), eq(300L), eq(1), any(Instant.class));
    }

    @Test
    @DisplayName("PERF-06: shutdown (@PreDestroy) сбрасывает все накопленные дельты")
    void testShutdownFlushesProgress() {
        String jobId = "job-shutdown";
        taskService.getProgressBuffer().record(jobId, 9999L, 4);

        assertTrue(taskService.getProgressBuffer().hasPendingProgress(jobId));

        taskService.shutdown();

        verify(jobRepository, times(1)).incrementProgress(eq(jobId), eq(9999L), eq(4), any(Instant.class));
        assertFalse(taskService.getProgressBuffer().hasPendingProgress(jobId));
    }

    @Test
    @DisplayName("Self-Healing: flushPendingProgress автоматически финализирует задание с завершенными задачами")
    void testFlushPendingProgressFinalizesCompletedJob() {
        String jobId = "job-self-heal";
        taskService.getProgressBuffer().record(jobId, 500L, 2);

        JobEntity job = new JobEntity();
        job.setId(jobId);
        job.setStatus("RUNNING");
        job.setTotalObjects(2);
        job.setTransferredObjects(2);
        job.setCopiedBytes(500L);
        job.setTotalBytes(500L);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        when(taskRepository.existsByJobIdAndStatusIn(eq(jobId), anyCollection())).thenReturn(false);
        when(taskRepository.countByJobIdAndStatus(eq(jobId), eq("FAILED"))).thenReturn(0L);

        taskService.flushPendingProgress();

        verify(jobRepository, times(1)).incrementProgress(eq(jobId), eq(500L), eq(2), any(Instant.class));
        assertEquals("COMPLETED", job.getStatus());
        assertNotNull(job.getCompletedAt());
        verify(jobRepository, atLeastOnce()).save(job);
    }
}
