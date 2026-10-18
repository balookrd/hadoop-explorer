package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.model.ClaimTasksRequest;
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
import org.springframework.data.domain.Pageable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TaskServiceOptimizationTest {

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
    @DisplayName("P1: getPendingTasksCountForCluster делегирует в единый SQL/JPQL запрос countPendingTasksByClusterId")
    void testGetPendingTasksCountUsesSingleQuery() {
        when(taskRepository.countPendingTasksByClusterId("dc1", "QUEUED")).thenReturn(42L);

        long count = taskService.getPendingTasksCountForCluster("dc1");

        assertEquals(42L, count);
        verify(taskRepository, times(1)).countPendingTasksByClusterId("dc1", "QUEUED");
        // Не должен вызывать N+1 findAll() на jobRepository
        verify(jobRepository, never()).findAll();
    }

    @Test
    @DisplayName("P1: claimTasks использует Pageable выборку и кэширует JobEntity без N+1 запросов")
    void testClaimTasksUsesPaginationAndJobCache() {
        String agentId = "worker-dc1-1";
        String clusterId = "dc1";
        String jobId = "job-batch-100";

        when(agentRegistry.getAgent(agentId)).thenReturn(Optional.of(
                new AgentRegistry.AgentEntry(agentId, clusterId, "localhost", 50051, "localhost:50051", "all", null)
        ));

        // 5 задач с одним и тем же jobId
        List<TaskEntity> candidateTasks = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            TaskEntity t = new TaskEntity();
            t.setId("task-" + i);
            t.setJobId(jobId);
            t.setStatus("QUEUED");
            t.setSourcePath("/src/file-" + i);
            t.setTargetPath("/dst/file-" + i);
            t.setFileSize(1024L);
            candidateTasks.add(t);
        }

        when(taskRepository.findByStatusForUpdateSkipLocked(eq("QUEUED"), any(Pageable.class))).thenReturn(candidateTasks);

        JobEntity job = new JobEntity();
        job.setId(jobId);
        job.setStatus("RUNNING");
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        var claimed = taskService.claimTasks(new ClaimTasksRequest(agentId, clusterId, 5));

        assertEquals(5, claimed.size(), "Должно быть забрано 5 задач");
        // Выборка задач должна производиться с Pageable и блокировкой SKIP LOCKED
        verify(taskRepository, times(1)).findByStatusForUpdateSkipLocked(eq("QUEUED"), any(Pageable.class));

        // Самое главное: JobEntity должен быть запрошен ИЗ КЭША РОВНО 1 РАЗ для всех 5 задач, а не 5 раз!
        verify(jobRepository, times(1)).findById(jobId);
    }

    @Test
    @DisplayName("P2 Retention Policy: cleanOldTasks удаляет завершенные и ошибочные задачи старше retentionDays")
    void testCleanOldTasksRetentionPolicy() {
        when(taskRepository.deleteByStatusInAndUpdatedAtBefore(anyCollection(), any(java.time.Instant.class)))
                .thenReturn(150);

        int deleted = taskService.cleanOldTasks(7);

        assertEquals(150, deleted);
        verify(taskRepository, times(1)).deleteByStatusInAndUpdatedAtBefore(
                argThat(statuses -> statuses.contains("COMPLETED") && statuses.contains("FAILED")),
                any(java.time.Instant.class)
        );
    }

    @Test
    @DisplayName("200+ Agents: completeTask использует атомарный incrementProgress и быструю проверку exists без блокировки JobEntity")
    void testCompleteTaskUsesAtomicProgressIncrementAndExistsCheck() {
        String taskId = "task-opt-1";
        String jobId = "job-atomic-progress";

        TaskEntity task = new TaskEntity();
        task.setId(taskId);
        task.setJobId(jobId);
        task.setStatus("RUNNING");
        task.setFileSize(2048L);
        task.setFileCount(1);
        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));

        // Симулируем, что в очереди еще остались незавершенные задачи
        when(taskRepository.existsByJobIdAndStatusIn(eq(jobId), anyCollection())).thenReturn(true);

        boolean completed = taskService.completeTask(new org.apache.hadoop.explorer.replicator.model.CompleteTaskRequest(
                taskId, "worker-200", 2048L, "checksum-ok", 1
        ));

        assertTrue(completed);
        assertEquals("COMPLETED", task.getStatus());

        // Проверяем, что выполнен атомарный инкремент в базе данных
        verify(jobRepository, times(1)).incrementProgress(eq(jobId), eq(2048L), eq(1), any(java.time.Instant.class));

        // Проверяем, что использовался индексный LIMIT 1 exists вместо COUNT
        verify(taskRepository, times(1)).existsByJobIdAndStatusIn(eq(jobId), anyCollection());

        // И самое главное: тяжелый jobRepository.save(job) НЕ вызывается, пока задачи еще не завершились
        verify(jobRepository, never()).save(any(JobEntity.class));
    }

    @Test
    @DisplayName("200+ Agents: completeTasksBatch пакетно коммитит список задач воркеров")
    void testCompleteTasksBatch() {
        String taskId1 = "batch-task-1";
        String taskId2 = "batch-task-2";
        String jobId = "job-batch";

        TaskEntity t1 = new TaskEntity();
        t1.setId(taskId1);
        t1.setJobId(jobId);
        t1.setStatus("RUNNING");

        TaskEntity t2 = new TaskEntity();
        t2.setId(taskId2);
        t2.setJobId(jobId);
        t2.setStatus("RUNNING");

        when(taskRepository.findById(taskId1)).thenReturn(Optional.of(t1));
        when(taskRepository.findById(taskId2)).thenReturn(Optional.of(t2));
        when(taskRepository.existsByJobIdAndStatusIn(eq(jobId), anyCollection())).thenReturn(true);

        int count = taskService.completeTasksBatch(List.of(
                new org.apache.hadoop.explorer.replicator.model.CompleteTaskRequest(taskId1, "worker-1", 100L, "ok", 1),
                new org.apache.hadoop.explorer.replicator.model.CompleteTaskRequest(taskId2, "worker-1", 200L, "ok", 1)
        ));

        assertEquals(2, count);
        verify(jobRepository, times(2)).incrementProgress(eq(jobId), anyLong(), anyInt(), any(java.time.Instant.class));
    }
}
