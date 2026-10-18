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
}
