package io.github.balookrd.pristan.parom.orchestrator.service;

import io.github.balookrd.pristan.parom.model.CompleteTaskRequest;
import io.github.balookrd.pristan.parom.model.FailTaskRequest;
import io.github.balookrd.pristan.parom.orchestrator.config.ParomProperties;
import io.github.balookrd.pristan.parom.orchestrator.entity.TaskEntity;
import io.github.balookrd.pristan.parom.orchestrator.registry.AgentRegistry;
import io.github.balookrd.pristan.parom.orchestrator.repository.JobRepository;
import io.github.balookrd.pristan.parom.orchestrator.repository.JobRunRepository;
import io.github.balookrd.pristan.parom.orchestrator.repository.TaskRepository;
import io.github.balookrd.pristan.parom.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A task report is accepted only from the agent the task is assigned to and only while it runs.
 *
 * <p>After a failover the previous agent may still finish or fail its attempt. Its report used
 * to be applied blindly: a late failure put a task that already runs on another agent back into
 * the queue (a second transfer and one retry burned), a late completion closed a task another
 * agent was still transferring.
 */
@DisplayName("Задачи: отчет принимается только от назначенного агента и только для RUNNING")
class TaskStatusFencingTest {

    private TaskRepository taskRepository;
    private TaskService taskService;
    private TaskEntity task;

    @BeforeEach
    void setUp() {
        taskRepository = mock(TaskRepository.class);
        taskService = new TaskService(taskRepository, mock(JobRepository.class), mock(JobRunRepository.class),
                mock(AgentRegistry.class), mock(TopologyRegistry.class), new ParomProperties());

        task = new TaskEntity();
        task.setId("task-1");
        task.setJobId("job-1");
        task.setStatus("RUNNING");
        task.setAssignedAgentId("agent-b");
        task.setRetryCount(1);
        task.setMaxRetries(3);
        when(taskRepository.findById("task-1")).thenReturn(Optional.of(task));
        when(taskRepository.findAllById(any())).thenReturn(List.of(task));
    }

    @Test
    @DisplayName("Сбой от прежнего агента не возвращает в очередь задачу, идущую на другом")
    void lateFailureFromPreviousAgentIsIgnored() {
        boolean accepted = taskService.failTask(new FailTaskRequest("task-1", "agent-a", "обрыв стрима"));

        assertFalse(accepted);
        assertEquals("RUNNING", task.getStatus());
        assertEquals("agent-b", task.getAssignedAgentId());
        assertEquals(1, task.getRetryCount());
        verify(taskRepository, never()).save(any());
    }

    @Test
    @DisplayName("Завершение от прежнего агента не закрывает задачу, идущую на другом")
    void lateCompletionFromPreviousAgentIsIgnored() {
        boolean accepted = taskService.completeTask(new CompleteTaskRequest("task-1", "agent-a", 1024L, null));

        assertFalse(accepted);
        assertEquals("RUNNING", task.getStatus());
        assertEquals("agent-b", task.getAssignedAgentId());
    }

    @Test
    @DisplayName("Пакетное завершение от прежнего агента тоже игнорируется")
    void lateBatchCompletionFromPreviousAgentIsIgnored() {
        int completed = taskService.completeTasksBatch(
                List.of(new CompleteTaskRequest("task-1", "agent-a", 1024L, null)));

        assertEquals(0, completed);
        assertEquals("RUNNING", task.getStatus());
    }

    @Test
    @DisplayName("Сбой по задаче, уже возвращенной в очередь, не сжигает еще одну попытку")
    void failureForRequeuedTaskIsIgnored() {
        task.setStatus("QUEUED");
        task.setAssignedAgentId(null);

        boolean accepted = taskService.failTask(new FailTaskRequest("task-1", "agent-a", "обрыв стрима"));

        assertFalse(accepted);
        assertEquals("QUEUED", task.getStatus());
        assertEquals(1, task.getRetryCount());
    }

    @Test
    @DisplayName("Отчет назначенного агента принимается")
    void reportFromAssignedAgentIsAccepted() {
        assertTrue(taskService.completeTask(new CompleteTaskRequest("task-1", "agent-b", 1024L, null)));
        assertEquals("COMPLETED", task.getStatus());
    }
}
