package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Транзакционность путей, вызываемых мимо прокси Spring.
 *
 * <p>{@code @Transactional} работает только на вызове через прокси. Остановка контейнера
 * (`@PreDestroy`) и действие, переданное в {@code runWithLock} ссылкой на свой же метод,
 * вызывают метод напрямую — аннотация не срабатывает, и записи идут без транзакции: на
 * PostgreSQL модифицирующий запрос в read-only транзакции репозитория просто падает, то есть
 * остаточный буфер прогресса теряется при каждой остановке узла, а failover осиротевших задач
 * применяется частями.
 */
@SpringBootTest
@ActiveProfiles("test")
class TaskServiceTransactionalSelfInvocationTest {

    @Autowired
    private PlatformTransactionManager transactionManager;

    private final TaskRepository taskRepository = mock(TaskRepository.class);
    private final JobRepository jobRepository = mock(JobRepository.class);
    private final JobRunRepository jobRunRepository = mock(JobRunRepository.class);
    private final AgentRegistry agentRegistry = mock(AgentRegistry.class);
    private final TopologyRegistry topologyRegistry = mock(TopologyRegistry.class);

    private TaskService newTaskService() {
        return new TaskService(taskRepository, jobRepository, jobRunRepository,
                agentRegistry, topologyRegistry, new ReplicatorProperties(),
                null, null, transactionManager);
    }

    @Test
    @DisplayName("Сброс остаточного буфера прогресса при остановке идет в транзакции")
    void shutdownFlushShouldRunInsideTransaction() {
        AtomicBoolean transactionActive = new AtomicBoolean(false);
        when(jobRepository.incrementProgress(anyString(), anyLong(), anyInt(), any()))
                .thenAnswer(invocation -> {
                    transactionActive.set(TransactionSynchronizationManager.isActualTransactionActive());
                    return 1;
                });
        // Незавершенные подзадачи остались — финализация задания в этом тесте не нужна
        when(taskRepository.existsByJobIdAndStatusIn(anyString(), anyCollection())).thenReturn(true);
        when(jobRepository.findByStatus("RUNNING")).thenReturn(List.of());

        TaskService taskService = newTaskService();
        taskService.getProgressBuffer().record("job-shutdown-flush", 4096L, 3);

        taskService.shutdown();

        verify(jobRepository).incrementProgress(eq("job-shutdown-flush"), eq(4096L), eq(3), any());
        assertTrue(transactionActive.get(),
                "Остаточный сброс прогресса при остановке должен выполняться в транзакции, "
                        + "иначе модифицирующий запрос отвергается read-only транзакцией репозитория");
    }

    @Test
    @DisplayName("Failover осиротевших задач из шедулера идет одной транзакцией")
    void orphanFailoverShouldRunInsideTransaction() {
        AtomicBoolean transactionActive = new AtomicBoolean(false);
        when(agentRegistry.getAgents()).thenReturn(List.of());
        when(taskRepository.findByStatusAndAssignedAgentIdIsNull(eq("RUNNING"), any()))
                .thenAnswer(invocation -> {
                    transactionActive.set(TransactionSynchronizationManager.isActualTransactionActive());
                    return List.of();
                });
        when(taskRepository.findByStatusAndUpdatedAtBefore(eq("RUNNING"), any(), any()))
                .thenReturn(List.of());

        TaskService taskService = newTaskService();

        taskService.checkAndFailoverOrphanedTasks();

        assertTrue(transactionActive.get(),
                "Пересчет осиротевших задач должен идти одной транзакцией: иначе частичный сбой "
                        + "оставляет failover применённым наполовину");
    }
}
