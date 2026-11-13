package io.github.balookrd.pristan.replicator.orchestrator.hms;

import io.github.balookrd.pristan.replicator.orchestrator.config.ReplicatorProperties;
import io.github.balookrd.pristan.replicator.orchestrator.hms.service.HmsCoordinatorService;
import io.github.balookrd.pristan.replicator.orchestrator.registry.AgentRegistry;
import io.github.balookrd.pristan.replicator.orchestrator.repository.HmsEventLogRepository;
import io.github.balookrd.pristan.replicator.orchestrator.repository.HmsReplicationJobRepository;
import io.github.balookrd.pristan.replicator.orchestrator.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Транзакционность автоматического failover брошенных HMS-задач.
 *
 * <p>Шедулер передает действие в {@code runWithLock} ссылкой на свой же метод, поэтому вызов
 * идет мимо прокси Spring и {@code @Transactional} не срабатывает. Проход снимает аренду,
 * пишет журнал событий и переназначает задачу — без транзакции сбой на середине оставляет
 * задачу без владельца, но со старой позицией в CDC.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("HMS: failover брошенных задач идет одной транзакцией")
class HmsFailoverTransactionTest {

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void scheduledFailoverShouldRunInsideTransaction() {
        HmsReplicationJobRepository hmsJobRepository = mock(HmsReplicationJobRepository.class);
        HmsEventLogRepository hmsEventLogRepository = mock(HmsEventLogRepository.class);
        AgentRegistry agentRegistry = mock(AgentRegistry.class);
        JobService jobService = mock(JobService.class);

        AtomicBoolean transactionActive = new AtomicBoolean(false);
        when(hmsJobRepository.findByAssignedAgentIdIsNotNullAndStatusIn(anyList()))
                .thenAnswer(invocation -> {
                    transactionActive.set(TransactionSynchronizationManager.isActualTransactionActive());
                    return List.of();
                });

        HmsCoordinatorService coordinator = new HmsCoordinatorService(
                hmsJobRepository, hmsEventLogRepository, null, agentRegistry, jobService,
                new ReplicatorProperties(), null, transactionManager);

        coordinator.scheduledCheckAndFailoverOrphanedHmsJobs();

        assertTrue(transactionActive.get(),
                "Проход failover должен идти одной транзакцией: иначе снятие аренды, журнал "
                        + "и переназначение задачи фиксируются порознь");
    }
}
