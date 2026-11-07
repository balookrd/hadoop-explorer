package org.apache.hadoop.explorer.yarn.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.hadoop.explorer.yarn.entity.ChangeRequestStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Счетчик переходов Change Request для Prometheus")
class ChangeRequestMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final ChangeRequestMetrics metrics = new ChangeRequestMetrics(registry);

    @AfterEach
    void clearSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    @DisplayName("Вне транзакции переход считается сразу")
    void countsTransitionImmediatelyOutsideTransaction() {
        metrics.recordTransition("prod-yarn", ChangeRequestStatus.SUBMITTED);

        assertEquals(1.0, count("prod-yarn", "SUBMITTED"));
    }

    @Test
    @DisplayName("В транзакции переход считается только после коммита")
    void countsTransitionOnlyAfterCommit() {
        TransactionSynchronizationManager.initSynchronization();

        metrics.recordTransition("prod-yarn", ChangeRequestStatus.APPROVED);
        assertEquals(0.0, count("prod-yarn", "APPROVED"));

        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        assertEquals(1.0, count("prod-yarn", "APPROVED"));
    }

    @Test
    @DisplayName("Откаченный переход не считается")
    void doesNotCountRolledBackTransition() {
        TransactionSynchronizationManager.initSynchronization();

        metrics.recordTransition("prod-yarn", ChangeRequestStatus.REJECTED);
        TransactionSynchronizationManager.getSynchronizations()
                .forEach(s -> s.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));

        assertEquals(0.0, count("prod-yarn", "REJECTED"));
    }

    private double count(String cluster, String status) {
        Counter counter = registry.find(ChangeRequestMetrics.CHANGE_REQUESTS_METRIC)
                .tags("cluster", cluster, "status", status)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }
}
