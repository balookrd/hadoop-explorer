package io.github.balookrd.pristan.artel.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.github.balookrd.pristan.artel.entity.ChangeRequestStatus;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Counts change request status transitions for Prometheus (artel_change_requests_total).
 * Inside a transaction the counter moves only after commit, so a rolled back transition is not counted.
 */
@Component
public class ChangeRequestMetrics {

    public static final String CHANGE_REQUESTS_METRIC = "artel.change.requests";

    private final MeterRegistry registry;

    public ChangeRequestMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordTransition(String clusterId, ChangeRequestStatus status) {
        Counter counter = Counter.builder(CHANGE_REQUESTS_METRIC)
                .description("Change request status transitions")
                .tag("cluster", clusterId)
                .tag("status", status.name())
                .register(registry);

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    counter.increment();
                }
            });
        } else {
            counter.increment();
        }
    }
}
