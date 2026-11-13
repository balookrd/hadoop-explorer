package io.github.balookrd.pristan.iskra.service;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.github.balookrd.pristan.iskra.repository.SparkSessionRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SparkMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final SparkSessionRepository repository = mock(SparkSessionRepository.class);
    private final SparkMetrics metrics = new SparkMetrics(registry, repository);

    @Test
    void recordsStatementDurationPerClusterLanguageAndStatus() {
        metrics.recordStatement("prod-analytics", "pyspark", "FINISHED", TimeUnit.SECONDS.toNanos(2));

        Timer timer = registry.find(SparkMetrics.STATEMENT_DURATION_METRIC)
                .tags("cluster", "prod-analytics", "language", "pyspark", "status", "FINISHED")
                .timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());
        assertEquals(2.0, timer.totalTime(TimeUnit.SECONDS), 0.001);
    }

    @Test
    void nullLabelsAreRecordedAsUnknown() {
        metrics.recordStatement(null, null, "FAILED", 1_000_000L);

        assertNotNull(registry.find(SparkMetrics.STATEMENT_DURATION_METRIC)
                .tags("cluster", "unknown", "language", "unknown", "status", "FAILED")
                .timer());
    }

    @Test
    void refreshPublishesActiveSessionsAndDropsVanishedRows() {
        when(repository.countByClusterAndKindExcludingStatuses(SparkMetrics.FINISHED_SESSION_STATUSES))
                .thenReturn(List.of(count("prod-analytics", "pyspark", 2L)))
                .thenReturn(List.of());

        metrics.refreshActiveSessions();
        assertEquals(2.0, registry.get(SparkMetrics.ACTIVE_SESSIONS_METRIC)
                .tags("cluster", "prod-analytics", "kind", "pyspark")
                .gauge().value());

        metrics.refreshActiveSessions();
        assertNull(registry.find(SparkMetrics.ACTIVE_SESSIONS_METRIC)
                .tags("cluster", "prod-analytics", "kind", "pyspark")
                .gauge());
    }

    @Test
    void unknownLanguageIsCollapsedToOtherAndCaseIsNormalized() {
        metrics.recordStatement("c1", "select secret from t", "FINISHED", 1_000_000L);
        metrics.recordStatement("c1", "PySpark", "FINISHED", 1_000_000L);
        metrics.recordStatement("c1", "  ", "FINISHED", 1_000_000L);

        assertNotNull(registry.find(SparkMetrics.STATEMENT_DURATION_METRIC).tags("language", "other").timer());
        assertNotNull(registry.find(SparkMetrics.STATEMENT_DURATION_METRIC).tags("language", "pyspark").timer());
        assertNotNull(registry.find(SparkMetrics.STATEMENT_DURATION_METRIC).tags("language", "unknown").timer());
        assertEquals(3, registry.find(SparkMetrics.STATEMENT_DURATION_METRIC).timers().size());
    }

    @Test
    void foreignKindsAreSummedIntoSingleOtherSeries() {
        when(repository.countByClusterAndKindExcludingStatuses(SparkMetrics.FINISHED_SESSION_STATUSES))
                .thenReturn(List.of(count("c1", "evil-1", 2L), count("c1", "evil-2", 3L),
                        count("c1", "PySpark", 1L), count("c1", null, 4L)));

        metrics.refreshActiveSessions();

        assertEquals(5.0, registry.get(SparkMetrics.ACTIVE_SESSIONS_METRIC)
                .tags("cluster", "c1", "kind", "other").gauge().value());
        assertEquals(1.0, registry.get(SparkMetrics.ACTIVE_SESSIONS_METRIC)
                .tags("cluster", "c1", "kind", "pyspark").gauge().value());
        assertEquals(4.0, registry.get(SparkMetrics.ACTIVE_SESSIONS_METRIC)
                .tags("cluster", "c1", "kind", "unknown").gauge().value());
        assertEquals(3, registry.find(SparkMetrics.ACTIVE_SESSIONS_METRIC).gauges().size());
    }

    @Test
    void refreshSurvivesRepositoryFailure() {
        when(repository.countByClusterAndKindExcludingStatuses(SparkMetrics.FINISHED_SESSION_STATUSES))
                .thenThrow(new IllegalStateException("db down"));

        assertDoesNotThrow(metrics::refreshActiveSessions);
    }

    private static SparkSessionRepository.ActiveSessionCount count(String cluster, String kind, long sessions) {
        return new SparkSessionRepository.ActiveSessionCount() {
            @Override
            public String getClusterId() {
                return cluster;
            }

            @Override
            public String getKind() {
                return kind;
            }

            @Override
            public Long getSessions() {
                return sessions;
            }
        };
    }
}
