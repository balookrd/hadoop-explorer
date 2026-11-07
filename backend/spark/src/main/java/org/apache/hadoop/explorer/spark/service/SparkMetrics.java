package org.apache.hadoop.explorer.spark.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.apache.hadoop.explorer.spark.repository.SparkSessionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Spark Explorer metrics for Prometheus: statement duration and sessions that are still alive.
 * Metric names match monitoring/grafana/dashboards/spark_sql_explorer_analytics.json.
 */
@Component
public class SparkMetrics {

    public static final String STATEMENT_DURATION_METRIC = "spark.statement.duration";
    public static final String ACTIVE_SESSIONS_METRIC = "spark.sessions.active";

    /** Statuses SparkSessionManager treats as finished, see getActiveSessions(). */
    public static final List<String> FINISHED_SESSION_STATUSES = List.of("dead", "killed");

    private static final String UNKNOWN = "unknown";
    private static final String OTHER = "other";

    // Languages and Livy session kinds Spark Explorer works with; anything else must not become a label value
    private static final Set<String> KNOWN_VALUES = Set.of("pyspark", "spark", "scalaspark", "scala", "sparkr", "sql");

    private static final Duration[] BUCKETS = {
            Duration.ofMillis(100), Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(5),
            Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofMinutes(1), Duration.ofMinutes(5)
    };

    private static final Logger log = LoggerFactory.getLogger(SparkMetrics.class);

    private final MeterRegistry registry;
    private final SparkSessionRepository sessionRepository;
    private final MultiGauge activeSessions;

    public SparkMetrics(MeterRegistry registry, SparkSessionRepository sessionRepository) {
        this.registry = registry;
        this.sessionRepository = sessionRepository;
        this.activeSessions = MultiGauge.builder(ACTIVE_SESSIONS_METRIC)
                .description("Spark sessions that are not dead or killed, refreshed every 30 s")
                .register(registry);
    }

    /**
     * Records one statement exactly once, at its terminal outcome.
     *
     * @param status terminal status written to execution history: FINISHED, CANCELLED or FAILED
     */
    public void recordStatement(String clusterId, String language, String status, long durationNanos) {
        try {
            Timer.builder(STATEMENT_DURATION_METRIC)
                    .description("Spark statement execution time from start to a terminal status")
                    .tag("cluster", clusterId == null ? UNKNOWN : clusterId)
                    .tag("language", normalize(language))
                    .tag("status", status)
                    .serviceLevelObjectives(BUCKETS)
                    .register(registry)
                    .record(durationNanos, TimeUnit.NANOSECONDS);
        } catch (RuntimeException e) {
            // Metrics must never change the outcome of a statement
            log.warn("Failed to record {}: {}", STATEMENT_DURATION_METRIC, e.getMessage());
        }
    }

    /** Maps a user-supplied language or kind to a bounded label value: known name, "unknown" or "other". */
    static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return UNKNOWN;
        }
        String lower = value.strip().toLowerCase(Locale.ROOT);
        return KNOWN_VALUES.contains(lower) ? lower : OTHER;
    }

    // One GROUP BY query every 30 s instead of a database query per Prometheus scrape
    @Scheduled(fixedDelay = 30_000)
    public void refreshActiveSessions() {
        try {
            // Several raw kinds can collapse into "other", so sum per (cluster, normalized kind)
            Map<List<String>, Long> totals = new LinkedHashMap<>();
            for (SparkSessionRepository.ActiveSessionCount count
                    : sessionRepository.countByClusterAndKindExcludingStatuses(FINISHED_SESSION_STATUSES)) {
                String cluster = count.getClusterId() == null ? UNKNOWN : count.getClusterId();
                totals.merge(List.of(cluster, normalize(count.getKind())), count.getSessions(), Long::sum);
            }
            List<MultiGauge.Row<?>> rows = new ArrayList<>();
            totals.forEach((key, sessions) ->
                    rows.add(MultiGauge.Row.of(Tags.of("cluster", key.get(0), "kind", key.get(1)), sessions)));
            // overwrite=true removes series for cluster/kind pairs that no longer have sessions
            activeSessions.register(rows, true);
        } catch (RuntimeException e) {
            // A database error must not stop the scheduler; the previous values stay published
            log.warn("Failed to refresh {}: {}", ACTIVE_SESSIONS_METRIC, e.getMessage());
        }
    }
}
