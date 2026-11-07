package org.apache.hadoop.explorer.sql.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Records SQL query execution time for Prometheus (sql_query_duration_seconds).
 * Metric names match monitoring/grafana/dashboards/spark_sql_explorer_analytics.json.
 */
@Component
public class SqlQueryMetrics {

    public static final String QUERY_DURATION_METRIC = "sql.query.duration";

    private static final Duration[] BUCKETS = {
            Duration.ofMillis(100), Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(5),
            Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofMinutes(1), Duration.ofMinutes(5)
    };

    private static final Logger log = LoggerFactory.getLogger(SqlQueryMetrics.class);

    private final MeterRegistry registry;

    public SqlQueryMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * @param status terminal status written to query history: FINISHED, CANCELLED or FAILED
     */
    public void recordQuery(SqlProperties.ClusterConfig cluster, String status, long durationNanos) {
        try {
            String engine = cluster.getType() == null ? "unknown" : cluster.getType().toLowerCase(Locale.ROOT);
            Timer.builder(QUERY_DURATION_METRIC)
                    .description("SQL query execution time from start to a terminal status")
                    .tag("cluster", String.valueOf(cluster.getId()))
                    .tag("engine", engine)
                    .tag("status", status)
                    .serviceLevelObjectives(BUCKETS)
                    .register(registry)
                    .record(durationNanos, TimeUnit.NANOSECONDS);
        } catch (RuntimeException e) {
            // Metrics must never change the outcome of a query
            log.warn("Failed to record {}: {}", QUERY_DURATION_METRIC, e.getMessage());
        }
    }
}
