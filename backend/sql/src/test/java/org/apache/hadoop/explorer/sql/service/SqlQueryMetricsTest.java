package org.apache.hadoop.explorer.sql.service;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SqlQueryMetricsTest {

    @Test
    void recordsDurationPerClusterEngineAndStatus() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SqlQueryMetrics metrics = new SqlQueryMetrics(registry);
        SqlProperties.ClusterConfig cluster = new SqlProperties.ClusterConfig();
        cluster.setId("trino-prod");
        cluster.setType("Trino");

        metrics.recordQuery(cluster, "FINISHED", TimeUnit.MILLISECONDS.toNanos(250));

        Timer timer = registry.find(SqlQueryMetrics.QUERY_DURATION_METRIC)
                .tags("cluster", "trino-prod", "engine", "trino", "status", "FINISHED")
                .timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());
        assertEquals(250.0, timer.totalTime(TimeUnit.MILLISECONDS), 0.001);
    }
}
