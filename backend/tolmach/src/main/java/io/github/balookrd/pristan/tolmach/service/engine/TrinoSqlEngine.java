package io.github.balookrd.pristan.tolmach.service.engine;

import io.trino.jdbc.TrinoStatement;
import io.github.balookrd.pristan.tolmach.config.SqlProperties;
import io.github.balookrd.pristan.tolmach.service.engine.jdbc.JdbcSqlEngine;
import io.github.balookrd.pristan.tolmach.service.engine.trino.TrinoConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Trino over trino-jdbc; used when the cluster has type: trino and mock-storage: false. */
public class TrinoSqlEngine extends JdbcSqlEngine {

    private static final long PROGRESS_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final TrinoConnectionFactory connections;

    public TrinoSqlEngine(SqlProperties.ClusterConfig cluster) {
        super(cluster, TrinoSqlEngine::attachProgressMonitor);
        this.connections = new TrinoConnectionFactory(cluster);
    }

    @Override
    protected Connection openConnection(String username) throws SQLException {
        return connections.open(username);
    }

    private static void attachProgressMonitor(Statement statement, Consumer<Map<String, Object>> events) throws SQLException {
        if (!statement.isWrapperFor(TrinoStatement.class)) {
            return;
        }
        AtomicLong lastEmitNanos = new AtomicLong();
        statement.unwrap(TrinoStatement.class).setProgressMonitor(stats -> {
            long now = System.nanoTime();
            long last = lastEmitNanos.get();
            if (last != 0 && now - last < PROGRESS_INTERVAL_NANOS || !lastEmitNanos.compareAndSet(last, now)) {
                return;
            }
            // Always RUNNING: Trino's own FINISHED/FAILED would make the UI close the stream before rows arrive
            events.accept(Map.of(
                    "type", "status",
                    "status", "RUNNING",
                    "progress", stats.getProgressPercentage().orElse(0.0),
                    "message", "Trino " + stats.getQueryId() + ": " + stats.getState()
                            + ", splits " + stats.getCompletedSplits() + "/" + stats.getTotalSplits()));
        });
    }
}
