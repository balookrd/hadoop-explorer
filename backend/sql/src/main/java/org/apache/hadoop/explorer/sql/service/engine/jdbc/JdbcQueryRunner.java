package org.apache.hadoop.explorer.sql.service.engine.jdbc;

import org.apache.hadoop.explorer.sql.dto.ColumnMetadata;
import org.apache.hadoop.explorer.sql.service.engine.QueryTimeoutException;
import org.apache.hadoop.explorer.sql.service.engine.SqlEngine;
import org.apache.hadoop.explorer.sql.service.engine.SqlEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Runs one SQL statement over JDBC and streams the result in the SSE contract of QueryManagerService:
 * status -> columns -> rows (batches) -> finished. Cancellation and the query timeout are enforced by a
 * watchdog calling Statement.cancel(), because a blocked execute()/next() never looks at the cancel flag.
 * CANCELLED is returned as a result (QueryManagerService publishes the terminal event), a timeout and
 * SQL errors are thrown as SqlEngineException (QueryManagerService marks the query FAILED).
 */
public final class JdbcQueryRunner {

    @FunctionalInterface
    public interface ConnectionProvider {
        Connection open(String username) throws SQLException;
    }

    @FunctionalInterface
    public interface StatementCustomizer {
        StatementCustomizer NONE = (statement, events) -> {
        };

        void customize(Statement statement, Consumer<Map<String, Object>> events) throws SQLException;
    }

    private enum StopReason { CANCELLED, TIMEOUT }

    private static final Logger log = LoggerFactory.getLogger(JdbcQueryRunner.class);
    private static final long WATCHDOG_PERIOD_MS = 200;
    private static final ScheduledExecutorService WATCHDOG = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "sql-query-watchdog");
        thread.setDaemon(true);
        return thread;
    });

    private final String clusterName;
    private final ConnectionProvider connectionProvider;
    private final Duration queryTimeout;
    private final int batchSize;
    private final StatementCustomizer customizer;

    public JdbcQueryRunner(String clusterName, ConnectionProvider connectionProvider, Duration queryTimeout,
                           int batchSize, StatementCustomizer customizer) {
        this.clusterName = clusterName;
        this.connectionProvider = connectionProvider;
        this.queryTimeout = queryTimeout;
        this.batchSize = Math.max(batchSize, 1);
        this.customizer = customizer;
    }

    public SqlEngine.ExecutionResult run(String queryId, String sql, String username, int maxRows,
                                         Consumer<Map<String, Object>> events, AtomicBoolean cancelled) {
        long startNanos = System.nanoTime();
        events.accept(Map.of("type", "status", "status", "QUEUED",
                "message", "Подключение к " + clusterName + " от имени " + username + "..."));
        if (cancelled.get()) {
            return cancelledResult(List.of(), List.of(), startNanos);
        }
        AtomicReference<StopReason> stop = new AtomicReference<>();
        List<ColumnMetadata> columns = List.of();
        List<List<Object>> rows = new ArrayList<>();
        try (Connection connection = connectionProvider.open(username);
             Statement statement = connection.createStatement()) {
            int fetchSize = batchSize;
            if (maxRows > 0 && maxRows < Integer.MAX_VALUE) {
                statement.setMaxRows(maxRows + 1); // the extra row tells us the result was truncated
                // JDBC requires fetchSize <= maxRows (H2 rejects a larger value)
                fetchSize = Math.min(batchSize, maxRows + 1);
            }
            statement.setFetchSize(fetchSize);
            customizer.customize(statement, events);
            ScheduledFuture<?> watchdog = startWatchdog(statement, cancelled, startNanos, stop);
            try {
                events.accept(Map.of("type", "status", "status", "RUNNING",
                        "message", "Исполнение под учетной записью " + username + "..."));
                if (!statement.execute(sql)) {
                    return finished(events, columns, rows, false, startNanos, maxRows);
                }
                try (ResultSet resultSet = statement.getResultSet()) {
                    columns = JdbcValueMapper.columns(resultSet.getMetaData());
                    events.accept(Map.of("type", "columns", "columns", columns));
                    boolean truncated = readRows(resultSet, columns.size(), maxRows, rows, events, stop);
                    if (stop.get() != null) {
                        return stopped(stop.get(), columns, rows, startNanos);
                    }
                    return finished(events, columns, rows, truncated, startNanos, maxRows);
                }
            } finally {
                watchdog.cancel(false);
            }
        } catch (SQLException e) {
            if (stop.get() != null) {
                return stopped(stop.get(), columns, rows, startNanos);
            }
            log.warn("Query {} failed on {}: {}", queryId, clusterName, e.getMessage());
            throw new SqlEngineException(clusterName + ": " + e.getMessage(), e);
        }
    }

    private boolean readRows(ResultSet resultSet, int columnCount, int maxRows, List<List<Object>> rows,
                             Consumer<Map<String, Object>> events, AtomicReference<StopReason> stop) throws SQLException {
        List<List<Object>> batch = new ArrayList<>(batchSize);
        boolean truncated = false;
        while (stop.get() == null && resultSet.next()) {
            if (maxRows > 0 && rows.size() >= maxRows) {
                truncated = true;
                break;
            }
            List<Object> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                row.add(JdbcValueMapper.toJson(resultSet.getObject(i)));
            }
            List<Object> frozen = Collections.unmodifiableList(row);
            rows.add(frozen);
            batch.add(frozen);
            if (batch.size() >= batchSize) {
                emitRows(events, batch, rows.size());
                batch = new ArrayList<>(batchSize);
            }
        }
        if (!batch.isEmpty()) {
            emitRows(events, batch, rows.size());
        }
        return truncated;
    }

    private static void emitRows(Consumer<Map<String, Object>> events, List<List<Object>> batch, int total) {
        events.accept(Map.of("type", "rows", "rows", batch, "total_rows", total));
    }

    private ScheduledFuture<?> startWatchdog(Statement statement, AtomicBoolean cancelled, long startNanos,
                                             AtomicReference<StopReason> stop) {
        long deadline = startNanos + queryTimeout.toNanos();
        return WATCHDOG.scheduleWithFixedDelay(() -> {
            StopReason reason = cancelled.get() ? StopReason.CANCELLED
                    : System.nanoTime() - deadline >= 0 ? StopReason.TIMEOUT : null;
            if (reason != null && stop.compareAndSet(null, reason)) {
                // Statement.cancel() may do network I/O (Trino: DELETE on the coordinator); keep the watchdog free
                Thread.ofVirtual().name("sql-query-cancel").start(() -> cancelQuietly(statement));
            }
        }, WATCHDOG_PERIOD_MS, WATCHDOG_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    private void cancelQuietly(Statement statement) {
        try {
            statement.cancel();
        } catch (SQLException e) {
            log.debug("Statement.cancel() on {} failed: {}", clusterName, e.getMessage());
        }
    }

    private SqlEngine.ExecutionResult stopped(StopReason reason, List<ColumnMetadata> columns,
                                              List<List<Object>> rows, long startNanos) {
        if (reason == StopReason.TIMEOUT) {
            throw new QueryTimeoutException("Превышен лимит времени выполнения запроса на " + clusterName
                    + ": " + queryTimeout.toSeconds() + " с (jdbc.query-timeout-seconds)");
        }
        return cancelledResult(columns, rows, startNanos);
    }

    private SqlEngine.ExecutionResult finished(Consumer<Map<String, Object>> events, List<ColumnMetadata> columns,
                                               List<List<Object>> rows, boolean truncated, long startNanos, int maxRows) {
        double durationMs = elapsedMs(startNanos);
        String message = truncated
                ? "Запрос выполнен на кластере " + clusterName + ". Результат усечен до " + maxRows
                  + " строк (sql.max-rows-per-query)."
                : "Запрос выполнен на кластере " + clusterName + ". Получено " + rows.size() + " строк.";
        events.accept(Map.of("type", "finished", "status", "FINISHED", "total_rows", rows.size(),
                "execution_time_ms", durationMs, "message", message));
        return new SqlEngine.ExecutionResult(columns, rows, rows.size(), durationMs, "FINISHED", message);
    }

    private static SqlEngine.ExecutionResult cancelledResult(List<ColumnMetadata> columns, List<List<Object>> rows,
                                                             long startNanos) {
        return new SqlEngine.ExecutionResult(columns, rows, rows.size(), elapsedMs(startNanos), "CANCELLED",
                "Запрос отменен пользователем");
    }

    private static double elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000.0;
    }
}
