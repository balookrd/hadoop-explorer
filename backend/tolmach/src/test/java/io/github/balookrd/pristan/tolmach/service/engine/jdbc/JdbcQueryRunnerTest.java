package io.github.balookrd.pristan.tolmach.service.engine.jdbc;

import io.github.balookrd.pristan.tolmach.dto.ColumnMetadata;
import io.github.balookrd.pristan.tolmach.service.engine.QueryTimeoutException;
import io.github.balookrd.pristan.tolmach.service.engine.SqlEngine;
import io.github.balookrd.pristan.tolmach.service.engine.SqlEngineException;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcQueryRunnerTest {

    private static final String URL = "jdbc:h2:mem:runner;DB_CLOSE_DELAY=-1";
    // 10^10 joined rows with a predicate H2 cannot fold: runs for minutes unless cancelled
    private static final String SLOW_QUERY =
            "SELECT COUNT(*) FROM SYSTEM_RANGE(1, 100000) a, SYSTEM_RANGE(1, 100000) b WHERE a.X + b.X < 0";

    private final List<Map<String, Object>> events = new CopyOnWriteArrayList<>();

    private JdbcQueryRunner runner(Duration timeout) {
        return new JdbcQueryRunner("h2-test", username -> DriverManager.getConnection(URL, "sa", ""),
                timeout, 1000, JdbcQueryRunner.StatementCustomizer.NONE);
    }

    @Test
    void streamsRowsInBatchesAndFinishes() {
        SqlEngine.ExecutionResult result = runner(Duration.ofMinutes(1))
                .run("q1", "SELECT X FROM SYSTEM_RANGE(1, 2500)", "analyst", 10_000, events::add, new AtomicBoolean());

        assertEquals("FINISHED", result.status());
        assertEquals(2500, result.totalRows());
        assertEquals(List.of(new ColumnMetadata("X", "bigint")), result.columns());
        assertEquals(1L, result.rows().get(0).get(0));
        List<Map<String, Object>> rowEvents = events.stream().filter(e -> "rows".equals(e.get("type"))).toList();
        assertEquals(3, rowEvents.size());
        assertEquals(2500, rowEvents.get(2).get("total_rows"));
        Map<String, Object> last = events.get(events.size() - 1);
        assertEquals("finished", last.get("type"));
        assertEquals("FINISHED", last.get("status"));
    }

    @Test
    void truncatesAtMaxRows() {
        SqlEngine.ExecutionResult result = runner(Duration.ofMinutes(1))
                .run("q2", "SELECT X FROM SYSTEM_RANGE(1, 2500)", "analyst", 1200, events::add, new AtomicBoolean());

        assertEquals(1200, result.totalRows());
        assertTrue(result.message().contains("усечен до 1200"), result.message());
    }

    @Test
    void statementWithoutResultSetFinishesWithZeroRows() {
        SqlEngine.ExecutionResult result = runner(Duration.ofMinutes(1))
                .run("q3", "CREATE TABLE IF NOT EXISTS runner_ddl (id BIGINT)", "analyst", 10, events::add, new AtomicBoolean());

        assertEquals("FINISHED", result.status());
        assertEquals(0, result.totalRows());
    }

    @Test
    void cancelStopsLongRunningQuery() throws Exception {
        AtomicBoolean cancel = new AtomicBoolean();
        CompletableFuture<SqlEngine.ExecutionResult> future = CompletableFuture.supplyAsync(() ->
                runner(Duration.ofMinutes(5)).run("q4", SLOW_QUERY, "analyst", 10, events::add, cancel));
        Thread.sleep(500);
        cancel.set(true);

        SqlEngine.ExecutionResult result = future.get(10, TimeUnit.SECONDS);
        assertEquals("CANCELLED", result.status());
    }

    @Test
    void timeoutFailsQuery() {
        QueryTimeoutException e = assertThrows(QueryTimeoutException.class, () ->
                runner(Duration.ofMillis(300)).run("q5", SLOW_QUERY, "analyst", 10, events::add, new AtomicBoolean()));
        assertTrue(e.getMessage().contains("h2-test"), e.getMessage());
    }

    @Test
    void sqlErrorBecomesSqlEngineException() {
        SqlEngineException e = assertThrows(SqlEngineException.class, () ->
                runner(Duration.ofMinutes(1)).run("q6", "SELECT * FROM missing_table", "analyst", 10, events::add, new AtomicBoolean()));
        assertTrue(e.getMessage().startsWith("h2-test: "), e.getMessage());
    }
}
