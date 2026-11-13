package io.github.balookrd.pristan.tolmach.service.engine.trino;

import io.github.balookrd.pristan.tolmach.config.SqlProperties;
import io.github.balookrd.pristan.tolmach.dto.ColumnMetadata;
import io.github.balookrd.pristan.tolmach.service.engine.SqlEngine;
import io.github.balookrd.pristan.tolmach.service.engine.SqlEngineException;
import io.github.balookrd.pristan.tolmach.service.engine.TrinoSqlEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.trino.TrinoContainer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class TrinoSqlEngineContainerTest {

    @Container
    static final TrinoContainer TRINO = new TrinoContainer("trinodb/trino:484");

    private static TrinoSqlEngine engine;

    @BeforeAll
    static void createEngine() {
        SqlProperties.ClusterConfig cluster = new SqlProperties.ClusterConfig();
        cluster.setId("trino-it");
        cluster.setName("Trino IT");
        cluster.setType("trino");
        cluster.setHost(TRINO.getHost());
        cluster.setPort(TRINO.getMappedPort(8080));
        cluster.setCatalog("tpch");
        cluster.setSchema("tiny");
        engine = new TrinoSqlEngine(cluster);
    }

    @Test
    void readsCatalogMetadata() {
        assertTrue(engine.getCatalogs("analyst").contains("tpch"));
        assertTrue(engine.getSchemas("analyst", "tpch").contains("tiny"));
        assertTrue(engine.getTables("analyst", "tpch", "tiny").contains("nation"));
        assertTrue(engine.getColumns("analyst", "tpch", "tiny", "nation").contains(new ColumnMetadata("nationkey", "bigint")));
    }

    @Test
    void runsQueryAsImpersonatedUser() {
        List<Map<String, Object>> events = new CopyOnWriteArrayList<>();
        SqlEngine.ExecutionResult result = engine.execute("q-user", "SELECT current_user", "analyst", 100, events::add, new AtomicBoolean());

        assertEquals("FINISHED", result.status());
        assertEquals(List.of(List.of("analyst")), result.rows());
        // Trino's own FINISHED state must never leak as a status event: the UI would close the stream early
        assertTrue(events.stream().filter(e -> "status".equals(e.get("type")))
                .noneMatch(e -> "FINISHED".equals(e.get("status"))));
    }

    @Test
    void streamsRowsAndTruncates() {
        SqlEngine.ExecutionResult result = engine.execute("q-rows", "SELECT nationkey, name FROM nation ORDER BY nationkey",
                "analyst", 10, e -> { }, new AtomicBoolean());

        assertEquals(10, result.totalRows());
        assertEquals(List.of("nationkey", "name"), result.columns().stream().map(ColumnMetadata::name).toList());
        assertEquals("bigint", result.columns().get(0).type());
        assertTrue(result.message().contains("усечен"), result.message());
    }

    @Test
    void cancelAbortsQueryOnCoordinator() throws Exception {
        AtomicBoolean cancel = new AtomicBoolean();
        CompletableFuture<SqlEngine.ExecutionResult> future = CompletableFuture.supplyAsync(() ->
                engine.execute("q-cancel", "SELECT count(*) FROM tpch.sf1000.lineitem", "analyst", 10, e -> { }, cancel));
        Thread.sleep(1500);
        cancel.set(true);

        assertEquals("CANCELLED", future.get(30, TimeUnit.SECONDS).status());
    }

    @Test
    void sqlErrorIsReportedWithClusterName() {
        SqlEngineException e = assertThrows(SqlEngineException.class, () ->
                engine.execute("q-err", "SELECT * FROM no_such_table", "analyst", 10, ev -> { }, new AtomicBoolean()));
        assertTrue(e.getMessage().startsWith("Trino IT: "), e.getMessage());
    }
}
