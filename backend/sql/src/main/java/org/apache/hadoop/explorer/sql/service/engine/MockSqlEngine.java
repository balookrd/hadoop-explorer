package org.apache.hadoop.explorer.sql.service.engine;

import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.dto.ColumnMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class MockSqlEngine implements SqlEngine {

    private static final Logger log = LoggerFactory.getLogger(MockSqlEngine.class);

    private final SqlProperties.ClusterConfig cluster;
    private static final Map<String, Map<String, Map<String, List<ColumnMetadata>>>> CATALOG_DATA = new LinkedHashMap<>();

    static {
        // TPCH catalog
        Map<String, Map<String, List<ColumnMetadata>>> tpchSchemas = new LinkedHashMap<>();
        Map<String, List<ColumnMetadata>> tpchSf1 = new LinkedHashMap<>();
        tpchSf1.put("customer", List.of(
                new ColumnMetadata("custkey", "bigint"),
                new ColumnMetadata("name", "varchar(25)"),
                new ColumnMetadata("address", "varchar(40)"),
                new ColumnMetadata("nationkey", "bigint"),
                new ColumnMetadata("phone", "varchar(15)"),
                new ColumnMetadata("acctbal", "double"),
                new ColumnMetadata("mktsegment", "varchar(10)")
        ));
        tpchSf1.put("orders", List.of(
                new ColumnMetadata("orderkey", "bigint"),
                new ColumnMetadata("custkey", "bigint"),
                new ColumnMetadata("orderstatus", "varchar(1)"),
                new ColumnMetadata("totalprice", "double"),
                new ColumnMetadata("orderdate", "date"),
                new ColumnMetadata("orderpriority", "varchar(15)")
        ));
        tpchSchemas.put("sf1", tpchSf1);
        CATALOG_DATA.put("tpch", tpchSchemas);

        // Analytics catalog
        Map<String, Map<String, List<ColumnMetadata>>> analyticsSchemas = new LinkedHashMap<>();
        Map<String, List<ColumnMetadata>> analyticsEvents = new LinkedHashMap<>();
        analyticsEvents.put("user_actions", List.of(
                new ColumnMetadata("event_id", "varchar(64)"),
                new ColumnMetadata("user_id", "bigint"),
                new ColumnMetadata("event_type", "varchar(50)"),
                new ColumnMetadata("created_at", "timestamp"),
                new ColumnMetadata("ip_address", "varchar(45)")
        ));
        analyticsEvents.put("dau_metrics", List.of(
                new ColumnMetadata("report_date", "date"),
                new ColumnMetadata("platform", "varchar(20)"),
                new ColumnMetadata("active_users", "integer"),
                new ColumnMetadata("avg_session_sec", "double")
        ));
        analyticsSchemas.put("events", analyticsEvents);
        CATALOG_DATA.put("analytics", analyticsSchemas);
    }

    public MockSqlEngine(SqlProperties.ClusterConfig cluster) {
        this.cluster = cluster;
    }

    @Override
    public List<String> getCatalogs(String username) {
        return new ArrayList<>(CATALOG_DATA.keySet());
    }

    @Override
    public List<String> getSchemas(String username, String catalog) {
        var schemas = CATALOG_DATA.get(catalog != null ? catalog.toLowerCase() : "tpch");
        if (schemas != null) {
            return new ArrayList<>(schemas.keySet());
        }
        return List.of("default");
    }

    @Override
    public List<String> getTables(String username, String catalog, String schema) {
        var schemas = CATALOG_DATA.get(catalog != null ? catalog.toLowerCase() : "tpch");
        if (schemas != null) {
            var tables = schemas.get(schema != null ? schema.toLowerCase() : "sf1");
            if (tables != null) {
                return new ArrayList<>(tables.keySet());
            }
        }
        return List.of("sample_table");
    }

    @Override
    public List<ColumnMetadata> getColumns(String username, String catalog, String schema, String table) {
        var schemas = CATALOG_DATA.get(catalog != null ? catalog.toLowerCase() : "tpch");
        if (schemas != null) {
            var tables = schemas.get(schema != null ? schema.toLowerCase() : "sf1");
            if (tables != null) {
                var cols = tables.get(table != null ? table.toLowerCase() : "customer");
                if (cols != null) {
                    return cols;
                }
            }
        }
        return List.of(
                new ColumnMetadata("id", "bigint"),
                new ColumnMetadata("name", "varchar"),
                new ColumnMetadata("created_at", "timestamp")
        );
    }

    @Override
    public ExecutionResult execute(
            String queryId,
            String query,
            String username,
            int maxRows,
            Consumer<Map<String, Object>> eventConsumer,
            AtomicBoolean cancelled
    ) {
        long startTime = System.currentTimeMillis();
        log.info("[MOCK ENGINE] Выполнение запроса для пользователя {}: {}", username, query);

        eventConsumer.accept(Map.of(
                "type", "status",
                "status", "QUEUED",
                "message", "Запрос поставлен в очередь планировщика (" + cluster.getName() + ")..."
        ));

        sleepQuietly(100);
        if (cancelled.get()) {
            eventConsumer.accept(Map.of("type", "status", "status", "CANCELLED", "message", "Запрос отменен пользователем"));
            return new ExecutionResult(List.of(), List.of(), 0, System.currentTimeMillis() - startTime, "CANCELLED", "Запрос отменен");
        }

        eventConsumer.accept(Map.of(
                "type", "status",
                "status", "RUNNING",
                "message", "Исполнение под учетной записью " + username + "..."
        ));

        List<ColumnMetadata> columns = List.of(
                new ColumnMetadata("id", "bigint"),
                new ColumnMetadata("user_identity", "varchar"),
                new ColumnMetadata("cluster_node", "varchar"),
                new ColumnMetadata("metric_value", "double"),
                new ColumnMetadata("status", "varchar"),
                new ColumnMetadata("timestamp", "timestamp")
        );

        eventConsumer.accept(Map.of(
                "type", "columns",
                "columns", columns
        ));

        int targetRows = Math.min(Math.max(maxRows, 1), 150);
        List<List<Object>> allRows = new ArrayList<>();
        Random random = new Random();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        int batchSize = 30;
        int generated = 0;

        while (generated < targetRows) {
            if (cancelled.get()) {
                eventConsumer.accept(Map.of("type", "status", "status", "CANCELLED", "message", "Запрос отменен во время выборки данных"));
                return new ExecutionResult(columns, allRows, generated, System.currentTimeMillis() - startTime, "CANCELLED", "Запрос отменен пользователем");
            }
            sleepQuietly(60);

            List<List<Object>> batch = new ArrayList<>();
            int currentBatchSize = Math.min(batchSize, targetRows - generated);
            for (int i = 0; i < currentBatchSize; i++) {
                int rowIdx = generated + i + 1;
                batch.add(List.of(
                        (long) rowIdx,
                        username,
                        "node-" + (random.nextInt(16) + 1) + ".prod.corp",
                        Math.round((10.5 + random.nextDouble() * 999.0) * 100.0) / 100.0,
                        (rowIdx % 3 == 0) ? "SUCCESS" : (rowIdx % 3 == 1 ? "COMPLETED" : "CACHED"),
                        now.minusMinutes(rowIdx * 2L).format(dtf)
                ));
            }
            allRows.addAll(batch);
            generated += currentBatchSize;

            eventConsumer.accept(Map.of(
                    "type", "rows",
                    "rows", batch,
                    "total_rows", generated
            ));
        }

        double durationMs = (double) (System.currentTimeMillis() - startTime);
        eventConsumer.accept(Map.of(
                "type", "finished",
                "status", "FINISHED",
                "total_rows", generated,
                "execution_time_ms", durationMs,
                "message", "Запрос выполнен успешно на кластере " + cluster.getName() + ". Получено " + generated + " строк."
        ));

        return new ExecutionResult(columns, allRows, generated, durationMs, "FINISHED", "Успешно");
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
