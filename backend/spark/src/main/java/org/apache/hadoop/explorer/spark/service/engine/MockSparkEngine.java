package org.apache.hadoop.explorer.spark.service.engine;

import org.apache.hadoop.explorer.spark.dto.ColumnMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class MockSparkEngine {

    private static final Logger log = LoggerFactory.getLogger(MockSparkEngine.class);

    private static final Map<String, List<ColumnMetadata>> TABLE_COLUMNS = new LinkedHashMap<>();
    private static final Map<String, List<List<Object>>> TABLE_ROWS = new LinkedHashMap<>();

    static {
        TABLE_COLUMNS.put("customers", List.of(
                new ColumnMetadata("cust_id", "bigint"),
                new ColumnMetadata("first_name", "string"),
                new ColumnMetadata("last_name", "string"),
                new ColumnMetadata("email", "string"),
                new ColumnMetadata("country", "string"),
                new ColumnMetadata("balance", "double")
        ));
        TABLE_ROWS.put("customers", List.of(
                List.of(101L, "Алексей", "Смирнов", "smirnov@corp.local", "RU", 15420.50),
                List.of(102L, "Мария", "Кузнецова", "kuznetsova@corp.local", "RU", 8940.00),
                List.of(103L, "John", "Doe", "jdoe@global.org", "US", 23150.75),
                List.of(104L, "Elena", "Popova", "popova@corp.local", "RU", 4200.10),
                List.of(105L, "Hans", "Müller", "mueller@euro.de", "DE", 18900.00),
                List.of(106L, "Анна", "Волкова", "volkova@corp.local", "RU", 31200.40),
                List.of(107L, "David", "Smith", "dsmith@global.org", "GB", 12500.00),
                List.of(108L, "Olga", "Sidorova", "sidorova@corp.local", "RU", 6700.80)
        ));

        TABLE_COLUMNS.put("transactions", List.of(
                new ColumnMetadata("tx_id", "string"),
                new ColumnMetadata("cust_id", "bigint"),
                new ColumnMetadata("amount", "double"),
                new ColumnMetadata("status", "string"),
                new ColumnMetadata("tx_time", "timestamp")
        ));
        TABLE_ROWS.put("transactions", List.of(
                List.of("tx-001", 101L, 1500.0, "SUCCESS", "2026-09-01 10:15:00"),
                List.of("tx-002", 102L, 340.5, "SUCCESS", "2026-09-01 11:20:12"),
                List.of("tx-003", 101L, 8900.0, "SUCCESS", "2026-09-02 09:05:44"),
                List.of("tx-004", 103L, 12000.0, "PENDING", "2026-09-03 14:40:00"),
                List.of("tx-005", 105L, 450.0, "SUCCESS", "2026-09-04 16:12:30"),
                List.of("tx-006", 106L, 990.0, "FAILED", "2026-09-05 08:30:15")
        ));
    }

    public record ExecutionResult(
            List<ColumnMetadata> columns,
            List<List<Object>> rows,
            long totalRows,
            String logs,
            double executionTimeMs,
            String status,
            String errorMessage
    ) {}

    public ExecutionResult executeCode(
            String executionId,
            String code,
            String language,
            String username,
            Consumer<Map<String, Object>> eventConsumer,
            AtomicBoolean cancelSignal
    ) {
        long startTime = System.currentTimeMillis();
        log.info("[MOCK SPARK ENGINE] Выполнение {} кода для {}: {}", language, username, code);

        eventConsumer.accept(Map.of(
                "type", "status",
                "status", "RUNNING",
                "progress", 0.1,
                "message", "Инициализация контекста SparkSession и компиляция DAG..."
        ));

        sleepQuietly(80);
        if (cancelSignal.get()) {
            eventConsumer.accept(Map.of("type", "finished", "status", "CANCELLED", "message", "Выполнение отменено пользователем"));
            return new ExecutionResult(List.of(), List.of(), 0, "Execution cancelled", System.currentTimeMillis() - startTime, "CANCELLED", "Отменено");
        }

        eventConsumer.accept(Map.of(
                "type", "log",
                "log", "INFO SparkContext: Starting job execution with 4 tasks\nINFO DAGScheduler: Submitting 2 missing stages"
        ));

        sleepQuietly(80);
        if (cancelSignal.get()) {
            eventConsumer.accept(Map.of("type", "finished", "status", "CANCELLED", "message", "Выполнение отменено во время расчета стадий"));
            return new ExecutionResult(List.of(), List.of(), 0, "Execution cancelled", System.currentTimeMillis() - startTime, "CANCELLED", "Отменено");
        }

        List<ColumnMetadata> cols;
        List<List<Object>> rows;
        String upper = code.toUpperCase();

        if (upper.contains("TRANSACTION") || upper.contains("TX_ID")) {
            cols = TABLE_COLUMNS.get("transactions");
            rows = TABLE_ROWS.get("transactions");
        } else {
            cols = TABLE_COLUMNS.get("customers");
            rows = TABLE_ROWS.get("customers");
        }

        double duration = (double) (System.currentTimeMillis() - startTime);
        String logs = "INFO SparkContext: Job finished successfully in " + duration + "ms\nStage 0: 2/2 tasks completed\nStage 1: 2/2 tasks completed";

        eventConsumer.accept(Map.of(
                "type", "finished",
                "status", "FINISHED",
                "progress", 1.0,
                "columns", cols,
                "rows", rows,
                "total_rows", rows.size(),
                "logs", logs,
                "execution_time_ms", duration
        ));

        return new ExecutionResult(cols, rows, rows.size(), logs, duration, "FINISHED", null);
    }

    public List<String> getDatabases() {
        return List.of("default", "analytics", "staging");
    }

    public List<String> getTables(String database) {
        return new ArrayList<>(TABLE_COLUMNS.keySet());
    }

    public List<ColumnMetadata> getColumns(String database, String table) {
        return TABLE_COLUMNS.getOrDefault(table.toLowerCase(), List.of(
                new ColumnMetadata("id", "bigint"),
                new ColumnMetadata("name", "string"),
                new ColumnMetadata("created_at", "timestamp")
        ));
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
