package org.apache.hadoop.explorer.sql.service.engine;

import org.apache.hadoop.explorer.sql.dto.ColumnMetadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public interface SqlEngine {

    List<String> getCatalogs(String username);

    List<String> getSchemas(String username, String catalog);

    List<String> getTables(String username, String catalog, String schema);

    List<ColumnMetadata> getColumns(String username, String catalog, String schema, String table);

    ExecutionResult execute(
            String queryId,
            String query,
            String username,
            int maxRows,
            Consumer<Map<String, Object>> eventConsumer,
            AtomicBoolean cancelled
    );

    record ExecutionResult(
            List<ColumnMetadata> columns,
            List<List<Object>> rows,
            long totalRows,
            double executionTimeMs,
            String status,
            String message
    ) {}
}
