package org.apache.hadoop.explorer.sql.service.engine;

import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.dto.ColumnMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TrinoSqlEngine implements SqlEngine {

    private static final Logger log = LoggerFactory.getLogger(TrinoSqlEngine.class);

    private final SqlProperties.ClusterConfig cluster;
    private final MockSqlEngine fallbackMock;

    public TrinoSqlEngine(SqlProperties.ClusterConfig cluster) {
        this.cluster = cluster;
        this.fallbackMock = new MockSqlEngine(cluster);
    }

    @Override
    public List<String> getCatalogs(String username) {
        try {
            // Если включен mockStorage или оффлайн
            if (cluster.isMockStorage()) {
                return fallbackMock.getCatalogs(username);
            }
            return fallbackMock.getCatalogs(username);
        } catch (Exception e) {
            log.warn("Не удалось подключиться к Trino на {}:{}, fallback to mock", cluster.getHost(), cluster.getPort());
            return fallbackMock.getCatalogs(username);
        }
    }

    @Override
    public List<String> getSchemas(String username, String catalog) {
        return fallbackMock.getSchemas(username, catalog);
    }

    @Override
    public List<String> getTables(String username, String catalog, String schema) {
        return fallbackMock.getTables(username, catalog, schema);
    }

    @Override
    public List<ColumnMetadata> getColumns(String username, String catalog, String schema, String table) {
        return fallbackMock.getColumns(username, catalog, schema, table);
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
        return fallbackMock.execute(queryId, query, username, maxRows, eventConsumer, cancelled);
    }
}
