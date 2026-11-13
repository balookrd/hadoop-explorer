package io.github.balookrd.pristan.spark.service;

import io.github.balookrd.pristan.spark.config.SparkProperties;
import io.github.balookrd.pristan.spark.dto.ColumnMetadata;
import io.github.balookrd.pristan.spark.model.SparkSessionRecord;
import io.github.balookrd.pristan.spark.repository.SparkSessionRepository;
import io.github.balookrd.pristan.spark.service.engine.SparkEngine;
import io.github.balookrd.pristan.spark.service.engine.SparkEngineRegistry;
import io.github.balookrd.pristan.spark.service.engine.livy.LivyException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Catalog tree. Emulator clusters answer from MockSparkEngine; Livy clusters run SHOW/DESCRIBE as
 * kind=sql statements in the user's active session, so the user's own metastore permissions apply.
 */
@Service
public class SparkCatalogService {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");
    private static final List<String> ACTIVE_STATES = List.of("idle", "busy");

    private final SparkEngineRegistry engines;
    private final SparkSessionRepository sessionRepository;
    private final Map<String, Object> cache = new ConcurrentHashMap<>();

    public SparkCatalogService(SparkEngineRegistry engines, SparkSessionRepository sessionRepository) {
        this.engines = engines;
        this.sessionRepository = sessionRepository;
    }

    public List<String> getDatabases(SparkProperties.SparkClusterConfig cluster, String username, String metastoreId, boolean refresh) {
        return cached(cluster.getId() + ":" + username + ":" + metastoreId + ":dbs", refresh, () -> {
            if (!engines.forCluster(cluster).remote()) {
                return engines.mock().getDatabases();
            }
            return column(runInUserSession(cluster, username, "SHOW DATABASES"), "namespace");
        });
    }

    public List<String> getTables(SparkProperties.SparkClusterConfig cluster, String username, String database, String metastoreId, boolean refresh) {
        return cached(cluster.getId() + ":" + username + ":" + database + ":" + metastoreId + ":tables", refresh, () -> {
            if (!engines.forCluster(cluster).remote()) {
                return engines.mock().getTables(database);
            }
            return column(runInUserSession(cluster, username, "SHOW TABLES IN `" + identifier(database) + "`"), "tableName");
        });
    }

    public List<ColumnMetadata> getColumns(SparkProperties.SparkClusterConfig cluster, String username, String database, String table,
                                           String metastoreId, boolean refresh) {
        return cached(cluster.getId() + ":" + username + ":" + database + ":" + table + ":" + metastoreId + ":cols", refresh, () -> {
            if (!engines.forCluster(cluster).remote()) {
                return engines.mock().getColumns(database, table);
            }
            SparkEngine.ExecutionResult result = runInUserSession(cluster, username,
                    "DESCRIBE `" + identifier(database) + "`.`" + identifier(table) + "`");
            List<ColumnMetadata> columns = new ArrayList<>();
            for (List<Object> row : result.rows()) {
                String name = row.get(0) == null ? "" : row.get(0).toString();
                if (name.isBlank() || name.startsWith("#")) {
                    break; // "# Partition Information" and later sections repeat columns
                }
                columns.add(new ColumnMetadata(name, row.size() > 1 && row.get(1) != null ? row.get(1).toString() : ""));
            }
            return columns;
        });
    }

    private SparkEngine.ExecutionResult runInUserSession(SparkProperties.SparkClusterConfig cluster, String username, String sql) {
        SparkSessionRecord session = sessionRepository
                .findFirstByUsernameAndClusterIdAndStatusInOrderByLastActivityAtDesc(username, cluster.getId(), ACTIVE_STATES)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.CONFLICT,
                        "Для просмотра каталога запустите сессию Spark на кластере " + cluster.getName()));
        SparkEngine.ExecutionResult result;
        try {
            result = engines.forCluster(cluster).execute(session.getLivySessionId(),
                    "catalog-" + UUID.randomUUID(), sql, "sql", username, event -> { }, new AtomicBoolean());
        } catch (LivyException e) {
            if (e.getStatus() != 409) {
                throw e; // 502/504 from SparkEngineExceptionHandler
            }
            // The session is dead in Livy (LivySparkEngine#waitUntilReady): without this the tree would keep
            // picking the same session and failing
            sessionRepository.updateStateIfActive(session.getId(), "dead", session.getYarnApplicationId(), Instant.now());
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Сессия Spark на кластере " + cluster.getName() + " завершилась, для просмотра каталога запустите новую");
        }
        if (!"FINISHED".equals(result.status())) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Spark: " + result.errorMessage());
        }
        return result;
    }

    private static List<String> column(SparkEngine.ExecutionResult result, String name) {
        int index = 0;
        for (int i = 0; i < result.columns().size(); i++) {
            if (result.columns().get(i).name().equals(name)) {
                index = i;
            }
        }
        List<String> values = new ArrayList<>();
        for (List<Object> row : result.rows()) {
            values.add(String.valueOf(row.get(index)));
        }
        return values;
    }

    private static String identifier(String value) {
        if (value == null || !IDENTIFIER.matcher(value).matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Недопустимое имя базы или таблицы: " + value);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private <T> T cached(String key, boolean refresh, Supplier<T> loader) {
        if (!refresh && cache.containsKey(key)) {
            return (T) cache.get(key);
        }
        T value = loader.get();
        cache.put(key, value);
        return value;
    }
}
