package io.github.balookrd.pristan.tolmach.service.engine.jdbc;

import io.github.balookrd.pristan.tolmach.config.SqlProperties;
import io.github.balookrd.pristan.tolmach.dto.ColumnMetadata;
import io.github.balookrd.pristan.tolmach.service.engine.SqlEngine;
import io.github.balookrd.pristan.tolmach.service.engine.SqlEngineException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Engine over a JDBC driver: metadata through DatabaseMetaData, queries through JdbcQueryRunner.
 * One connection per call: impersonation is a connection property, so connections are per user.
 */
public abstract class JdbcSqlEngine implements SqlEngine {

    @FunctionalInterface
    private interface MetadataQuery {
        ResultSet run(DatabaseMetaData metaData) throws SQLException;
    }

    protected final SqlProperties.ClusterConfig cluster;
    private final JdbcQueryRunner runner;

    protected JdbcSqlEngine(SqlProperties.ClusterConfig cluster, JdbcQueryRunner.StatementCustomizer customizer) {
        this.cluster = cluster;
        this.runner = new JdbcQueryRunner(cluster.getName(), this::openConnection,
                Duration.ofSeconds(cluster.getJdbc().getQueryTimeoutSeconds()),
                cluster.getJdbc().getFetchSize(), customizer);
    }

    protected abstract Connection openConnection(String username) throws SQLException;

    /** Catalog argument for DatabaseMetaData calls; HiveServer2 has no catalogs and expects null. */
    protected String metadataCatalog(String catalog) {
        return catalog;
    }

    @Override
    public List<String> getCatalogs(String username) {
        return readNames(username, DatabaseMetaData::getCatalogs, "TABLE_CAT");
    }

    @Override
    public List<String> getSchemas(String username, String catalog) {
        return readNames(username, md -> md.getSchemas(metadataCatalog(catalog), null), "TABLE_SCHEM");
    }

    @Override
    public List<String> getTables(String username, String catalog, String schema) {
        return readNames(username,
                md -> md.getTables(metadataCatalog(catalog), escape(md, schema), "%", null), "TABLE_NAME");
    }

    @Override
    public List<ColumnMetadata> getColumns(String username, String catalog, String schema, String table) {
        try (Connection connection = openConnection(username)) {
            DatabaseMetaData md = connection.getMetaData();
            try (ResultSet rs = md.getColumns(metadataCatalog(catalog), escape(md, schema), escape(md, table), "%")) {
                List<ColumnMetadata> columns = new ArrayList<>();
                while (rs.next()) {
                    columns.add(new ColumnMetadata(rs.getString("COLUMN_NAME"),
                            JdbcValueMapper.typeName(rs.getString("TYPE_NAME"))));
                }
                return columns;
            }
        } catch (SQLException e) {
            throw new SqlEngineException(cluster.getName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public ExecutionResult execute(String queryId, String query, String username, int maxRows,
                                   Consumer<Map<String, Object>> eventConsumer, AtomicBoolean cancelled) {
        return runner.run(queryId, query, username, maxRows, eventConsumer, cancelled);
    }

    private List<String> readNames(String username, MetadataQuery query, String column) {
        try (Connection connection = openConnection(username);
             ResultSet rs = query.run(connection.getMetaData())) {
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                names.add(rs.getString(column));
            }
            return names;
        } catch (SQLException e) {
            throw new SqlEngineException(cluster.getName() + ": " + e.getMessage(), e);
        }
    }

    /** Escapes LIKE wildcards: "_" in schema/table names must match literally. */
    private static String escape(DatabaseMetaData md, String value) throws SQLException {
        if (value == null) {
            return null;
        }
        String escape = md.getSearchStringEscape();
        if (escape == null || escape.isEmpty()) {
            return value;
        }
        return value.replace(escape, escape + escape).replace("_", escape + "_").replace("%", escape + "%");
    }
}
