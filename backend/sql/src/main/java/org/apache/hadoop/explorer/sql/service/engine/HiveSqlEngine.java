package org.apache.hadoop.explorer.sql.service.engine;

import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.service.engine.hive.HiveConnectionFactory;
import org.apache.hadoop.explorer.sql.service.engine.jdbc.JdbcQueryRunner;
import org.apache.hadoop.explorer.sql.service.engine.jdbc.JdbcSqlEngine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** HiveServer2 over the Kyuubi JDBC driver; used when the cluster has type: hive and mock-storage: false. */
public class HiveSqlEngine extends JdbcSqlEngine {

    private final HiveConnectionFactory connections;

    public HiveSqlEngine(SqlProperties.ClusterConfig cluster) {
        super(cluster, JdbcQueryRunner.StatementCustomizer.NONE);
        this.connections = new HiveConnectionFactory(cluster);
    }

    @Override
    protected Connection openConnection(String username) throws SQLException {
        return connections.open(username);
    }

    /** HiveServer2 has no catalogs; the UI tree needs one root entry. */
    @Override
    public List<String> getCatalogs(String username) {
        return List.of("default");
    }

    @Override
    protected String metadataCatalog(String catalog) {
        return null;
    }
}
