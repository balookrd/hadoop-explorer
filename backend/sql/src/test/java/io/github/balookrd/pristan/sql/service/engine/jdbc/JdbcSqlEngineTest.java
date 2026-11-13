package io.github.balookrd.pristan.sql.service.engine.jdbc;

import io.github.balookrd.pristan.sql.config.SqlProperties;
import io.github.balookrd.pristan.sql.dto.ColumnMetadata;
import io.github.balookrd.pristan.sql.service.engine.SqlEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JdbcSqlEngineTest {

    private static final String URL = "jdbc:h2:mem:engine;DB_CLOSE_DELAY=-1";

    static final class H2SqlEngine extends JdbcSqlEngine {
        H2SqlEngine(SqlProperties.ClusterConfig cluster) {
            super(cluster, JdbcQueryRunner.StatementCustomizer.NONE);
        }

        @Override
        protected Connection openConnection(String username) throws SQLException {
            return DriverManager.getConnection(URL, "sa", "");
        }
    }

    private static H2SqlEngine engine;

    @BeforeAll
    static void createSchema() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL, "sa", ""); Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS SALES");
            s.execute("CREATE TABLE IF NOT EXISTS SALES.ORDERS (ID BIGINT, AMOUNT DECIMAL(10,2))");
            s.execute("CREATE TABLE IF NOT EXISTS SALES.ORDER_ITEMS (ID BIGINT)");
            // Matches the unescaped LIKE pattern ORDER_ITEMS ("_" = any char) but is a different table
            s.execute("CREATE TABLE IF NOT EXISTS SALES.ORDERSITEMS (ID BIGINT, NOTE VARCHAR(10))");
        }
        SqlProperties.ClusterConfig cluster = new SqlProperties.ClusterConfig();
        cluster.setId("h2");
        cluster.setName("H2 test");
        engine = new H2SqlEngine(cluster);
    }

    @Test
    void readsCatalogSchemaTableAndColumns() {
        assertTrue(engine.getCatalogs("analyst").contains("ENGINE"));
        assertTrue(engine.getSchemas("analyst", "ENGINE").contains("SALES"));
        assertEquals(java.util.List.of("ORDERS", "ORDERSITEMS", "ORDER_ITEMS"), engine.getTables("analyst", "ENGINE", "SALES"));
        assertEquals(new ColumnMetadata("ID", "bigint"), engine.getColumns("analyst", "ENGINE", "SALES", "ORDERS").get(0));
    }

    @Test
    void underscoreInTableNameIsNotAWildcard() {
        // Without escaping the pattern would also describe ORDERSITEMS (3 columns instead of 1)
        assertEquals(1, engine.getColumns("analyst", "ENGINE", "SALES", "ORDER_ITEMS").size());
    }

    @Test
    void executeDelegatesToRunner() {
        SqlEngine.ExecutionResult result = engine.execute("q1", "SELECT COUNT(*) FROM SALES.ORDERS", "analyst",
                100, e -> { }, new AtomicBoolean());
        assertEquals("FINISHED", result.status());
        assertEquals(1, result.totalRows());
    }

    @Test
    void missingTypeNameInColumnMetadataBecomesUnknown() throws SQLException {
        java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("COLUMN_NAME")).thenReturn("c");
        when(rs.getString("TYPE_NAME")).thenReturn(null);
        java.sql.DatabaseMetaData md = mock(java.sql.DatabaseMetaData.class);
        when(md.getSearchStringEscape()).thenReturn("\\");
        when(md.getColumns(any(), any(), any(), any())).thenReturn(rs);
        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(md);
        SqlProperties.ClusterConfig cluster = new SqlProperties.ClusterConfig();
        cluster.setName("mocked");
        JdbcSqlEngine mocked = new JdbcSqlEngine(cluster, JdbcQueryRunner.StatementCustomizer.NONE) {
            @Override
            protected Connection openConnection(String username) {
                return connection;
            }
        };

        assertEquals(java.util.List.of(new ColumnMetadata("c", "unknown")),
                mocked.getColumns("analyst", "c1", "s1", "t1"));
    }
}
