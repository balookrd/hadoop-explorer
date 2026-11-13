package io.github.balookrd.pristan.sql.service.engine.hive;

import io.github.balookrd.pristan.sql.config.SqlProperties;
import io.github.balookrd.pristan.sql.service.ClusterService;
import io.github.balookrd.pristan.sql.service.engine.HiveSqlEngine;
import io.github.balookrd.pristan.sql.service.engine.SqlEngineException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HiveConnectionFactoryTest {

    private static final String KRB5_PROPERTY = "java.security.krb5.conf";
    private String savedKrb5;

    @BeforeEach
    void clearKrb5Property() {
        savedKrb5 = System.getProperty(KRB5_PROPERTY);
        System.clearProperty(KRB5_PROPERTY);
    }

    @AfterEach
    void restoreKrb5Property() {
        if (savedKrb5 == null) {
            System.clearProperty(KRB5_PROPERTY);
        } else {
            System.setProperty(KRB5_PROPERTY, savedKrb5);
        }
    }

    private static SqlProperties.ClusterConfig kerberized() {
        SqlProperties.ClusterConfig c = cluster();
        c.getKerberos().setEnabled(true);
        c.getKerberos().setPrincipal("svc_sql_explorer@COMPANY.LOCAL");
        c.getKerberos().setKeytab("/etc/security/keytabs/sql-explorer.keytab");
        c.getKerberos().setServerPrincipal("hive/_HOST@COMPANY.LOCAL");
        return c;
    }

    private static SqlProperties.ClusterConfig cluster() {
        SqlProperties.ClusterConfig c = new SqlProperties.ClusterConfig();
        c.setId("hive");
        c.setName("Hive");
        c.setType("hive");
        c.setHost("hive-server");
        c.setPort(10000);
        c.setSchema("demo_db");
        return c;
    }

    @Test
    void plainUrlWithProxyUser() {
        assertEquals("jdbc:hive2://hive-server:10000/demo_db;connectTimeout=30000;socketTimeout=300000;hive.server2.proxy.user=analyst",
                new HiveConnectionFactory(cluster()).jdbcUrl("analyst"));
    }

    @Test
    void kerberosUrlReplacesHostInServerPrincipal() {
        SqlProperties.ClusterConfig c = cluster();
        c.getKerberos().setEnabled(true);
        c.getKerberos().setPrincipal("svc_sql_explorer@COMPANY.LOCAL");
        c.getKerberos().setKeytab("/etc/security/keytabs/sql-explorer.keytab");
        c.getKerberos().setServerPrincipal("hive/_HOST@COMPANY.LOCAL");

        assertEquals("jdbc:hive2://hive-server:10000/demo_db"
                        + ";principal=hive/hive-server@COMPANY.LOCAL"
                        + ";kyuubiClientPrincipal=svc_sql_explorer@COMPANY.LOCAL"
                        + ";kyuubiClientKeytab=/etc/security/keytabs/sql-explorer.keytab"
                        + ";connectTimeout=30000;socketTimeout=300000;hive.server2.proxy.user=analyst",
                new HiveConnectionFactory(c).jdbcUrl("analyst"));
    }

    @Test
    void kerberosWithoutServerPrincipalIsAConfigurationError() {
        SqlProperties.ClusterConfig c = cluster();
        c.getKerberos().setEnabled(true);
        c.getKerberos().setPrincipal("svc_sql_explorer@COMPANY.LOCAL");
        c.getKerberos().setKeytab("/k.keytab");

        assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(c).jdbcUrl("analyst"));
    }

    @Test
    void disabledImpersonationOmitsProxyUser() {
        SqlProperties.ClusterConfig c = cluster();
        c.getImpersonation().setEnabled(false);
        assertEquals("jdbc:hive2://hive-server:10000/demo_db;connectTimeout=30000;socketTimeout=300000", new HiveConnectionFactory(c).jdbcUrl("analyst"));
    }

    @Test
    void usernameCannotInjectUrlParameters() {
        // ';' would start a new session variable, e.g. a different proxy user
        assertThrows(SqlEngineException.class,
                () -> new HiveConnectionFactory(cluster()).jdbcUrl("analyst;hive.server2.proxy.user=hive"));
    }

    @Test
    void serviceUserIsSentAsJdbcUser() {
        assertEquals("hadoop-explorer", new HiveConnectionFactory(cluster()).connectionProperties().getProperty("user"));
    }

    @Test
    void kerberosRequiresPrincipalAndKeytab() {
        // Without kyuubiClientKeytab the driver would log in from the process ticket cache
        SqlProperties.ClusterConfig noKeytab = kerberized();
        noKeytab.getKerberos().setKeytab(" ");
        SqlEngineException e = assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(noKeytab));
        assertTrue(e.getMessage().contains("kerberos.keytab"), e.getMessage());

        SqlProperties.ClusterConfig noPrincipal = kerberized();
        noPrincipal.getKerberos().setPrincipal(null);
        e = assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(noPrincipal));
        assertTrue(e.getMessage().contains("kerberos.principal"), e.getMessage());
    }

    @Test
    void sslIsRejectedBecauseOnlyBinaryTransportIsBuilt() {
        SqlProperties.ClusterConfig c = cluster();
        c.getJdbc().setSsl(true);
        SqlEngineException e = assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(c));
        assertTrue(e.getMessage().contains("jdbc.ssl"), e.getMessage());
    }

    @Test
    void krb5ConfMustEqualJvmPropertyBecauseDriverHasNoSuchOption() {
        SqlProperties.ClusterConfig c = kerberized();
        c.getKerberos().setKrb5Conf("/etc/security/keytabs/krb5.conf");

        SqlEngineException unset = assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(c));
        assertTrue(unset.getMessage().contains("(не задан)"), unset.getMessage());

        System.setProperty(KRB5_PROPERTY, "/etc/krb5.conf");
        SqlEngineException other = assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(c));
        assertTrue(other.getMessage().contains("-Djava.security.krb5.conf=/etc/krb5.conf"), other.getMessage());

        System.setProperty(KRB5_PROPERTY, "/etc/security/keytabs/krb5.conf");
        assertDoesNotThrow(() -> new HiveConnectionFactory(c));
    }

    @Test
    void relativeKrb5ConfIsRejected() {
        SqlProperties.ClusterConfig c = kerberized();
        c.getKerberos().setKrb5Conf("conf/krb5.conf");
        SqlEngineException e = assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(c));
        assertTrue(e.getMessage().contains("абсолютным путем"), e.getMessage());
    }

    @Test
    void trinoAndHiveClustersMustAgreeOnKrb5ConfAtStartup() {
        SqlProperties.ClusterConfig hive = kerberized();
        hive.setId("hive-a");
        hive.getKerberos().setKrb5Conf("/etc/a/krb5.conf");
        SqlProperties.ClusterConfig trino = new SqlProperties.ClusterConfig();
        trino.setId("trino-b");
        trino.setName("Trino");
        trino.setType("trino");
        trino.setHost("trino");
        trino.setPort(8443);
        trino.getJdbc().setSsl(true);
        trino.getKerberos().setEnabled(true);
        trino.getKerberos().setPrincipal("svc_sql_explorer@COMPANY.LOCAL");
        trino.getKerberos().setKeytab("/etc/security/keytabs/sql-explorer.keytab");
        trino.getKerberos().setKrb5Conf("/etc/b/krb5.conf");
        SqlProperties props = new SqlProperties();
        props.setClusters(List.of(hive, trino));

        // JVM property unset: trino-jdbc would set it to /etc/b on its first connect, Hive would then use /etc/b
        SqlEngineException unset = assertThrows(SqlEngineException.class, () -> new ClusterService(props));
        assertTrue(unset.getMessage().startsWith("Hive: kerberos.krb5-conf=/etc/a/krb5.conf"), unset.getMessage());

        System.setProperty(KRB5_PROPERTY, "/etc/a/krb5.conf");
        SqlEngineException differs = assertThrows(SqlEngineException.class, () -> new ClusterService(props));
        assertTrue(differs.getMessage().startsWith("Trino: kerberos.krb5-conf=/etc/b/krb5.conf"), differs.getMessage());

        trino.getKerberos().setKrb5Conf("/etc/a/krb5.conf");
        assertDoesNotThrow(() -> new ClusterService(props));
    }

    @Test
    void missingKeytabReachesCatalogCallerAsSqlEngineException() {
        // Goes through the real driver: Kyuubi checks the keytab before any network I/O and wraps the failure
        SqlProperties.ClusterConfig c = kerberized();
        c.setHost("127.0.0.1");
        c.getKerberos().setKeytab("/nonexistent/sql-explorer.keytab");

        SQLException direct = assertThrows(SQLException.class, () -> new HiveConnectionFactory(c).open("analyst"));
        assertTrue(direct.getMessage().contains("keytab does not exist"), direct.getMessage());
        SqlEngineException e = assertThrows(SqlEngineException.class, () -> new HiveSqlEngine(c).getSchemas("analyst", "default"));
        assertTrue(e.getMessage().startsWith("Hive: "), e.getMessage());
    }

    @Test
    void driverErrorsDoNotRevealClientKeytabAndPrincipal() throws Exception {
        // Kyuubi repeats the whole JDBC URL in "Could not open client transport with JDBC Uri: ..."
        java.nio.file.Path keytab = java.nio.file.Files.createTempFile("sql-explorer", ".keytab");
        try {
            SqlProperties.ClusterConfig c = kerberized();
            c.setHost("127.0.0.1");
            c.setPort(1);
            c.getJdbc().setConnectTimeoutSeconds(2);
            c.getKerberos().setKeytab(keytab.toString());

            SQLException direct = assertThrows(SQLException.class, () -> new HiveConnectionFactory(c).open("analyst"));
            assertNoClientCredentials(direct.getMessage(), keytab.toString());
            SqlEngineException e = assertThrows(SqlEngineException.class,
                    () -> new HiveSqlEngine(c).getSchemas("analyst", "default"));
            assertNoClientCredentials(e.getMessage(), keytab.toString());
        } finally {
            java.nio.file.Files.deleteIfExists(keytab);
        }
    }

    @Test
    void maskingKeepsTheRestOfTheUrl() {
        assertEquals("Could not open client transport with JDBC Uri: jdbc:hive2://h:10000/db;principal=hive/h@R;"
                        + "kyuubiClientPrincipal=***;kyuubiClientKeytab=***;connectTimeout=30000",
                HiveConnectionFactory.maskClientCredentials("Could not open client transport with JDBC Uri: "
                        + "jdbc:hive2://h:10000/db;principal=hive/h@R;kyuubiClientPrincipal=svc@R;"
                        + "kyuubiClientKeytab=/etc/security/keytabs/svc.keytab;connectTimeout=30000"));
    }

    private static void assertNoClientCredentials(String message, String keytab) {
        assertTrue(message != null && !message.contains(keytab), message);
        assertTrue(!message.contains("svc_sql_explorer@COMPANY.LOCAL"), message);
    }

    @Test
    void socketTimeoutIsConfigurableAndZeroDisablesIt() {
        SqlProperties.ClusterConfig c = cluster();
        c.getImpersonation().setEnabled(false);
        c.getJdbc().setSocketTimeoutSeconds(45);
        assertEquals("jdbc:hive2://hive-server:10000/demo_db;connectTimeout=30000;socketTimeout=45000",
                new HiveConnectionFactory(c).jdbcUrl("analyst"));

        c.getJdbc().setSocketTimeoutSeconds(0);
        assertEquals("jdbc:hive2://hive-server:10000/demo_db;connectTimeout=30000",
                new HiveConnectionFactory(c).jdbcUrl("analyst"));
    }

    @Test
    void urlDelimitersInConfiguredValuesAreRejected() {
        SqlProperties.ClusterConfig schema = cluster();
        schema.setSchema("demo_db;hive.server2.proxy.user=hive");
        SqlEngineException e = assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(schema));
        assertTrue(e.getMessage().contains("schema"), e.getMessage());

        SqlProperties.ClusterConfig host = cluster();
        host.setHost("hive-server?x=1");
        assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(host));

        SqlProperties.ClusterConfig keytab = kerberized();
        keytab.getKerberos().setKeytab("/k.keytab#v=1");
        e = assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(keytab));
        assertTrue(e.getMessage().contains("kerberos.keytab"), e.getMessage());

        SqlProperties.ClusterConfig principal = kerberized();
        principal.getKerberos().setPrincipal("svc;x=1@COMPANY.LOCAL");
        assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(principal));

        SqlProperties.ClusterConfig serverPrincipal = kerberized();
        serverPrincipal.getKerberos().setServerPrincipal("hive/_HOST@COMPANY.LOCAL;auth=noSasl");
        assertThrows(SqlEngineException.class, () -> new HiveConnectionFactory(serverPrincipal));
    }

    @Test
    void controlCharactersInRejectedUsernameAreMasked() {
        SqlEngineException e = assertThrows(SqlEngineException.class,
                () -> new HiveConnectionFactory(cluster()).jdbcUrl("bad\nINFO forged\u0007"));
        assertTrue(e.getMessage().contains("'bad?INFO forged?'"), e.getMessage());
    }

    @Test
    void trinoKrb5ConfWithKerberosHiveRequiresMatchingJvmProperty() {
        SqlProperties.ClusterConfig hive = kerberized();
        hive.setId("hive-a");
        SqlProperties.ClusterConfig trino = new SqlProperties.ClusterConfig();
        trino.setId("trino-b");
        trino.setName("Trino");
        trino.setType("trino");
        trino.setHost("trino");
        trino.setPort(8443);
        trino.getJdbc().setSsl(true);
        trino.getKerberos().setEnabled(true);
        trino.getKerberos().setPrincipal("svc_sql_explorer@COMPANY.LOCAL");
        trino.getKerberos().setKeytab("/etc/security/keytabs/sql-explorer.keytab");
        trino.getKerberos().setKrb5Conf("/etc/b/krb5.conf");
        SqlProperties props = new SqlProperties();
        props.setClusters(List.of(hive, trino));

        // Each cluster alone is valid: Trino accepts the path while the JVM property is unset, Hive has no krb5-conf
        SqlEngineException e = assertThrows(SqlEngineException.class, () -> new ClusterService(props));
        assertTrue(e.getMessage().startsWith("kerberos.krb5-conf=/etc/b/krb5.conf задан при Kerberos-кластере Hive"),
                e.getMessage());

        System.setProperty(KRB5_PROPERTY, "/etc/b/krb5.conf");
        assertDoesNotThrow(() -> new ClusterService(props));

        // Without a Kerberos Hive cluster the Trino-only rule of task 2 still applies
        System.clearProperty(KRB5_PROPERTY);
        hive.getKerberos().setEnabled(false);
        assertDoesNotThrow(() -> new ClusterService(props));
    }
}
