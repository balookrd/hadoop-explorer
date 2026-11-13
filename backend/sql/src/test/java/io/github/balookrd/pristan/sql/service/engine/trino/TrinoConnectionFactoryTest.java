package io.github.balookrd.pristan.sql.service.engine.trino;

import io.github.balookrd.pristan.sql.config.SqlProperties;
import io.github.balookrd.pristan.sql.service.ClusterService;
import io.github.balookrd.pristan.sql.service.engine.SqlEngineException;
import io.github.balookrd.pristan.sql.service.engine.TrinoSqlEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrinoConnectionFactoryTest {

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
        c.setType("trino");
        c.getJdbc().setSsl(true);
        c.getKerberos().setEnabled(true);
        c.getKerberos().setPrincipal("svc_sql_explorer@COMPANY.LOCAL");
        c.getKerberos().setKeytab("/etc/security/keytabs/sql-explorer.keytab");
        return c;
    }

    private static SqlProperties.ClusterConfig cluster() {
        SqlProperties.ClusterConfig c = new SqlProperties.ClusterConfig();
        c.setId("trino");
        c.setName("Trino");
        c.setHost("trino-coordinator");
        c.setPort(8443);
        c.setCatalog("tpch");
        c.setSchema("sf1");
        return c;
    }

    @Test
    void urlContainsCatalogAndSchema() {
        assertEquals("jdbc:trino://trino-coordinator:8443/tpch/sf1", new TrinoConnectionFactory(cluster()).jdbcUrl());
    }

    @Test
    void urlWithoutCatalogHasNoPath() {
        SqlProperties.ClusterConfig c = cluster();
        c.setCatalog("");
        assertEquals("jdbc:trino://trino-coordinator:8443", new TrinoConnectionFactory(c).jdbcUrl());
    }

    @Test
    void impersonationSendsEndUserAsSessionUser() {
        Properties p = new TrinoConnectionFactory(cluster()).connectionProperties("analyst");
        assertEquals("hadoop-explorer", p.getProperty("user"));
        assertEquals("analyst", p.getProperty("sessionUser"));
        assertNull(p.getProperty("SSL"));
        assertNull(p.getProperty("KerberosPrincipal"));
    }

    @Test
    void connectTimeoutBoundsDriverHttpCalls() {
        assertEquals("30s", new TrinoConnectionFactory(cluster()).connectionProperties("analyst").getProperty("timeout"));

        SqlProperties.ClusterConfig c = cluster();
        c.getJdbc().setConnectTimeoutSeconds(5);
        assertEquals("5s", new TrinoConnectionFactory(c).connectionProperties("analyst").getProperty("timeout"));
    }

    @Test
    void disabledImpersonationRunsAsServiceUser() {
        SqlProperties.ClusterConfig c = cluster();
        c.getImpersonation().setEnabled(false);
        assertNull(new TrinoConnectionFactory(c).connectionProperties("analyst").getProperty("sessionUser"));
    }

    @Test
    void kerberosAndTlsPropertiesAreMapped() {
        SqlProperties.ClusterConfig c = cluster();
        c.getJdbc().setSsl(true);
        c.getJdbc().setSslVerification("CA");
        c.getJdbc().setSslTruststorePath("/etc/sql-explorer/trino-truststore.jks");
        c.getJdbc().setSslTruststorePassword("changeit");
        c.getKerberos().setEnabled(true);
        c.getKerberos().setPrincipal("svc_sql_explorer@COMPANY.LOCAL");
        c.getKerberos().setKeytab("/etc/security/keytabs/sql-explorer.keytab");
        c.getKerberos().setKrb5Conf("/etc/security/keytabs/krb5.conf");

        Properties p = new TrinoConnectionFactory(c).connectionProperties("analyst");

        assertEquals("svc_sql_explorer", p.getProperty("user"));
        assertEquals("analyst", p.getProperty("sessionUser"));
        assertEquals("true", p.getProperty("SSL"));
        assertEquals("CA", p.getProperty("SSLVerification"));
        assertEquals("/etc/sql-explorer/trino-truststore.jks", p.getProperty("SSLTrustStorePath"));
        assertEquals("changeit", p.getProperty("SSLTrustStorePassword"));
        assertEquals("trino", p.getProperty("KerberosRemoteServiceName"));
        assertEquals("svc_sql_explorer@COMPANY.LOCAL", p.getProperty("KerberosPrincipal"));
        assertEquals("/etc/security/keytabs/sql-explorer.keytab", p.getProperty("KerberosKeytabPath"));
        assertEquals("/etc/security/keytabs/krb5.conf", p.getProperty("KerberosConfigPath"));
        assertEquals("false", p.getProperty("KerberosUseCanonicalHostname"));
    }

    @Test
    void explicitServiceUserWins() {
        SqlProperties.ClusterConfig c = cluster();
        c.getKerberos().setEnabled(true);
        c.getJdbc().setSsl(true);
        c.getKerberos().setPrincipal("svc_sql_explorer/host.company.local@COMPANY.LOCAL");
        c.getKerberos().setKeytab("/etc/security/keytabs/sql-explorer.keytab");
        assertEquals("svc_sql_explorer", new TrinoConnectionFactory(c).connectionProperties("analyst").getProperty("user"));

        c.getJdbc().setServiceUser("svc_bi");
        assertEquals("svc_bi", new TrinoConnectionFactory(c).connectionProperties("analyst").getProperty("user"));
    }

    @Test
    void krb5ConfIsNotPassedByDefault() {
        Properties p = new TrinoConnectionFactory(kerberized()).connectionProperties("analyst");
        assertNull(p.getProperty("KerberosConfigPath"));
        assertEquals("trino", p.getProperty("KerberosRemoteServiceName"));
    }

    @Test
    void krb5ConfMatchingJvmPropertyIsPassed() {
        System.setProperty(KRB5_PROPERTY, "/etc/security/keytabs/krb5.conf");
        SqlProperties.ClusterConfig c = kerberized();
        c.getKerberos().setKrb5Conf("/etc/security/keytabs/krb5.conf");
        assertEquals("/etc/security/keytabs/krb5.conf",
                new TrinoConnectionFactory(c).connectionProperties("analyst").getProperty("KerberosConfigPath"));
    }

    @Test
    void krb5ConfDifferentFromJvmPropertyIsConfigurationError() {
        System.setProperty(KRB5_PROPERTY, "/etc/krb5.conf");
        SqlProperties.ClusterConfig c = kerberized();
        c.getKerberos().setKrb5Conf("/etc/security/keytabs/krb5.conf");

        SqlEngineException e = assertThrows(SqlEngineException.class, () -> new TrinoConnectionFactory(c));
        assertTrue(e.getMessage().contains("глобально для процесса"), e.getMessage());
    }

    @Test
    void clustersWithDifferentKrb5ConfFailAtStartup() {
        SqlProperties.ClusterConfig a = kerberized();
        a.setId("a");
        a.getKerberos().setKrb5Conf("/etc/a/krb5.conf");
        SqlProperties.ClusterConfig b = kerberized();
        b.setId("b");
        b.getKerberos().setKrb5Conf("/etc/b/krb5.conf");
        SqlProperties props = new SqlProperties();
        props.setClusters(List.of(a, b));

        SqlEngineException e = assertThrows(SqlEngineException.class, () -> new ClusterService(props));
        assertTrue(e.getMessage().contains("разные kerberos.krb5-conf"), e.getMessage());

        b.getKerberos().setKrb5Conf("/etc/a/krb5.conf");
        assertDoesNotThrow(() -> new ClusterService(props));
    }

    @Test
    void kerberosRequiresServiceNamePrincipalAndKeytab() {
        SqlProperties.ClusterConfig noService = kerberized();
        noService.getKerberos().setServiceName("");
        assertTrue(assertThrows(SqlEngineException.class, () -> new TrinoConnectionFactory(noService))
                .getMessage().contains("kerberos.service-name"));

        SqlProperties.ClusterConfig noKeytab = kerberized();
        noKeytab.getKerberos().setKeytab(null);
        assertTrue(assertThrows(SqlEngineException.class, () -> new TrinoConnectionFactory(noKeytab))
                .getMessage().contains("kerberos.keytab"));
    }

    @Test
    void krb5PropertyChangedAfterStartupIsSqlExceptionNotIllegalState() {
        SqlProperties.ClusterConfig c = kerberized();
        c.getKerberos().setKrb5Conf("/etc/security/keytabs/krb5.conf");
        TrinoConnectionFactory factory = new TrinoConnectionFactory(c);
        System.setProperty(KRB5_PROPERTY, "/etc/krb5.conf");

        SQLException e = assertThrows(SQLException.class, () -> factory.open("analyst"));
        assertTrue(e.getMessage().contains("глобально для процесса"), e.getMessage());
    }

    @Test
    void driverConnectFailureReachesCatalogCallerAsSqlEngineException() {
        // Goes through the real driver: trino-jdbc 484 rejects the missing krb5.conf inside connect()
        // ("Kerberos config file ... does not exist"); the catalog caller must see SqlEngineException (HTTP 502)
        SqlProperties.ClusterConfig c = kerberized();
        c.setName("Trino KRB");
        c.getKerberos().setKeytab("/nonexistent/sql-explorer.keytab");
        c.getKerberos().setKrb5Conf("/nonexistent/krb5.conf");

        SqlEngineException e = assertThrows(SqlEngineException.class, () -> new TrinoSqlEngine(c).getCatalogs("analyst"));
        assertTrue(e.getMessage().startsWith("Trino KRB: "), e.getMessage());
        assertFalse(e.getCause() instanceof IllegalStateException);
        assertNull(System.getProperty(KRB5_PROPERTY));
    }

    @Test
    void kerberosRequiresSsl() {
        SqlProperties.ClusterConfig c = kerberized();
        c.getJdbc().setSsl(false);
        assertTrue(assertThrows(SqlEngineException.class, () -> new TrinoConnectionFactory(c))
                .getMessage().contains("jdbc.ssl: true"));
    }

    @Test
    void relativeKrb5ConfIsRejected() {
        SqlProperties.ClusterConfig c = kerberized();
        c.getKerberos().setKrb5Conf("conf/krb5.conf");
        assertTrue(assertThrows(SqlEngineException.class, () -> new TrinoConnectionFactory(c))
                .getMessage().contains("абсолютным путем"));
    }
}
