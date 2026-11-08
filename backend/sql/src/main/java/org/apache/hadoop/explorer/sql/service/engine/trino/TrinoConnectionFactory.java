package org.apache.hadoop.explorer.sql.service.engine.trino;

import io.trino.jdbc.TrinoDriver;
import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.service.engine.SqlEngineException;
import org.apache.hadoop.explorer.sql.service.engine.jdbc.ServiceUser;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/**
 * Builds Trino JDBC connections. The service authenticates as ServiceUser (Kerberos keytab when enabled),
 * the end user goes to sessionUser (X-Trino-User); Trino checks the impersonation rule
 * service user -> end user in its access control.
 */
public final class TrinoConnectionFactory {

    /**
     * trino-jdbc 484 writes KerberosConfigPath into this JVM-wide system property on every connect and refuses
     * (IllegalStateException) when it is already set to another path.
     */
    static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";

    private final SqlProperties.ClusterConfig cluster;
    // Direct driver instance: DriverManager would also see the H2 driver and pick by URL prefix
    private final Driver driver = new TrinoDriver();

    public TrinoConnectionFactory(SqlProperties.ClusterConfig cluster) {
        validate(cluster);
        this.cluster = cluster;
    }

    /**
     * Fails fast on a Kerberos setup the driver would reject or silently ignore. Agreement of krb5.conf
     * across clusters is checked by EngineConfigValidator.
     */
    public static void validate(SqlProperties.ClusterConfig cluster) {
        SqlProperties.KerberosConfig kerberos = cluster.getKerberos();
        if (!kerberos.isEnabled()) {
            return;
        }
        // Without KerberosRemoteServiceName the driver does not enable SPNEGO at all and sends no ticket
        requireSet(cluster, "kerberos.service-name", kerberos.getServiceName());
        requireSet(cluster, "kerberos.principal", kerberos.getPrincipal());
        requireSet(cluster, "kerberos.keytab", kerberos.getKeytab());
        // The driver refuses Kerberos over plain HTTP on every connect ("TLS/SSL is required for Kerberos authentication")
        if (!cluster.getJdbc().isSsl()) {
            throw new SqlEngineException(cluster.getName() + ": при kerberos.enabled: true требуется jdbc.ssl: true");
        }
        // The driver compares File.getAbsolutePath() of the value with the raw JVM property: a relative path
        // would pass the first connect (property unset) and then be refused by every later one
        String krb5Conf = kerberos.getKrb5Conf();
        if (notBlank(krb5Conf) && !new File(krb5Conf).isAbsolute()) {
            throw new SqlEngineException(cluster.getName() + ": kerberos.krb5-conf должен быть абсолютным путем: " + krb5Conf);
        }
        String mismatch = krb5ConfMismatch(kerberos.getKrb5Conf());
        if (mismatch != null) {
            throw new SqlEngineException(cluster.getName() + ": " + mismatch);
        }
    }

    /** Null when krb5Conf is unset or matches the JVM property; otherwise the error text. */
    private static String krb5ConfMismatch(String krb5Conf) {
        String jvm = System.getProperty(KRB5_CONF_PROPERTY);
        if (!notBlank(krb5Conf) || jvm == null || Objects.equals(jvm, krb5Conf)) {
            return null;
        }
        return "kerberos.krb5-conf=" + krb5Conf + " не совпадает с -D" + KRB5_CONF_PROPERTY + "=" + jvm
                + ": trino-jdbc устанавливает этот параметр глобально для процесса. Укажите тот же путь или уберите kerberos.krb5-conf";
    }

    private static void requireSet(SqlProperties.ClusterConfig cluster, String key, String value) {
        if (!notBlank(value)) {
            throw new SqlEngineException(cluster.getName() + ": при kerberos.enabled: true параметр " + key + " обязателен");
        }
    }

    public String jdbcUrl() {
        StringBuilder url = new StringBuilder("jdbc:trino://").append(cluster.getHost()).append(':').append(cluster.getPort());
        if (notBlank(cluster.getCatalog())) {
            url.append('/').append(cluster.getCatalog());
            if (notBlank(cluster.getSchema())) {
                url.append('/').append(cluster.getSchema());
            }
        }
        return url.toString();
    }

    public Properties connectionProperties(String username) {
        SqlProperties.JdbcConfig jdbc = cluster.getJdbc();
        SqlProperties.KerberosConfig kerberos = cluster.getKerberos();
        Properties p = new Properties();
        p.setProperty("user", ServiceUser.resolve(cluster));
        if (cluster.getImpersonation().isEnabled()) {
            p.setProperty("sessionUser", username);
        }
        p.setProperty("source", "hadoop-explorer-sql");
        // trino-jdbc 484: connect/read/write timeout of every HTTP call to the coordinator and the retry budget
        // of a failed call. Metadata calls have no watchdog, so this bounds them when the coordinator hangs.
        p.setProperty("timeout", jdbc.getConnectTimeoutSeconds() + "s");
        if (jdbc.isSsl()) {
            p.setProperty("SSL", "true");
            p.setProperty("SSLVerification", jdbc.getSslVerification());
            if (notBlank(jdbc.getSslTruststorePath())) {
                p.setProperty("SSLTrustStorePath", jdbc.getSslTruststorePath());
                if (jdbc.getSslTruststorePassword() != null) {
                    p.setProperty("SSLTrustStorePassword", jdbc.getSslTruststorePassword());
                }
            }
        }
        if (kerberos.isEnabled()) {
            // service-name, principal and keytab are mandatory (validate); krb5.conf is optional because the driver
            // turns KerberosConfigPath into the JVM-wide java.security.krb5.conf
            p.setProperty("KerberosRemoteServiceName", kerberos.getServiceName());
            p.setProperty("KerberosPrincipal", kerberos.getPrincipal());
            p.setProperty("KerberosKeytabPath", kerberos.getKeytab());
            if (notBlank(kerberos.getKrb5Conf())) {
                p.setProperty("KerberosConfigPath", kerberos.getKrb5Conf());
            }
            // SPN is built from the configured host (trino/<host>@REALM), not from reverse DNS
            p.setProperty("KerberosUseCanonicalHostname", "false");
        }
        return p;
    }

    public Connection open(String username) throws SQLException {
        if (cluster.getKerberos().isEnabled()) {
            // Another component may have set the JVM property after startup validation
            String mismatch = krb5ConfMismatch(cluster.getKerberos().getKrb5Conf());
            if (mismatch != null) {
                throw new SQLException(mismatch);
            }
        }
        Connection connection;
        try {
            connection = driver.connect(jdbcUrl(), connectionProperties(username));
        } catch (RuntimeException e) {
            // e.g. IllegalStateException "Refusing to set system property 'java.security.krb5.conf'": keep it a
            // SQLException so callers report SqlEngineException (HTTP 502), not an unhandled 500
            throw new SQLException(e.getMessage(), e);
        }
        if (connection == null) {
            throw new SQLException("Trino JDBC driver rejected URL " + jdbcUrl());
        }
        return connection;
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }
}
