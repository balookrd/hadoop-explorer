package org.apache.hadoop.explorer.sql.service.engine.hive;

import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.service.engine.SqlEngineException;
import org.apache.hadoop.explorer.sql.service.engine.jdbc.ServiceUser;
import org.apache.kyuubi.jdbc.KyuubiHiveDriver;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Builds HiveServer2 connections with the Kyuubi JDBC driver. The service logs in with its keytab
 * (kyuubiClientPrincipal/kyuubiClientKeytab), the end user goes to hive.server2.proxy.user; HiveServer2
 * allows it when hadoop.proxyuser.<service>.hosts/groups permit the service account.
 */
public final class HiveConnectionFactory {

    /**
     * Kyuubi 1.12.0 has no krb5.conf option: its Krb5LoginModule login (refreshKrb5Config=true) re-reads the
     * JVM-wide Kerberos configuration from this system property, it never sets the property itself.
     */
    static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";

    // The user name is pasted into the URL: ';' or '=' would add session variables
    private static final Pattern CLIENT_CREDENTIALS = Pattern.compile("(kyuubiClientKeytab|kyuubiClientPrincipal)=[^;]*");
    private static final Pattern SAFE_USER = Pattern.compile("[A-Za-z0-9._@-]+");
    // Configured values pasted into the URL: ';' starts a session variable, '?' hive confs, '#' hive vars
    private static final Pattern URL_DELIMITERS = Pattern.compile("[;?#]");
    private static final Pattern CONTROL_CHARS = Pattern.compile("\\p{Cntrl}");

    private final SqlProperties.ClusterConfig cluster;
    // Direct driver instance: DriverManager would also see the H2 and Trino drivers
    private final Driver driver = new KyuubiHiveDriver();

    public HiveConnectionFactory(SqlProperties.ClusterConfig cluster) {
        validate(cluster);
        this.cluster = cluster;
    }

    /** Fails fast on settings the driver would silently ignore or turn into a ticket-cache login. */
    public static void validate(SqlProperties.ClusterConfig cluster) {
        // Only the binary transport without TLS is built here: jdbc.ssl would be ignored and traffic sent in clear
        if (cluster.getJdbc().isSsl()) {
            throw new SqlEngineException(cluster.getName()
                    + ": jdbc.ssl для type: hive не поддерживается (подключение к HiveServer2 идет по binary-транспорту без TLS)");
        }
        SqlProperties.KerberosConfig kerberos = cluster.getKerberos();
        requireNoUrlDelimiters(cluster, "host", cluster.getHost());
        requireNoUrlDelimiters(cluster, "schema", cluster.getSchema());
        if (!kerberos.isEnabled()) {
            return;
        }
        requireNoUrlDelimiters(cluster, "kerberos.principal", kerberos.getPrincipal());
        requireNoUrlDelimiters(cluster, "kerberos.keytab", kerberos.getKeytab());
        requireNoUrlDelimiters(cluster, "kerberos.server-principal", kerberos.getServerPrincipal());
        // Without a keytab the driver silently falls back to the process ticket cache (KRB5CCNAME)
        requireSet(cluster, "kerberos.principal", kerberos.getPrincipal());
        requireSet(cluster, "kerberos.keytab", kerberos.getKeytab());
        requireSet(cluster, "kerberos.server-principal", kerberos.getServerPrincipal());
        String krb5Conf = kerberos.getKrb5Conf();
        if (notBlank(krb5Conf) && !new File(krb5Conf).isAbsolute()) {
            throw new SqlEngineException(cluster.getName() + ": kerberos.krb5-conf должен быть абсолютным путем: " + krb5Conf);
        }
        String mismatch = krb5ConfMismatch(krb5Conf);
        if (mismatch != null) {
            throw new SqlEngineException(cluster.getName() + ": " + mismatch);
        }
    }

    /** Null when krb5Conf is unset or equals the JVM property; otherwise the error text. */
    private static String krb5ConfMismatch(String krb5Conf) {
        String jvm = System.getProperty(KRB5_CONF_PROPERTY);
        if (!notBlank(krb5Conf) || Objects.equals(jvm, krb5Conf)) {
            return null;
        }
        return "kerberos.krb5-conf=" + krb5Conf + " не совпадает с -D" + KRB5_CONF_PROPERTY + "="
                + (jvm == null ? "(не задан)" : jvm)
                + ": драйвер Kyuubi не принимает путь к krb5.conf и использует конфигурацию Kerberos JVM."
                + " Задайте -D" + KRB5_CONF_PROPERTY + " с тем же путем или уберите kerberos.krb5-conf";
    }

    private static void requireNoUrlDelimiters(SqlProperties.ClusterConfig cluster, String key, String value) {
        if (value != null && URL_DELIMITERS.matcher(value).find()) {
            throw new SqlEngineException(cluster.getName() + ": параметр " + key
                    + " не должен содержать символы ';', '?' и '#': они меняют разбор JDBC URL HiveServer2");
        }
    }

    private static void requireSet(SqlProperties.ClusterConfig cluster, String key, String value) {
        if (!notBlank(value)) {
            throw new SqlEngineException(cluster.getName() + ": при kerberos.enabled: true параметр " + key
                    + " обязателен" + ("kerberos.server-principal".equals(key) ? " (например hive/_HOST@REALM)" : ""));
        }
    }

    public String jdbcUrl(String username) {
        boolean impersonate = cluster.getImpersonation().isEnabled();
        if (impersonate && (username == null || !SAFE_USER.matcher(username).matches())) {
            // Control characters would forge log lines and break the error text
            String shown = username == null ? null : CONTROL_CHARS.matcher(username).replaceAll("?");
            throw new SqlEngineException(cluster.getName() + ": имя пользователя '" + shown
                    + "' недопустимо для hive.server2.proxy.user");
        }
        String database = cluster.getSchema() == null || cluster.getSchema().isBlank() ? "default" : cluster.getSchema();
        StringBuilder url = new StringBuilder("jdbc:hive2://")
                .append(cluster.getHost()).append(':').append(cluster.getPort()).append('/').append(database);
        SqlProperties.KerberosConfig kerberos = cluster.getKerberos();
        if (kerberos.isEnabled()) {
            url.append(";principal=").append(serverPrincipal());
            url.append(";kyuubiClientPrincipal=").append(kerberos.getPrincipal());
            url.append(";kyuubiClientKeytab=").append(kerberos.getKeytab());
        }
        // Kyuubi 1.12.0 reads both from the session variables (ms). connectTimeout bounds the TCP connect only;
        // socketTimeout bounds every Thrift read, so a HiveServer2 that hangs in the SASL handshake or
        // OpenSession fails instead of blocking forever. Long queries are not cut: ExecuteStatement runs
        // asynchronously and the driver polls GetOperationStatus, which HiveServer2 answers within
        // hive.server2.long.polling.timeout (5 s by default).
        url.append(";connectTimeout=").append(cluster.getJdbc().getConnectTimeoutSeconds() * 1000L);
        if (cluster.getJdbc().getSocketTimeoutSeconds() > 0) {
            url.append(";socketTimeout=").append(cluster.getJdbc().getSocketTimeoutSeconds() * 1000L);
        }
        if (impersonate) {
            url.append(";hive.server2.proxy.user=").append(username);
        }
        return url.toString();
    }

    /**
     * Kyuubi repeats the whole JDBC URL in its errors ("Could not open client transport with JDBC Uri: ...");
     * the client principal and keytab path are masked before the text reaches the API and query history.
     * The original exception stays attached as the cause, so the log keeps the full text.
     */
    static String maskClientCredentials(String message) {
        return message == null ? null : CLIENT_CREDENTIALS.matcher(message).replaceAll("$1=***");
    }

    public Properties connectionProperties() {
        Properties p = new Properties();
        p.setProperty("user", ServiceUser.resolve(cluster));
        return p;
    }

    public Connection open(String username) throws SQLException {
        if (cluster.getKerberos().isEnabled()) {
            // Another component may have changed the JVM property after startup validation
            String mismatch = krb5ConfMismatch(cluster.getKerberos().getKrb5Conf());
            if (mismatch != null) {
                throw new SQLException(mismatch);
            }
        }
        Connection connection;
        try {
            connection = driver.connect(jdbcUrl(username), connectionProperties());
        } catch (SQLException e) {
            throw new SQLException(maskClientCredentials(e.getMessage()), e.getSQLState(), e.getErrorCode(), e);
        } catch (RuntimeException e) {
            if (e instanceof SqlEngineException) {
                throw e;
            }
            // Keep driver runtime failures a SQLException so callers report SqlEngineException (HTTP 502)
            throw new SQLException(maskClientCredentials(e.getMessage()), e);
        }
        if (connection == null) {
            throw new SQLException("Kyuubi Hive JDBC driver rejected URL for " + cluster.getName());
        }
        return connection;
    }

    private String serverPrincipal() {
        return cluster.getKerberos().getServerPrincipal().replace("_HOST", cluster.getHost());
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }
}
