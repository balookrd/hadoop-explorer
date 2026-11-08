package org.apache.hadoop.explorer.sql.service.engine;

import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.service.engine.hive.HiveConnectionFactory;
import org.apache.hadoop.explorer.sql.service.engine.trino.TrinoConnectionFactory;

import java.util.List;
import java.util.stream.Collectors;

/** Startup check of the real-engine clusters (mock-storage: false), so a bad setup fails before the first query. */
public final class EngineConfigValidator {

    private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";

    private EngineConfigValidator() {
    }

    /**
     * Validates every Trino and Hive cluster and requires one krb5.conf for all Kerberos clusters: the JDK keeps
     * a single Kerberos configuration per process (trino-jdbc sets java.security.krb5.conf, the Kyuubi driver
     * re-reads it on every keytab login).
     * <p>
     * The "different krb5-conf" check is reachable only while -Djava.security.krb5.conf is unset and only for
     * Trino clusters: with the JVM property set, each cluster's own validate() already rejects a differing path,
     * and a Hive cluster never accepts krb5-conf without the JVM property.
     * <p>
     * With a Kerberos Hive cluster, a krb5-conf on any Kerberos cluster also requires the JVM property to equal
     * it: otherwise Hive would log in with the JDK default krb5.conf before the first Trino connection and with
     * the Trino path after it.
     */
    public static void validateAll(List<SqlProperties.ClusterConfig> clusters) {
        List<SqlProperties.ClusterConfig> real = clusters.stream().filter(c -> !c.isMockStorage()).toList();
        for (SqlProperties.ClusterConfig cluster : real) {
            switch (String.valueOf(cluster.getType()).toLowerCase()) {
                case "trino" -> TrinoConnectionFactory.validate(cluster);
                case "hive" -> HiveConnectionFactory.validate(cluster);
                default -> { }
            }
        }
        List<SqlProperties.ClusterConfig> kerberized = real.stream()
                .filter(c -> isJdbcEngine(c) && c.getKerberos().isEnabled())
                .toList();
        List<String> paths = kerberized.stream().map(c -> c.getKerberos().getKrb5Conf())
                .filter(p -> p != null && !p.isBlank()).distinct().toList();
        if (paths.size() > 1) {
            throw new SqlEngineException("Кластеры с Kerberos задают разные kerberos.krb5-conf ("
                    + String.join(", ", paths) + "): конфигурация Kerberos JDK (java.security.krb5.conf) одна на процесс,"
                    + " допустим один путь для всех кластеров Trino и Hive. Кластеры: "
                    + kerberized.stream().map(SqlProperties.ClusterConfig::getId).collect(Collectors.joining(", ")));
        }
        boolean kerberizedHive = kerberized.stream().anyMatch(c -> "hive".equalsIgnoreCase(c.getType()));
        String jvm = System.getProperty(KRB5_CONF_PROPERTY);
        if (kerberizedHive && paths.size() == 1 && !paths.get(0).equals(jvm)) {
            throw new SqlEngineException("kerberos.krb5-conf=" + paths.get(0) + " задан при Kerberos-кластере Hive, а -D"
                    + KRB5_CONF_PROPERTY + "=" + (jvm == null ? "(не задан)" : jvm)
                    + ": драйвер Kyuubi читает только свойство JVM, и до первого соединения trino-jdbc Hive использовал бы"
                    + " другой krb5.conf. Задайте -D" + KRB5_CONF_PROPERTY + "=" + paths.get(0) + ". Кластеры: "
                    + kerberized.stream().map(SqlProperties.ClusterConfig::getId).collect(Collectors.joining(", ")));
        }
    }

    private static boolean isJdbcEngine(SqlProperties.ClusterConfig cluster) {
        return "trino".equalsIgnoreCase(cluster.getType()) || "hive".equalsIgnoreCase(cluster.getType());
    }
}
