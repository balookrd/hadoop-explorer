package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class DefaultKafkaAdminClientFactory implements KafkaAdminClientFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaAdminClientFactory.class);

    private static final Set<String> PROTOCOLS = Set.of("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL");

    /**
     * Топология кластеров: из нее берутся протокол и truststore. Клиент знал только
     * SASL_PLAINTEXT, и к брокерам с TLS (SASL_SSL) оркестратор не подключался вовсе.
     */
    private TopologyRegistry topologyRegistry;

    @Autowired(required = false)
    public void setTopologyRegistry(TopologyRegistry topologyRegistry) {
        this.topologyRegistry = topologyRegistry;
    }

    @Override
    public AdminClient createAdminClient(String bootstrapServers,
                                  boolean kerberosEnabled,
                                  String principal,
                                  String keytabPath,
                                  String serviceName) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers != null ? bootstrapServers : "localhost:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "8000");

        if (kerberosEnabled && (principal == null || principal.isBlank() || keytabPath == null || keytabPath.isBlank())) {
            // Без кредов клиент пошел бы к керберизованным брокерам без SASL и получил бы
            // невнятный отказ по таймауту: лучше назвать причину сразу
            throw new IllegalArgumentException("Для Kafka-кластера " + bootstrapServers
                    + " включен Kerberos, но не заданы principal и keytab: задайте их в задаче или в топологии кластера");
        }
        ReplicatorProperties.ClusterConfig cluster = findCluster(bootstrapServers);
        String protocol = securityProtocol(cluster, kerberosEnabled);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
        if (protocol.endsWith("SSL") && cluster != null) {
            if (cluster.getKafkaTruststoreLocation() != null && !cluster.getKafkaTruststoreLocation().isBlank()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, cluster.getKafkaTruststoreLocation());
            }
            if (cluster.getKafkaTruststorePassword() != null && !cluster.getKafkaTruststorePassword().isBlank()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, cluster.getKafkaTruststorePassword());
            }
            if (cluster.getKafkaTruststoreType() != null && !cluster.getKafkaTruststoreType().isBlank()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, cluster.getKafkaTruststoreType());
            }
        }
        if (kerberosEnabled) {
            props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
            props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, serviceName != null && !serviceName.isBlank() ? serviceName : "kafka");
            String sanitizedKeytab = sanitizeJaasValue(keytabPath);
            String sanitizedPrincipal = sanitizeJaasValue(principal);
            String jaasTemplate = "com.sun.security.auth.module.Krb5LoginModule required " +
                    "useKeyTab=true " +
                    "storeKey=true " +
                    "keyTab=\"%s\" " +
                    "principal=\"%s\";";
            props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(jaasTemplate, sanitizedKeytab, sanitizedPrincipal));
            log.debug("Настроен Kerberos SASL/GSSAPI для Kafka AdminClient: principal={}", principal);
        }

        return AdminClient.create(props);
    }

    /**
     * Протокол клиента: из топологии кластера, иначе по флагу Kerberos. При Kerberos протокол
     * обязан быть SASL: заданный в топологии SSL поднимается до SASL_SSL.
     */
    static String securityProtocol(ReplicatorProperties.ClusterConfig cluster, boolean kerberosEnabled) {
        String configured = cluster != null && cluster.getKafkaSecurityProtocol() != null
                ? cluster.getKafkaSecurityProtocol().trim().toUpperCase(Locale.ROOT) : "";
        if (!configured.isEmpty() && !PROTOCOLS.contains(configured)) {
            throw new IllegalArgumentException("Неизвестный kafka-security-protocol '" + configured
                    + "' у кластера " + cluster.getId() + ": допустимы " + PROTOCOLS);
        }
        if (configured.isEmpty()) {
            return kerberosEnabled ? "SASL_PLAINTEXT" : "PLAINTEXT";
        }
        if (kerberosEnabled && !configured.startsWith("SASL_")) {
            return "SSL".equals(configured) ? "SASL_SSL" : "SASL_PLAINTEXT";
        }
        return configured;
    }

    /** Кластер топологии, у которого есть хотя бы один из переданных брокеров. */
    ReplicatorProperties.ClusterConfig findCluster(String bootstrapServers) {
        if (topologyRegistry == null || bootstrapServers == null || bootstrapServers.isBlank()) {
            return null;
        }
        Set<String> brokers = brokers(bootstrapServers);
        for (var c : topologyRegistry.getClusters()) {
            var cfg = topologyRegistry.getCluster(c.getId()).orElse(null);
            if (cfg != null && cfg.getKafkaBootstrapServers() != null
                    && brokers(cfg.getKafkaBootstrapServers()).stream().anyMatch(brokers::contains)) {
                return cfg;
            }
        }
        return null;
    }

    private static Set<String> brokers(String bootstrapServers) {
        return Arrays.stream(bootstrapServers.split(","))
                .map(b -> b.trim().toLowerCase(Locale.ROOT))
                .filter(b -> !b.isEmpty())
                .collect(Collectors.toSet());
    }

    private static String sanitizeJaasValue(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace(";", "")
                    .replace("\n", "")
                    .replace("\r", "");
    }
}
