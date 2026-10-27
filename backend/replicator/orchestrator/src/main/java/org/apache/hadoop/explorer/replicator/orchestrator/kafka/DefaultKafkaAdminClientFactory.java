package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class DefaultKafkaAdminClientFactory implements KafkaAdminClientFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaAdminClientFactory.class);

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
        if (kerberosEnabled) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
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
