package org.apache.hadoop.explorer.replicator.kafka.security;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * Билдер конфигурации безопасности Kerberos (SASL/GSSAPI) для клиентов Apache Kafka.
 * <p>
 * Формирует изолированный in-memory JAAS-контекст через свойство {@code sasl.jaas.config},
 * исключая зависимость от глобального файла {@code -Djava.security.auth.login.config}.
 */
public final class KafkaSecurityConfigBuilder {

    private static final Logger log = LoggerFactory.getLogger(KafkaSecurityConfigBuilder.class);

    private KafkaSecurityConfigBuilder() {
    }

    /**
     * Обогащает переданные свойства Kafka параметрами аутентификации Kerberos.
     *
     * @param props    исходные свойства Kafka Producer/Consumer/Admin
     * @param krbConfig конфигурация Kerberos
     * @return обогащенный объект Properties
     */
    public static Properties applyKerberos(Properties props, KafkaKerberosConfig krbConfig) {
        if (props == null) {
            props = new Properties();
        }
        if (krbConfig == null || !krbConfig.isEnabled()) {
            return props;
        }

        String principal = krbConfig.getPrincipal();
        String keytabPath = krbConfig.getKeytabPath();

        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("Kerberos principal must be specified when Kerberos is enabled");
        }
        if (keytabPath == null || keytabPath.isBlank()) {
            throw new IllegalArgumentException("Kerberos keytabPath must be specified when Kerberos is enabled");
        }

        File keytabFile = new File(keytabPath);
        if (!keytabFile.exists() || !keytabFile.canRead()) {
            log.warn("Файл keytab не найден или недоступен для чтения по пути: {}", keytabPath);
        }

        // 1. Протокол безопасности и SASL механизм GSSAPI
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, krbConfig.getSecurityProtocol());
        props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
        props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, krbConfig.getServiceName());

        // 2. Генерация динамического JAAS конфига (Krb5LoginModule) с защитой от JAAS Injection
        String sanitizedKeytab = sanitizeJaasValue(keytabPath);
        String sanitizedPrincipal = sanitizeJaasValue(principal);
        String jaasTemplate = "com.sun.security.auth.module.Krb5LoginModule required " +
                "useKeyTab=true " +
                "storeKey=true " +
                "doNotPrompt=true " +
                "refreshKrb5Config=true " +
                "keyTab=\"%s\" " +
                "principal=\"%s\";";
        String jaasConfig = String.format(jaasTemplate, sanitizedKeytab, sanitizedPrincipal);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        // 3. krb5.conf (если указан)
        if (krbConfig.getKrb5ConfPath() != null && !krbConfig.getKrb5ConfPath().isBlank()) {
            String existingKrb5Conf = System.getProperty("java.security.krb5.conf");
            if (existingKrb5Conf == null || !existingKrb5Conf.equals(krbConfig.getKrb5ConfPath())) {
                System.setProperty("java.security.krb5.conf", krbConfig.getKrb5ConfPath());
                log.info("Установлено системное свойство java.security.krb5.conf = {}", krbConfig.getKrb5ConfPath());
            }
        }

        // 4. SSL параметры для SASL_SSL
        if ("SASL_SSL".equalsIgnoreCase(krbConfig.getSecurityProtocol())) {
            if (krbConfig.getTruststoreLocation() != null && !krbConfig.getTruststoreLocation().isBlank()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, krbConfig.getTruststoreLocation());
            }
            if (krbConfig.getTruststorePassword() != null) {
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, krbConfig.getTruststorePassword());
            }
        }

        log.info("Успешно применены параметры Kerberos для Kafka: protocol={}, service={}, principal={}",
                krbConfig.getSecurityProtocol(), krbConfig.getServiceName(), principal);

        return props;
    }

    /**
     * Очищает и экранирует спецсимволы для безопасной вставки в JAAS login configuration,
     * предотвращая инъекции директив (JAAS Injection).
     */
    public static String sanitizeJaasValue(String value) {
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
