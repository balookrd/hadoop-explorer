package org.apache.hadoop.explorer.replicator.kafka.security;

import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
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

        HadoopFsManager.validatePrincipalSafety(principal);
        validateKerberosPathSafety(keytabPath, "keytabPath");
        if (krbConfig.getKrb5ConfPath() != null && !krbConfig.getKrb5ConfPath().isBlank()) {
            validateKerberosPathSafety(krbConfig.getKrb5ConfPath(), "krb5ConfPath");
        }
        if (krbConfig.getTruststoreLocation() != null && !krbConfig.getTruststoreLocation().isBlank()) {
            validateKerberosPathSafety(krbConfig.getTruststoreLocation(), "truststoreLocation");
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

        // 3. Безопасная инициализация krb5.conf (без мутации существующего свойства JVM - SEC-11)
        if (krbConfig.getKrb5ConfPath() != null && !krbConfig.getKrb5ConfPath().isBlank()) {
            initializeKrb5ConfSafely(krbConfig.getKrb5ConfPath());
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

    private static final java.util.Set<String> FORBIDDEN_KERBEROS_OS_PREFIXES = java.util.Set.of(
            "/proc", "/sys", "/dev"
    );

    private static final java.util.List<String> FORBIDDEN_KERBEROS_EXACT_OR_SUFFIX = java.util.List.of(
            "/etc/shadow", "/etc/passwd", "/etc/sudoers", "/etc/master.passwd", "/etc/gshadow",
            "/.ssh", "/.gnupg", "/.env", "/id_rsa", "/id_ed25519", "/id_ecdsa", "/id_dsa"
    );

    /**
     * Валидация безопасности путей Kerberos (keytab, krb5.conf, truststore).
     * Разрешает легитимные пути конфигурации (например, /etc/krb5.conf, /etc/security/keytabs/...),
     * но предотвращает Path Traversal, Null Byte Injection и доступ к критическим файлам ОС.
     */
    public static void validateKerberosPathSafety(String pathStr, String paramName) {
        if (pathStr == null || pathStr.isBlank()) {
            return;
        }
        if (pathStr.indexOf('\0') >= 0) {
            throw new SecurityException("Попытка инъекции нулевого байта (Null Byte Injection) в параметр Kerberos '" +
                    paramName + "': " + pathStr);
        }
        String normalized = pathStr.replace('\\', '/').trim();
        if (normalized.contains("..")) {
            throw new SecurityException("Попытка Path Traversal ('..') в параметре Kerberos '" +
                    paramName + "': " + pathStr);
        }

        String pathOnly = normalized;
        if (normalized.contains("://")) {
            try {
                java.net.URI uri = java.net.URI.create(normalized);
                if (uri.getPath() != null && !uri.getPath().isEmpty()) {
                    pathOnly = uri.getPath();
                }
            } catch (Exception ignored) {
                int schemeIdx = normalized.indexOf("://");
                int slashIdx = normalized.indexOf('/', schemeIdx + 3);
                if (slashIdx >= 0) {
                    pathOnly = normalized.substring(slashIdx);
                }
            }
        }

        String lowerPath = pathOnly.toLowerCase(java.util.Locale.ROOT);
        for (String forbidden : FORBIDDEN_KERBEROS_OS_PREFIXES) {
            if (lowerPath.equals(forbidden) || lowerPath.startsWith(forbidden + "/")) {
                throw new SecurityException("Доступ к системным псевдо-ФС ядра ('" + forbidden +
                        "') в параметре Kerberos '" + paramName + "' запрещен политикой безопасности: " + pathStr);
            }
        }

        for (String sensitive : FORBIDDEN_KERBEROS_EXACT_OR_SUFFIX) {
            if (lowerPath.equals(sensitive) || lowerPath.endsWith(sensitive) || lowerPath.contains(sensitive + "/") || lowerPath.contains(sensitive + ".")) {
                throw new SecurityException("Доступ к конфиденциальным системным файлам ('" + sensitive +
                        "') в параметре Kerberos '" + paramName + "' запрещен политикой безопасности: " + pathStr);
            }
        }
    }

    /**
     * Безопасная инициализация пути к krb5.conf без деструктивной мутации глобального окружения JVM (SEC-11).
     * <p>
     * Если системное свойство {@code java.security.krb5.conf} уже установлено в JVM (например, UGI/HDFS,
     * другим кластером или через аргументы командной строки JVM), повторная перезапись блокируется,
     * предотвращая сброс настроек KDC и разрыв соединений с другими Kerberos Realm.
     *
     * @param requestedKrb5Conf запрошенный путь к файлу krb5.conf
     * @return true, если свойство было успешно установлено или уже содержит данный путь; false, если мутация отклонена
     */
    public static synchronized boolean initializeKrb5ConfSafely(String requestedKrb5Conf) {
        if (requestedKrb5Conf == null || requestedKrb5Conf.isBlank()) {
            return false;
        }
        validateKerberosPathSafety(requestedKrb5Conf, "krb5ConfPath");

        File f = new File(requestedKrb5Conf);
        if (!f.exists() || !f.canRead()) {
            log.warn("Указанный файл krb5.conf не существует или недоступен для чтения: {}", requestedKrb5Conf);
            return false;
        }

        String existingKrb5Conf = System.getProperty("java.security.krb5.conf");
        if (existingKrb5Conf == null || existingKrb5Conf.isBlank()) {
            System.setProperty("java.security.krb5.conf", requestedKrb5Conf);
            log.info("Установлено первичное системное свойство java.security.krb5.conf = {}", requestedKrb5Conf);
            return true;
        } else if (existingKrb5Conf.equals(requestedKrb5Conf)) {
            return true;
        } else {
            log.warn("Предотвращена мутация глобального системного свойства java.security.krb5.conf (SEC-11): " +
                    "в JVM уже активен путь '{}'. Запрос на переключение на '{}' отклонен во избежание сброса настроек KDC " +
                    "и рассинхронизации Kerberos-аутентификации других сервисов платформы.",
                    existingKrb5Conf, requestedKrb5Conf);
            return false;
        }
    }

    /**
     * Сброс свойства java.security.krb5.conf для изолированного тестирования.
     */
    public static synchronized void resetKrb5ConfForTesting(String value) {
        if (value == null) {
            System.clearProperty("java.security.krb5.conf");
        } else {
            System.setProperty("java.security.krb5.conf", value);
        }
    }
}
