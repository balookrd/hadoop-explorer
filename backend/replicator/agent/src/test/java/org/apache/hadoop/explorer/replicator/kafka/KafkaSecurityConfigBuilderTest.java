package org.apache.hadoop.explorer.replicator.kafka;

import org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig;
import org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование билдера безопасности Kerberos для Kafka (KafkaSecurityConfigBuilder)")
class KafkaSecurityConfigBuilderTest {

    private String originalKrb5Conf;

    @BeforeEach
    void saveOriginalKrb5() {
        originalKrb5Conf = System.getProperty("java.security.krb5.conf");
    }

    @AfterEach
    void restoreOriginalKrb5() {
        KafkaSecurityConfigBuilder.resetKrb5ConfForTesting(originalKrb5Conf);
    }

    @Test
    @DisplayName("При выключенном Kerberos свойства не модифицируются")
    void testKerberosDisabled() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        KafkaKerberosConfig config = new KafkaKerberosConfig();
        config.setEnabled(false);

        Properties result = KafkaSecurityConfigBuilder.applyKerberos(props, config);
        assertNull(result.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertNull(result.get(SaslConfigs.SASL_MECHANISM));
        assertNull(result.get(SaslConfigs.SASL_JAAS_CONFIG));
    }

    @Test
    @DisplayName("Генерация валидных параметров SASL_PLAINTEXT и JAAS конфига")
    void testKerberosSaslPlaintext() {
        Properties props = new Properties();

        KafkaKerberosConfig config = new KafkaKerberosConfig();
        config.setEnabled(true);
        config.setSecurityProtocol("SASL_PLAINTEXT");
        config.setServiceName("kafka");
        config.setPrincipal("kafka-replicator@REALM.CORP");
        config.setKeytabPath("/etc/security/keytabs/kafka-replicator.keytab");

        Properties result = KafkaSecurityConfigBuilder.applyKerberos(props, config);

        assertEquals("SASL_PLAINTEXT", result.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("GSSAPI", result.getProperty(SaslConfigs.SASL_MECHANISM));
        assertEquals("kafka", result.getProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));

        String jaas = result.getProperty(SaslConfigs.SASL_JAAS_CONFIG);
        assertNotNull(jaas);
        assertTrue(jaas.contains("com.sun.security.auth.module.Krb5LoginModule required"));
        assertTrue(jaas.contains("useKeyTab=true"));
        assertTrue(jaas.contains("principal=\"kafka-replicator@REALM.CORP\""));
        assertTrue(jaas.contains("keyTab=\"/etc/security/keytabs/kafka-replicator.keytab\""));
    }

    @Test
    @DisplayName("Генерация параметров SASL_SSL с Truststore")
    void testKerberosSaslSsl() {
        Properties props = new Properties();

        KafkaKerberosConfig config = new KafkaKerberosConfig();
        config.setEnabled(true);
        config.setSecurityProtocol("SASL_SSL");
        config.setServiceName("kafka");
        config.setPrincipal("kafka-replicator@REALM.CORP");
        config.setKeytabPath("/etc/security/keytabs/kafka-replicator.keytab");
        config.setTruststoreLocation("/var/private/ssl/kafka.truststore.jks");
        config.setTruststorePassword("truststore-secret");

        Properties result = KafkaSecurityConfigBuilder.applyKerberos(props, config);

        assertEquals("SASL_SSL", result.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("GSSAPI", result.getProperty(SaslConfigs.SASL_MECHANISM));
        assertEquals("/var/private/ssl/kafka.truststore.jks", result.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals("truststore-secret", result.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    }

    @Test
    @DisplayName("Валидация обязательных полей principal и keytab при включении Kerberos")
    void testValidationFailureOnMissingFields() {
        Properties props = new Properties();

        KafkaKerberosConfig configNoKeytab = new KafkaKerberosConfig();
        configNoKeytab.setEnabled(true);
        configNoKeytab.setPrincipal("user@REALM");
        assertThrows(IllegalArgumentException.class, () -> KafkaSecurityConfigBuilder.applyKerberos(props, configNoKeytab));

        KafkaKerberosConfig configNoPrincipal = new KafkaKerberosConfig();
        configNoPrincipal.setEnabled(true);
        configNoPrincipal.setKeytabPath("/some/path.keytab");
        assertThrows(IllegalArgumentException.class, () -> KafkaSecurityConfigBuilder.applyKerberos(props, configNoPrincipal));
    }

    @Test
    @DisplayName("SEC-11: Первичная инициализация krb5.conf при незаданном системном свойстве")
    void testKrb5ConfInitializationWhenEmpty() throws Exception {
        KafkaSecurityConfigBuilder.resetKrb5ConfForTesting(null);
        Path tempKrb5 = Files.createTempFile("test-krb5-", ".conf");
        try {
            boolean result = KafkaSecurityConfigBuilder.initializeKrb5ConfSafely(tempKrb5.toAbsolutePath().toString());
            assertTrue(result, "Первичная инициализация должна завершиться успешно");
            assertEquals(tempKrb5.toAbsolutePath().toString(), System.getProperty("java.security.krb5.conf"));
        } finally {
            Files.deleteIfExists(tempKrb5);
        }
    }

    @Test
    @DisplayName("SEC-11: Блокировка мутации java.security.krb5.conf при уже заданном пути")
    void testKrb5ConfMutationBlockedWhenAlreadySet() throws Exception {
        Path tempKrb5First = Files.createTempFile("krb5-first-", ".conf");
        Path tempKrb5Second = Files.createTempFile("krb5-second-", ".conf");
        try {
            KafkaSecurityConfigBuilder.resetKrb5ConfForTesting(tempKrb5First.toAbsolutePath().toString());

            // Попытка переключить на второй krb5.conf должна быть заблокирована
            boolean result = KafkaSecurityConfigBuilder.initializeKrb5ConfSafely(tempKrb5Second.toAbsolutePath().toString());
            assertFalse(result, "Повторная мутация krb5.conf должна быть отклонена");
            assertEquals(tempKrb5First.toAbsolutePath().toString(), System.getProperty("java.security.krb5.conf"),
                    "Системное свойство должно сохранить исходный путь и не перезаписываться");
        } finally {
            Files.deleteIfExists(tempKrb5First);
            Files.deleteIfExists(tempKrb5Second);
        }
    }

    @Test
    @DisplayName("SEC-11: Повторная передача идентичного krb5.conf разрешена")
    void testKrb5ConfSamePathAllowed() throws Exception {
        Path tempKrb5 = Files.createTempFile("krb5-same-", ".conf");
        try {
            KafkaSecurityConfigBuilder.resetKrb5ConfForTesting(tempKrb5.toAbsolutePath().toString());

            boolean result = KafkaSecurityConfigBuilder.initializeKrb5ConfSafely(tempKrb5.toAbsolutePath().toString());
            assertTrue(result, "Передача идентичного пути должна подтверждаться без ошибки");
            assertEquals(tempKrb5.toAbsolutePath().toString(), System.getProperty("java.security.krb5.conf"));
        } finally {
            Files.deleteIfExists(tempKrb5);
        }
    }

    @Test
    @DisplayName("SEC-11: Валидация безопасности путей keytab и krb5.conf блокирует системные файлы ОС и инъекции")
    void testPathSafetyEnforcedOnKeytabAndKrb5Conf() {
        Properties props = new Properties();

        KafkaKerberosConfig configEtcShadow = new KafkaKerberosConfig();
        configEtcShadow.setEnabled(true);
        configEtcShadow.setPrincipal("kafka-replicator@CORP.LOCAL");
        configEtcShadow.setKeytabPath("/etc/shadow");
        assertThrows(SecurityException.class, () -> KafkaSecurityConfigBuilder.applyKerberos(props, configEtcShadow));

        KafkaKerberosConfig configTraversalKrb5 = new KafkaKerberosConfig();
        configTraversalKrb5.setEnabled(true);
        configTraversalKrb5.setPrincipal("kafka-replicator@CORP.LOCAL");
        configTraversalKrb5.setKeytabPath("/var/data/kafka.keytab");
        configTraversalKrb5.setKrb5ConfPath("../../etc/passwd");
        assertThrows(SecurityException.class, () -> KafkaSecurityConfigBuilder.applyKerberos(props, configTraversalKrb5));

        KafkaKerberosConfig configNullByte = new KafkaKerberosConfig();
        configNullByte.setEnabled(true);
        configNullByte.setPrincipal("kafka-replicator@CORP.LOCAL");
        configNullByte.setKeytabPath("/var/data/safe.keytab\0malicious");
        assertThrows(SecurityException.class, () -> KafkaSecurityConfigBuilder.applyKerberos(props, configNullByte));
    }

    @Test
    @DisplayName("SEC-11: Валидация безопасности принципала блокирует системные учетки ОС и инъекции")
    void testPrincipalSafetyEnforced() {
        Properties props = new Properties();

        KafkaKerberosConfig configRoot = new KafkaKerberosConfig();
        configRoot.setEnabled(true);
        configRoot.setPrincipal("root");
        configRoot.setKeytabPath("/var/data/kafka.keytab");
        assertThrows(SecurityException.class, () -> KafkaSecurityConfigBuilder.applyKerberos(props, configRoot));

        KafkaKerberosConfig configInjection = new KafkaKerberosConfig();
        configInjection.setEnabled(true);
        configInjection.setPrincipal("user\nadmin");
        configInjection.setKeytabPath("/var/data/kafka.keytab");
        assertThrows(SecurityException.class, () -> KafkaSecurityConfigBuilder.applyKerberos(props, configInjection));
    }
}
