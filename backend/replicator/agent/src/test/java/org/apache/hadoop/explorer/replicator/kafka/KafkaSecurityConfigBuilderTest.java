package org.apache.hadoop.explorer.replicator.kafka;

import org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig;
import org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование билдера безопасности Kerberos для Kafka (KafkaSecurityConfigBuilder)")
class KafkaSecurityConfigBuilderTest {

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
}
