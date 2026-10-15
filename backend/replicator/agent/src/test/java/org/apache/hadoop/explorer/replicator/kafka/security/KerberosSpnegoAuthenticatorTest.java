package org.apache.hadoop.explorer.replicator.kafka.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование генератора Kerberos SPNEGO токенов")
class KerberosSpnegoAuthenticatorTest {

    @Test
    @DisplayName("Возвращает null если Kerberos выключен в конфигурации")
    void shouldReturnNullWhenDisabled() {
        KafkaKerberosConfig config = new KafkaKerberosConfig();
        config.setEnabled(false);
        config.setSrKerberosEnabled(false);

        KerberosSpnegoAuthenticator authenticator = new KerberosSpnegoAuthenticator(config);
        String header = authenticator.generateNegotiateHeader(URI.create("http://sr-host:8081/schemas/ids/1"));
        assertNull(header, "Заголовок должен быть null если Kerberos выключен");
    }

    @Test
    @DisplayName("Возвращает null и логирует предупреждение если keytab файл отсутствует")
    void shouldReturnNullGracefullyWhenKeytabMissing() {
        KafkaKerberosConfig config = new KafkaKerberosConfig();
        config.setEnabled(true);
        config.setSrKerberosEnabled(true);
        config.setPrincipal("kafka-replicator@REALM.CORP");
        config.setKeytabPath("/non/existent/path/kafka.keytab");

        KerberosSpnegoAuthenticator authenticator = new KerberosSpnegoAuthenticator(config);
        String header = authenticator.generateNegotiateHeader(URI.create("http://sr-host:8081/schemas/ids/1"));
        assertNull(header, "Должен безопасно возвращать null без аварийного падения приложения");
    }

    @Test
    @DisplayName("Возвращает null если principal не задан")
    void shouldReturnNullWhenPrincipalMissing() {
        KafkaKerberosConfig config = new KafkaKerberosConfig();
        config.setSrKerberosEnabled(true);
        config.setKeytabPath("/etc/security/keytabs/kafka.keytab");

        KerberosSpnegoAuthenticator authenticator = new KerberosSpnegoAuthenticator(config);
        String header = authenticator.generateNegotiateHeader(URI.create("http://sr-host:8081"));
        assertNull(header);
    }
}
