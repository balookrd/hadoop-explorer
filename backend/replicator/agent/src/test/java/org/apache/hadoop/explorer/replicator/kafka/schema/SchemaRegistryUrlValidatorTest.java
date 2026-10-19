package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.HwxRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.SchemaRegistryUrlValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование валидации URL Schema Registry и защиты от SSRF (SEC-04)")
class SchemaRegistryUrlValidatorTest {

    @Test
    @DisplayName("Валидные HTTP/HTTPS адреса успешно нормализуются")
    void testValidUrls() {
        assertEquals("http://schema-registry.internal:8081",
                SchemaRegistryUrlValidator.validate("http://schema-registry.internal:8081///"));
        assertEquals("https://schema-registry.internal:8081/sr",
                SchemaRegistryUrlValidator.validate("https://schema-registry.internal:8081/sr/"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "http://169.254.169.254/latest/meta-data/",
            "http://169.254.1.1:8080",
            "http://metadata.google.internal/computeMetadata/v1/",
            "https://169.254.169.254"
    })
    @DisplayName("Попытки SSRF на облачные метаданные и link-local адреса блокируются")
    void testBlockSsrfMetadataAddresses(String evilUrl) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                SchemaRegistryUrlValidator.validate(evilUrl));
        assertTrue(ex.getMessage().contains("SSRF blocked"), "Ожидалось сообщение о блокировке SSRF: " + ex.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "file:///etc/passwd",
            "ftp://files.example.com",
            "gopher://127.0.0.1:25",
            "jar:file:/tmp/app.jar!/"
    })
    @DisplayName("Недопустимые схемы URI блокируются")
    void testBlockDisallowedSchemes(String disallowedUrl) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                SchemaRegistryUrlValidator.validate(disallowedUrl));
        assertTrue(ex.getMessage().contains("Only http and https are allowed"),
                "Ожидалось сообщение о недопустимой схеме: " + ex.getMessage());
    }

    @Test
    @DisplayName("CRLF инъекции в URL блокируются")
    void testBlockCrlfInjection() {
        assertThrows(IllegalArgumentException.class, () ->
                SchemaRegistryUrlValidator.validate("http://sr.example.com:8081\r\nX-Injected: true"));
        assertThrows(IllegalArgumentException.class, () ->
                SchemaRegistryUrlValidator.validate("http://sr.example.com:8081\nHost: evil.com"));
    }

    @Test
    @DisplayName("Клиенты Schema Registry отклоняют небезопасные URL в конструкторе")
    void testClientsRejectEvilUrls() {
        assertThrows(IllegalArgumentException.class, () ->
                new ConfluentRegistryClient("http://169.254.169.254/latest/meta-data"));
        assertThrows(IllegalArgumentException.class, () ->
                new HwxRegistryClient("file:///etc/shadow"));
    }
}
