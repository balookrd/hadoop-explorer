package org.apache.hadoop.explorer.replicator.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Поведение клиента control plane при неудачной настройке TLS.
 *
 * <p>Ключевая гарантия: сбой настройки TLS не должен превращаться в открытое соединение.
 * Молчаливый откат в plaintext отправлял бы трафик control plane вместе с заголовком
 * {@code x-agent-secret} по сети в открытом виде — при включенном TLS оператор об этом
 * даже не узнает.
 */
@DisplayName("OrchestratorGrpcClient: настройка TLS")
class OrchestratorGrpcClientTlsTest {

    @Test
    @DisplayName("Отсутствующий файл доверенных сертификатов не приводит к откату в plaintext")
    void testMissingTrustCertFailsInsteadOfPlaintextDowngrade() {
        assertThrows(IllegalStateException.class, () -> new OrchestratorGrpcClient(
                        List.of("127.0.0.1:50050"),
                        "agent-secret",
                        true,
                        "/nonexistent/path/truststore.pem",
                        false,
                        false),
                "При включенном TLS и нечитаемом сертификате клиент обязан отказать, а не уйти в plaintext");
    }

    @Test
    @DisplayName("Битый файл сертификата не приводит к откату в plaintext")
    void testBrokenTrustCertFailsInsteadOfPlaintextDowngrade() throws Exception {
        java.io.File broken = java.io.File.createTempFile("broken-truststore", ".pem");
        java.nio.file.Files.writeString(broken.toPath(), "this is not a certificate");
        try {
            assertThrows(IllegalStateException.class, () -> new OrchestratorGrpcClient(
                            List.of("127.0.0.1:50050"),
                            "agent-secret",
                            true,
                            broken.getAbsolutePath(),
                            false,
                            false),
                    "Некорректный сертификат обязан прерывать запуск, а не ослаблять канал");
        } finally {
            java.nio.file.Files.deleteIfExists(broken.toPath());
        }
    }
}
