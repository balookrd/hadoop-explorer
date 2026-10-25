package org.apache.hadoop.explorer.replicator.telemetry;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Проба контейнера должна отличать работающий приемник от просто открытого порта.
 *
 * <p>HEALTHCHECK агента сводился к открытию TCP-соединения на порт приемника: агент с
 * незапущенным или уже остановленным gRPC-сервером такую проверку проходил, и оркестратор
 * контейнеров продолжал считать его исправным.
 */
@DisplayName("Проба живости агента по health-протоколу")
class GrpcHealthProbeTest {

    private Server server;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (server != null) {
            server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private String startServer(GrpcHealth health) throws IOException {
        server = ServerBuilder.forPort(0).addService(health.service()).build().start();
        return "localhost:" + server.getPort();
    }

    @Test
    @DisplayName("Работающий приемник проходит пробу")
    void testServingAgentPassesProbe() throws Exception {
        GrpcHealth health = new GrpcHealth();
        health.serving();

        assertTrue(GrpcHealthProbeMain.probe(startServer(health), "", 3),
                "Исправный агент обязан проходить пробу");
    }

    @Test
    @DisplayName("Приемник в NOT_SERVING пробу не проходит, хотя порт открыт")
    void testNotServingAgentFailsProbe() throws Exception {
        GrpcHealth health = new GrpcHealth();
        health.notServing();
        String target = startServer(health);

        assertFalse(GrpcHealthProbeMain.probe(target, "", 3),
                "Порт открыт, но обслуживать вызовы агент не может: проба обязана упасть");
    }

    @Test
    @DisplayName("Остановленный приемник пробу не проходит")
    void testStoppedAgentFailsProbe() throws Exception {
        GrpcHealth health = new GrpcHealth();
        health.serving();
        String target = startServer(health);
        assertTrue(GrpcHealthProbeMain.probe(target, "", 3));

        server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);

        assertFalse(GrpcHealthProbeMain.probe(target, "", 2),
                "После остановки сервера проба обязана упасть");
    }

    @Test
    @DisplayName("Статус отдельной подсистемы проверяется по имени сервиса")
    void testPerSubsystemProbe() throws Exception {
        GrpcHealth health = new GrpcHealth();
        health.serving();
        health.notServing("replicator.DataTransferService");
        String target = startServer(health);

        assertTrue(GrpcHealthProbeMain.probe(target, "", 3),
                "Процесс в целом жив");
        assertFalse(GrpcHealthProbeMain.probe(target, "replicator.DataTransferService", 3),
                "Передача данных не работает: подсистемная проба обязана это показать");
    }
}
