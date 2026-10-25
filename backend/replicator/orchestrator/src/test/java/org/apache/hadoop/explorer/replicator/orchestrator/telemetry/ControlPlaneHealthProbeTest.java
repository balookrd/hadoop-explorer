package org.apache.hadoop.explorer.replicator.orchestrator.telemetry;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import org.apache.hadoop.explorer.replicator.generated.control.ClaimTasksRequest;
import org.apache.hadoop.explorer.replicator.generated.control.OrchestratorControlPlaneServiceGrpc;
import org.apache.hadoop.explorer.replicator.orchestrator.grpc.OrchestratorGrpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Control Plane обязан отвечать на стандартную health-пробу gRPC.
 *
 * <p>Сервиса {@code grpc.health.v1.Health} не было, и живость control plane проверялась только
 * открытым TCP-портом: узел с упавшей инициализацией или уже остановленным сервером выглядел для
 * балансировщика исправным и продолжал получать вызовы агентов.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Control Plane: health-проба gRPC")
class ControlPlaneHealthProbeTest {

    @Autowired
    private OrchestratorGrpcServer grpcServer;

    private ManagedChannel channel;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private ManagedChannel channel() {
        assertTrue(grpcServer.isRunning(), "gRPC Control Plane должен быть запущен в тестовом профиле");
        channel = ManagedChannelBuilder.forAddress("localhost", grpcServer.getPort())
                .usePlaintext()
                .build();
        return channel;
    }

    @Test
    @DisplayName("Запущенный Control Plane отвечает SERVING без секрета агента")
    void testHealthCheckReportsServing() {
        HealthCheckResponse response = HealthGrpc.newBlockingStub(channel())
                .check(HealthCheckRequest.newBuilder().build());

        assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus(),
                "Проба балансировщика секрета агента не знает и обязана получать статус");
    }

    @Test
    @DisplayName("Исключение для health не открывает вызовы Control Plane")
    void testBusinessCallStillRequiresSecret() {
        var stub = OrchestratorControlPlaneServiceGrpc.newBlockingStub(channel());

        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> stub.claimTasks(ClaimTasksRequest.newBuilder()
                        .setWorkerId("agent-health-probe")
                        .setClusterId("dc1")
                        .setBatchSize(1)
                        .build()));

        assertEquals(io.grpc.Status.Code.UNAUTHENTICATED, error.getStatus().getCode(),
                "Забор задач обязан остаться закрытым, получено: " + error.getStatus());
    }
}
