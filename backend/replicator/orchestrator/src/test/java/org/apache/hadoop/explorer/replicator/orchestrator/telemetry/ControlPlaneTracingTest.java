package org.apache.hadoop.explorer.replicator.orchestrator.telemetry;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.apache.hadoop.explorer.replicator.generated.control.ClaimTasksRequest;
import org.apache.hadoop.explorer.replicator.generated.control.OrchestratorControlPlaneServiceGrpc;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.grpc.OrchestratorGrpcServer;
import org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Вызовы агентов к Control Plane должны попадать в трассу.
 *
 * <p>Проверяется не конфигурация, а факт: реальный вызов gRPC по сети порождает серверный спан с
 * именем метода. Экземпляр OpenTelemetry берется из контекста Spring, тот же, что инструментирует
 * HTTP-слой, иначе вызовы агентов оказались бы в отдельной трассе.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Control Plane: вызовы агентов попадают в трассу")
class ControlPlaneTracingTest {

    /** Экспортер в память вместо OTLP-коллектора: Spring Boot сам заворачивает его в процессор. */
    @TestConfiguration
    static class InMemoryTracingConfig {
        static final InMemorySpanExporter EXPORTER = InMemorySpanExporter.create();

        @Bean
        SpanExporter inMemorySpanExporter() {
            return EXPORTER;
        }
    }

    @Autowired
    private OrchestratorGrpcServer grpcServer;

    @Autowired
    private OpenTelemetry openTelemetry;

    @Autowired
    private ReplicatorProperties properties;

    private ManagedChannel channel;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
        InMemoryTracingConfig.EXPORTER.reset();
    }

    @Test
    @DisplayName("Забор задач агентом порождает серверный спан с именем метода")
    void testAgentCallProducesServerSpan() {
        InMemoryTracingConfig.EXPORTER.reset();

        channel = ManagedChannelBuilder.forAddress("localhost", grpcServer.getPort())
                .usePlaintext()
                .intercept(AgentGrpcAuthInterceptor.createClientInterceptor(properties.getAgentSecret()))
                .build();

        OrchestratorControlPlaneServiceGrpc.newBlockingStub(channel)
                .claimTasks(ClaimTasksRequest.newBuilder()
                        .setWorkerId("agent-tracing-test")
                        .setClusterId("dc1")
                        .setBatchSize(1)
                        .build());

        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush().join(5, TimeUnit.SECONDS);

        var serverSpans = InMemoryTracingConfig.EXPORTER.getFinishedSpanItems().stream()
                .filter(span -> span.getKind() == SpanKind.SERVER)
                .map(span -> span.getName())
                .toList();

        assertTrue(serverSpans.contains(
                        "replicator.control.OrchestratorControlPlaneService/ClaimTasks"),
                "Вызова агента нет в трассе, собраны спаны: " + serverSpans);
    }
}
