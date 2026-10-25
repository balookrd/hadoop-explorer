package org.apache.hadoop.explorer.replicator.telemetry;

import io.grpc.*;
import io.grpc.stub.ClientCalls;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Серверы репликатора должны отвечать по стандартному health-протоколу gRPC.
 *
 * <p>Ни у оркестратора, ни у агентов сервиса {@code grpc.health.v1.Health} не было: балансировщик
 * и оркестратор контейнеров могли судить о живости только по открытому TCP-порту, то есть процесс
 * с уже остановленным приемником или не прошедший инициализацию считался исправным.
 *
 * <p>Отдельно проверяется, что health-проверка доступна без {@code x-agent-secret}: пробы
 * балансировщика и контейнера секрета агента не знают, а остальные вызовы остаются закрытыми.
 */
@DisplayName("Телеметрия: health-протокол gRPC")
class GrpcHealthServiceTest {

    private static final String SECRET = "test-health-agent-secret-0123456789";

    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private HealthGrpc.HealthBlockingStub start(GrpcHealth health, boolean withAuth) throws IOException {
        String name = InProcessServerBuilder.generateName();
        ServerBuilder<?> builder = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(health.service())
                .addService(businessService());
        if (withAuth) {
            builder.intercept(AgentGrpcAuthInterceptor.forServer(SECRET));
        }
        server = builder.build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return HealthGrpc.newBlockingStub(channel);
    }

    @Test
    @DisplayName("Запущенный сервер отвечает SERVING")
    void testServingStatus() throws Exception {
        GrpcHealth health = new GrpcHealth();
        health.serving();

        HealthCheckResponse response = start(health, false)
                .check(HealthCheckRequest.newBuilder().build());

        assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus(),
                "Сервер обязан объявлять себя готовым: иначе проба судит о живости только по "
                        + "открытому TCP-порту");
    }

    @Test
    @DisplayName("Статус конкретного сервиса виден отдельно от общего")
    void testPerServiceStatus() throws Exception {
        GrpcHealth health = new GrpcHealth();
        health.serving();
        health.notServing("replicator.DataTransferService");

        HealthGrpc.HealthBlockingStub stub = start(health, false);

        assertEquals(HealthCheckResponse.ServingStatus.SERVING,
                stub.check(HealthCheckRequest.newBuilder().build()).getStatus());
        assertEquals(HealthCheckResponse.ServingStatus.NOT_SERVING,
                stub.check(HealthCheckRequest.newBuilder()
                        .setService("replicator.DataTransferService").build()).getStatus(),
                "Неработающая подсистема обязана быть видна отдельно: процесс жив, "
                        + "но принимать данные не может");
    }

    @Test
    @DisplayName("Остановка переводит сервер в NOT_SERVING")
    void testShutdownStopsServing() throws Exception {
        GrpcHealth health = new GrpcHealth();
        health.serving();
        HealthGrpc.HealthBlockingStub stub = start(health, false);
        assertEquals(HealthCheckResponse.ServingStatus.SERVING,
                stub.check(HealthCheckRequest.newBuilder().build()).getStatus());

        health.shutdown();

        assertEquals(HealthCheckResponse.ServingStatus.NOT_SERVING,
                stub.check(HealthCheckRequest.newBuilder().build()).getStatus(),
                "Терминальное состояние обязано быть видно пробе: иначе балансировщик продолжит "
                        + "гнать трафик на останавливающийся узел до истечения таймаутов");
    }

    @Test
    @DisplayName("Проба проходит без секрета агента, остальные вызовы — нет")
    void testHealthIsReachableWithoutAgentSecret() throws Exception {
        GrpcHealth health = new GrpcHealth();
        health.serving();
        HealthGrpc.HealthBlockingStub stub = start(health, true);

        HealthCheckResponse response = stub.check(HealthCheckRequest.newBuilder().build());
        assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus(),
                "Проба балансировщика не знает x-agent-secret: health обязан быть доступен");

        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> ClientCalls.blockingUnaryCall(channel, BUSINESS_METHOD, CallOptions.DEFAULT, "ping"));
        assertEquals(Status.Code.UNAUTHENTICATED, error.getStatus().getCode(),
                "Исключение для health не должно открывать остальные вызовы, получено: "
                        + error.getStatus());
    }

    /**
     * Зарегистрированный сервис нужен по существу: для незарегистрированного метода сервер
     * отвечает UNIMPLEMENTED еще до перехватчиков, и проверка аутентификации была бы пустой.
     */
    private static ServerServiceDefinition businessService() {
        return ServerServiceDefinition.builder("replicator.control.OrchestratorControlPlane")
                .addMethod(BUSINESS_METHOD, io.grpc.stub.ServerCalls.asyncUnaryCall(
                        (request, responseObserver) -> {
                            responseObserver.onNext("ok");
                            responseObserver.onCompleted();
                        }))
                .build();
    }

    private static final MethodDescriptor<String, String> BUSINESS_METHOD =
            MethodDescriptor.<String, String>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName("replicator.control.OrchestratorControlPlane/ClaimTasks")
                    .setRequestMarshaller(new StringMarshaller())
                    .setResponseMarshaller(new StringMarshaller())
                    .build();

    private static class StringMarshaller implements MethodDescriptor.Marshaller<String> {
        @Override
        public java.io.InputStream stream(String value) {
            return new java.io.ByteArrayInputStream(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public String parse(java.io.InputStream stream) {
            try {
                return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
    }
}
