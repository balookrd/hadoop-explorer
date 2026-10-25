package org.apache.hadoop.explorer.replicator.telemetry;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;

import java.util.concurrent.TimeUnit;

/**
 * Проба живости агента по стандартному health-протоколу gRPC.
 *
 * <p>Запускается из HEALTHCHECK контейнера тем же jar-ом, что и сам агент, поэтому отдельный
 * grpc_health_probe в образ ставить не нужно. Прежняя проверка открывала TCP-соединение на порт
 * приемника: процесс с незапущенным или уже остановленным gRPC-сервером такую проверку проходил.
 *
 * <p>Код возврата 0 — SERVING, 1 — любое другое состояние или недоступность.
 *
 * <pre>java -cp /app/replicator-agent.jar \
 *     org.apache.hadoop.explorer.replicator.telemetry.GrpcHealthProbeMain localhost:50051</pre>
 */
public final class GrpcHealthProbeMain {

    private static final long DEFAULT_TIMEOUT_SECONDS = 3;

    private GrpcHealthProbeMain() {
    }

    public static void main(String[] args) {
        String target = args.length > 0 ? args[0] : "localhost:50051";
        String service = args.length > 1 ? args[1] : "";
        long timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
        if (args.length > 2) {
            try {
                timeoutSeconds = Long.parseLong(args[2]);
            } catch (NumberFormatException ignored) {
                // остается значение по умолчанию
            }
        }

        boolean serving = probe(target, service, timeoutSeconds);
        if (!serving) {
            System.err.println("health probe: " + target
                    + (service.isBlank() ? "" : " (" + service + ")") + " не в состоянии SERVING");
        }
        System.exit(serving ? 0 : 1);
    }

    /**
     * Проверка состояния: {@code true} только если сервер ответил SERVING.
     *
     * <p>Проба умышленно не передает x-agent-secret: health-протокол освобожден от
     * аутентификации, потому что оркестратор контейнеров секрета агента не знает.
     */
    public static boolean probe(String target, String service, long timeoutSeconds) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        try {
            HealthCheckResponse response = HealthGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                    .check(HealthCheckRequest.newBuilder().setService(service).build());
            return response.getStatus() == HealthCheckResponse.ServingStatus.SERVING;
        } catch (Exception e) {
            return false;
        } finally {
            channel.shutdownNow();
            try {
                channel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
