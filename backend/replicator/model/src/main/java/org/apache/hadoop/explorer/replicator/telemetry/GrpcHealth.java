package org.apache.hadoop.explorer.replicator.telemetry;

import io.grpc.BindableService;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;

/**
 * Стандартный health-протокол gRPC ({@code grpc.health.v1.Health}) для серверов репликатора.
 *
 * <p>Без него балансировщик и оркестратор контейнеров могли судить о живости только по открытому
 * TCP-порту: процесс с остановленным приемником или не прошедший инициализацию считался исправным
 * и продолжал получать трафик. Статус выставляется и в целом по серверу, и отдельно по
 * подсистемам, чтобы было видно, что процесс жив, а принимать данные не может.
 *
 * <p>Проба доступна без {@code x-agent-secret}: пробы балансировщика и контейнера секрета агента
 * не знают (см. исключение в {@code AgentGrpcAuthInterceptor}).
 */
public final class GrpcHealth {

    /** Полное имя стандартного health-сервиса gRPC. */
    public static final String HEALTH_SERVICE_NAME = "grpc.health.v1.Health";

    private final HealthStatusManager manager = new HealthStatusManager();

    /** Сервис для регистрации в {@code ServerBuilder.addService}. */
    public BindableService service() {
        return manager.getHealthService();
    }

    /** Сервер готов принимать вызовы. */
    public void serving() {
        manager.setStatus(HealthStatusManager.SERVICE_NAME_ALL_SERVICES,
                HealthCheckResponse.ServingStatus.SERVING);
    }

    public void serving(String serviceName) {
        manager.setStatus(serviceName, HealthCheckResponse.ServingStatus.SERVING);
    }

    /** Сервер жив, но обслуживать вызовы не может. */
    public void notServing() {
        manager.setStatus(HealthStatusManager.SERVICE_NAME_ALL_SERVICES,
                HealthCheckResponse.ServingStatus.NOT_SERVING);
    }

    public void notServing(String serviceName) {
        manager.setStatus(serviceName, HealthCheckResponse.ServingStatus.NOT_SERVING);
    }

    /**
     * Терминальное состояние при остановке: статус снимается совсем, и проба уводит трафик
     * с узла, а не ждет истечения таймаутов.
     */
    public void shutdown() {
        manager.enterTerminalState();
    }
}
