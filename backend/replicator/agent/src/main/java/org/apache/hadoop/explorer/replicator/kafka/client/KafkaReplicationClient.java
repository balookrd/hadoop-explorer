package org.apache.hadoop.explorer.replicator.kafka.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.generated.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Клиент репликации Kafka через gRPC с поддержкой пула каналов (Multi-Target Endpoint Pool),
 * двунаправленного стриминга батчей, балансировки по партициям (Partition-Pinned Routing)
 * и синхронизации смещений.
 */
public class KafkaReplicationClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaReplicationClient.class);

    private final String agentSecret;
    private final boolean tlsEnabled;
    private final String trustCertPath;
    private volatile KafkaTargetBalancer balancer;

    /**
     * Иммутабельный снимок пула приемников.
     *
     * <p>Публикуется целиком одной записью в {@link #pool}: читатели никогда не видят
     * промежуточного состояния. Правка списков на месте ломала воркеров
     * {@code IndexOutOfBoundsException} в момент перестройки топологии.
     */
    private record EndpointPool(
            List<ManagedChannel> channels,
            List<KafkaDataTransferServiceGrpc.KafkaDataTransferServiceStub> asyncStubs,
            List<KafkaDataTransferServiceGrpc.KafkaDataTransferServiceBlockingStub> blockingStubs
    ) {
        int size() {
            return channels.size();
        }
    }

    private volatile EndpointPool pool;

    /** Маршруты «топик — эндпоинты его задачи»; пусто означает «весь пул». */
    private volatile java.util.Map<String, java.util.List<String>> topicRoutes = java.util.Map.of();

    /**
     * Конструктор для одиночного удаленного хоста (обратная совместимость).
     */
    public KafkaReplicationClient(String targetHost, int targetPort) {
        this(List.of(Objects.requireNonNull(targetHost, "targetHost must not be null") + ":" + targetPort));
    }

    /**
     * Конструктор для одиночного канала (для smoke-тестов и in-process тестов).
     */
    public KafkaReplicationClient(ManagedChannel channel) {
        this(List.of(Objects.requireNonNull(channel, "channel must not be null")), List.of("in-process:0"));
    }

    /**
     * Конструктор для пула целевых эндпоинтов (формат "host:port").
     * Создает независимые gRPC каналы с оптимизацией буферов для каждого узла.
     */
    public KafkaReplicationClient(List<String> targetEndpoints) {
        this(targetEndpoints, null);
    }

    public KafkaReplicationClient(List<String> targetEndpoints, String agentSecret) {
        this(targetEndpoints, agentSecret, false, null);
    }

    public KafkaReplicationClient(List<String> targetEndpoints, String agentSecret, boolean tlsEnabled, String trustCertPath) {
        if (targetEndpoints == null || targetEndpoints.isEmpty()) {
            throw new IllegalArgumentException("targetEndpoints list must not be null or empty");
        }
        this.agentSecret = agentSecret;
        this.tlsEnabled = tlsEnabled;
        this.trustCertPath = trustCertPath;
        this.balancer = new KafkaTargetBalancer(targetEndpoints);
        this.pool = buildPool(targetEndpoints);
        log.info("Инициализирован KafkaReplicationClient с {} целевыми узлами: {} (auth={}, tls={})",
                pool.size(), targetEndpoints, (agentSecret != null && !agentSecret.isBlank()), tlsEnabled);
    }

    /** Сборка нового снимка пула по списку эндпоинтов (без публикации). */
    private EndpointPool buildPool(List<String> endpoints) {
        io.grpc.ClientInterceptor authInterceptor = (agentSecret != null && !agentSecret.isBlank())
                ? org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor.createClientInterceptor(agentSecret)
                : null;

        List<ManagedChannel> newChannels = new ArrayList<>(endpoints.size());
        List<KafkaDataTransferServiceGrpc.KafkaDataTransferServiceStub> newAsync = new ArrayList<>(endpoints.size());
        List<KafkaDataTransferServiceGrpc.KafkaDataTransferServiceBlockingStub> newBlocking = new ArrayList<>(endpoints.size());

        for (String ep : endpoints) {
            ManagedChannel ch = createChannel(ep);
            // Перехватчик трассировки ставится всегда, аутентификация — если секрет задан
            io.grpc.Channel targetChannel = (authInterceptor != null)
                    ? io.grpc.ClientInterceptors.intercept(ch, authInterceptor, org.apache.hadoop.explorer.replicator.telemetry.GrpcTracing.clientInterceptor())
                    : io.grpc.ClientInterceptors.intercept(ch, org.apache.hadoop.explorer.replicator.telemetry.GrpcTracing.clientInterceptor());
            newChannels.add(ch);
            newAsync.add(KafkaDataTransferServiceGrpc.newStub(targetChannel));
            newBlocking.add(KafkaDataTransferServiceGrpc.newBlockingStub(targetChannel));
        }
        return new EndpointPool(List.copyOf(newChannels), List.copyOf(newAsync), List.copyOf(newBlocking));
    }

    private ManagedChannel createChannel(String ep) {
        String[] parts = ep.trim().split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 50052;

        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port)
                .maxInboundMessageSize(org.apache.hadoop.explorer.replicator.kafka.config.KafkaClientSettings.GRPC_MAX_MESSAGE_BYTES);

        if (tlsEnabled) {
            if (trustCertPath != null && !trustCertPath.isBlank()) {
                java.io.File trustFile = new java.io.File(trustCertPath);
                if (trustFile.exists() && trustFile.canRead()) {
                    builder.useTransportSecurity();
                } else {
                    log.warn("Сертификат доверия TLS не найден по пути: {}, используется системный TLS trust", trustCertPath);
                    builder.useTransportSecurity();
                }
            } else {
                builder.useTransportSecurity();
            }
        } else {
            builder.usePlaintext();
        }

        return builder.build();
    }

    /**
     * Динамическое обновление пула целевых агентов-приемников без перезапуска агента-отправителя.
     */
    public synchronized void updateEndpoints(List<String> newEndpoints) {
        if (newEndpoints == null || newEndpoints.isEmpty()) {
            return;
        }
        List<String> current = balancer.getEndpoints();
        if (current.equals(newEndpoints)) {
            return;
        }
        log.info("[KafkaReplicationClient] Обновление пула целевых эндпоинтов: {} -> {}", current, newEndpoints);

        // Новый пул собирается полностью, и только потом публикуется одной записью.
        // Старые каналы закрываются после публикации, чтобы читатели не остались
        // ни с пустым пулом, ни со закрытым каналом в руках раньше времени.
        EndpointPool previous = this.pool;
        EndpointPool replacement = buildPool(newEndpoints);
        this.balancer = new KafkaTargetBalancer(newEndpoints);
        this.pool = replacement;

        if (previous != null) {
            for (ManagedChannel ch : previous.channels()) {
                try {
                    ch.shutdown();
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Конструктор для пула готовых ManagedChannel (используется для in-process тестов мульти-агентности).
     */
    public KafkaReplicationClient(List<ManagedChannel> channels, List<String> endpointNames) {
        this(channels, endpointNames, null);
    }

    public KafkaReplicationClient(List<ManagedChannel> channels, List<String> endpointNames, String agentSecret) {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("channels list must not be null or empty");
        }
        if (endpointNames == null || endpointNames.size() != channels.size()) {
            throw new IllegalArgumentException("endpointNames must match channels size");
        }
        this.agentSecret = agentSecret;
        this.tlsEnabled = false;
        this.trustCertPath = null;
        this.balancer = new KafkaTargetBalancer(endpointNames);

        io.grpc.ClientInterceptor authInterceptor = (agentSecret != null && !agentSecret.isBlank())
                ? org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor.createClientInterceptor(agentSecret)
                : null;

        List<KafkaDataTransferServiceGrpc.KafkaDataTransferServiceStub> newAsync = new ArrayList<>(channels.size());
        List<KafkaDataTransferServiceGrpc.KafkaDataTransferServiceBlockingStub> newBlocking = new ArrayList<>(channels.size());
        for (ManagedChannel ch : channels) {
            // Перехватчик трассировки ставится всегда, аутентификация — если секрет задан
            io.grpc.Channel targetChannel = (authInterceptor != null)
                    ? io.grpc.ClientInterceptors.intercept(ch, authInterceptor, org.apache.hadoop.explorer.replicator.telemetry.GrpcTracing.clientInterceptor())
                    : io.grpc.ClientInterceptors.intercept(ch, org.apache.hadoop.explorer.replicator.telemetry.GrpcTracing.clientInterceptor());
            newAsync.add(KafkaDataTransferServiceGrpc.newStub(targetChannel));
            newBlocking.add(KafkaDataTransferServiceGrpc.newBlockingStub(targetChannel));
        }
        this.pool = new EndpointPool(List.copyOf(channels), List.copyOf(newAsync), List.copyOf(newBlocking));
    }

    public KafkaTargetBalancer getBalancer() {
        return balancer;
    }

    public int getEndpointCount() {
        return pool.size();
    }

    public List<String> getTargetEndpoints() {
        return balancer.getEndpoints();
    }

    /**
     * Определяет индекс целевого агента-приемника для заданной партиции.
     */
    /** Приемник для батча не выбран: отправлять некуда. */
    public static final int NO_TARGET = -1;

    public int selectTargetIndex(int partition) {
        return balancer.selectTargetIndex(partition);
    }

    /**
     * Маршруты «топик — целевые эндпоинты его задачи».
     *
     * <p>Пул приемников общий для агента, но выбор приемника ограничен эндпоинтами той задачи,
     * которой принадлежит топик. Без этого HRW-балансировщик раскидывал партиции топика по
     * всем приемникам ЦОД, и при двух задачах с разными целевыми кластерами часть данных
     * уезжала в чужой кластер, а в нужный не доезжала.
     */
    public void updateTopicRoutes(java.util.Map<String, java.util.List<String>> topicToEndpoints) {
        if (topicToEndpoints == null || topicToEndpoints.isEmpty()) {
            this.topicRoutes = java.util.Map.of();
            return;
        }
        java.util.Map<String, java.util.List<String>> routes = new java.util.HashMap<>();
        topicToEndpoints.forEach((topic, endpoints) -> {
            if (topic != null && !topic.isBlank() && endpoints != null && !endpoints.isEmpty()) {
                routes.put(topic, java.util.List.copyOf(endpoints));
            }
        });
        this.topicRoutes = java.util.Map.copyOf(routes);
    }

    /**
     * Индекс приемника для партиции конкретного топика с учетом маршрута его задачи.
     *
     * <p>Партиции внутри маршрута по-прежнему распределяются балансировщиком, так что
     * параллелизм задачи сохраняется. Топик без маршрута обслуживается всем пулом — поведение
     * как до появления маршрутов.
     */
    public int selectTargetIndex(String topic, int partition) {
        java.util.List<String> route = (topic != null) ? topicRoutes.get(topic) : null;
        if (route == null || route.isEmpty()) {
            return selectTargetIndex(partition);
        }

        java.util.List<String> poolEndpoints = balancer.getEndpoints();
        java.util.List<Integer> candidates = new java.util.ArrayList<>(route.size());
        for (int i = 0; i < poolEndpoints.size(); i++) {
            if (route.contains(poolEndpoints.get(i))) {
                candidates.add(i);
            }
        }
        if (candidates.isEmpty()) {
            // Fail-closed: приемников маршрута в пуле нет. Подстановка любого узла из пула
            // уводила бы данные маршрутизируемого топика в чужой кластер — ровно то, против
            // чего маршруты и вводились. Вызывающая сторона обязана задержать батч.
            return NO_TARGET;
        }
        // Выбор внутри маршрута детерминирован по номеру партиции: порядок партиции
        // сохраняется между вызовами
        return candidates.get(Math.floorMod(partition, candidates.size()));
    }

    /**
     * Помечает целевой узел как подозреваемый/сбойный (переводит в карантин).
     */
    public void markTargetSuspect(int targetIndex, Throwable cause) {
        balancer.markFailed(targetIndex, cause);
    }

    /**
     * Открывает двунаправленный стрим отправки батчей к узлу по умолчанию (индекс 0).
     */
    public StreamObserver<KafkaBatchRequest> openBatchStream(StreamObserver<KafkaBatchAck> ackObserver) {
        return openBatchStream(0, ackObserver);
    }

    /**
     * Открывает двунаправленный стрим отправки батчей к конкретному целевому узлу.
     */
    public StreamObserver<KafkaBatchRequest> openBatchStream(int targetIndex, StreamObserver<KafkaBatchAck> ackObserver) {
        EndpointPool current = pool;
        int idx = Math.max(0, Math.min(targetIndex, current.size() - 1));
        return current.asyncStubs().get(idx).streamBatches(ackObserver);
    }

    /**
     * Открывает двунаправленный стрим для конкретной партиции с учетом балансировки нагрузки.
     */
    public StreamObserver<KafkaBatchRequest> openBatchStreamForPartition(int partition, StreamObserver<KafkaBatchAck> ackObserver) {
        int targetIdx = selectTargetIndex(partition);
        return openBatchStream(targetIdx, ackObserver);
    }

    /**
     * Синхронная отправка одиночного батча на узел по умолчанию (smoke-тесты).
     */
    public KafkaBatchAck sendBatchSync(KafkaBatchRequest batch, long timeoutSeconds) throws Exception {
        return sendBatchSync(0, batch, timeoutSeconds);
    }

    /**
     * Синхронная отправка одиночного батча на указанный узел.
     */
    public KafkaBatchAck sendBatchSync(int targetIndex, KafkaBatchRequest batch, long timeoutSeconds) throws Exception {
        CompletableFuture<KafkaBatchAck> future = new CompletableFuture<>();
        StreamObserver<KafkaBatchRequest> reqStream = openBatchStream(targetIndex, new StreamObserver<>() {
            @Override
            public void onNext(KafkaBatchAck ack) {
                future.complete(ack);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
        });
        reqStream.onNext(batch);
        reqStream.onCompleted();
        return future.get(timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Синхронизирует смещения консьюмер-группы с удаленным кластером (вызывается на первом живом узле).
     */
    public OffsetSyncResponse syncOffsets(OffsetSyncRequest request, long timeoutSeconds) {
        List<Integer> healthy = balancer.getHealthyIndices();
        int targetIdx = healthy.isEmpty() ? 0 : healthy.get(0);

        EndpointPool current = pool;
        int idx = Math.max(0, Math.min(targetIdx, current.size() - 1));
        return current.blockingStubs().get(idx)
                .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                .syncConsumerOffsets(request);
    }

    /**
     * Синхронизирует метаданные схемы с целевым реестром через удаленный узел-приемник.
     */
    public SchemaSyncResponse syncSchema(SchemaSyncRequest request) {
        return syncSchemaTarget(0, request);
    }

    public SchemaSyncResponse syncSchemaTarget(int targetIndex, SchemaSyncRequest request) {
        EndpointPool current = pool;
        int idx = Math.max(0, Math.min(targetIndex, current.size() - 1));
        return current.blockingStubs().get(idx)
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .syncSchemaMetadata(request);
    }

    /**
     * Проверяет доступность удаленного агента и замеряет RTT (на первом доступном узле).
     */
    public long ping(String localDcId) {
        return pingTarget(0, localDcId);
    }

    public long pingTarget(int targetIndex, String localDcId) {
        EndpointPool current = pool;
        int idx = Math.max(0, Math.min(targetIndex, current.size() - 1));
        long sendTime = System.currentTimeMillis();
        KafkaPing ping = KafkaPing.newBuilder()
                .setSenderId(localDcId)
                .setTimestampMs(sendTime)
                .build();

        KafkaPong pong = current.blockingStubs().get(idx).withDeadlineAfter(3, TimeUnit.SECONDS).heartbeat(ping);
        long rtt = System.currentTimeMillis() - sendTime;
        balancer.markHealthy(idx);
        log.debug("Heartbeat с узлом #{} ({}) успешен (RTT: {} мс, удаленный ЦОД: {})",
                idx, balancer.getEndpoint(idx), rtt, pong.getReceiverId());
        return rtt;
    }

    /**
     * Проверяет связь со всеми сконфигурированными целевыми узлами.
     */
    public Map<String, Long> pingAll(String localDcId) {
        Map<String, Long> results = new LinkedHashMap<>();
        for (int i = 0; i < pool.size(); i++) {
            String ep = balancer.getEndpoint(i);
            try {
                long rtt = pingTarget(i, localDcId);
                results.put(ep, rtt);
            } catch (Exception e) {
                log.warn("Не удалось выполнить ping узла #{} ({}): {}", i, ep, e.getMessage());
                balancer.markFailed(i, e);
                results.put(ep, -1L);
            }
        }
        return results;
    }

    @Override
    public void close() {
        for (ManagedChannel ch : pool.channels()) {
            try {
                ch.shutdown().awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception ignored) {
            }
        }
    }
}
