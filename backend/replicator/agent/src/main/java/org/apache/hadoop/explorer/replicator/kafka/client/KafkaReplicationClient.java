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
    private final KafkaTargetBalancer balancer;
    private final List<ManagedChannel> channels;
    private final List<KafkaDataTransferServiceGrpc.KafkaDataTransferServiceStub> asyncStubs;
    private final List<KafkaDataTransferServiceGrpc.KafkaDataTransferServiceBlockingStub> blockingStubs;

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
        if (targetEndpoints == null || targetEndpoints.isEmpty()) {
            throw new IllegalArgumentException("targetEndpoints list must not be null or empty");
        }
        this.agentSecret = agentSecret;
        this.balancer = new KafkaTargetBalancer(targetEndpoints);
        this.channels = new ArrayList<>();
        this.asyncStubs = new ArrayList<>();
        this.blockingStubs = new ArrayList<>();

        io.grpc.ClientInterceptor authInterceptor = (agentSecret != null && !agentSecret.isBlank())
                ? org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor.createClientInterceptor(agentSecret)
                : null;

        for (String ep : targetEndpoints) {
            String[] parts = ep.trim().split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 50052;

            ManagedChannel ch = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext() // В продакшене mTLS: .useTransportSecurity()
                    .maxInboundMessageSize(64 * 1024 * 1024)
                    .build();

            this.channels.add(ch);
            io.grpc.Channel targetChannel = (authInterceptor != null)
                    ? io.grpc.ClientInterceptors.intercept(ch, authInterceptor)
                    : ch;

            this.asyncStubs.add(KafkaDataTransferServiceGrpc.newStub(targetChannel));
            this.blockingStubs.add(KafkaDataTransferServiceGrpc.newBlockingStub(targetChannel));
        }
        log.info("Инициализирован KafkaReplicationClient с {} целевыми узлами: {} (auth={})",
                channels.size(), targetEndpoints, (agentSecret != null && !agentSecret.isBlank()));
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
        this.balancer = new KafkaTargetBalancer(endpointNames);
        this.channels = new ArrayList<>(channels);
        this.asyncStubs = new ArrayList<>();
        this.blockingStubs = new ArrayList<>();

        io.grpc.ClientInterceptor authInterceptor = (agentSecret != null && !agentSecret.isBlank())
                ? org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor.createClientInterceptor(agentSecret)
                : null;

        for (ManagedChannel ch : channels) {
            io.grpc.Channel targetChannel = (authInterceptor != null)
                    ? io.grpc.ClientInterceptors.intercept(ch, authInterceptor)
                    : ch;
            this.asyncStubs.add(KafkaDataTransferServiceGrpc.newStub(targetChannel));
            this.blockingStubs.add(KafkaDataTransferServiceGrpc.newBlockingStub(targetChannel));
        }
    }

    public KafkaTargetBalancer getBalancer() {
        return balancer;
    }

    public int getEndpointCount() {
        return channels.size();
    }

    public List<String> getTargetEndpoints() {
        return balancer.getEndpoints();
    }

    /**
     * Определяет индекс целевого агента-приемника для заданной партиции.
     */
    public int selectTargetIndex(int partition) {
        return balancer.selectTargetIndex(partition);
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
        int idx = Math.max(0, Math.min(targetIndex, asyncStubs.size() - 1));
        return asyncStubs.get(idx).streamBatches(ackObserver);
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

        return blockingStubs.get(targetIdx)
                .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                .syncConsumerOffsets(request);
    }

    /**
     * Проверяет доступность удаленного агента и замеряет RTT (на первом доступном узле).
     */
    public long ping(String localDcId) {
        return pingTarget(0, localDcId);
    }

    public long pingTarget(int targetIndex, String localDcId) {
        int idx = Math.max(0, Math.min(targetIndex, blockingStubs.size() - 1));
        long sendTime = System.currentTimeMillis();
        KafkaPing ping = KafkaPing.newBuilder()
                .setSenderId(localDcId)
                .setTimestampMs(sendTime)
                .build();

        KafkaPong pong = blockingStubs.get(idx).withDeadlineAfter(3, TimeUnit.SECONDS).heartbeat(ping);
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
        for (int i = 0; i < blockingStubs.size(); i++) {
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
        for (ManagedChannel ch : channels) {
            try {
                ch.shutdown().awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception ignored) {
            }
        }
    }
}
