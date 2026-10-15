package org.apache.hadoop.explorer.replicator.kafka.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.generated.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Клиент репликации Kafka через gRPC с поддержкой двунаправленного стриминга батчей и синхронизации смещений.
 */
public class KafkaReplicationClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaReplicationClient.class);

    private final String targetHost;
    private final int targetPort;
    private final ManagedChannel channel;
    private final KafkaDataTransferServiceGrpc.KafkaDataTransferServiceStub asyncStub;
    private final KafkaDataTransferServiceGrpc.KafkaDataTransferServiceBlockingStub blockingStub;

    public KafkaReplicationClient(String targetHost, int targetPort) {
        this.targetHost = Objects.requireNonNull(targetHost, "targetHost must not be null");
        this.targetPort = targetPort;
        this.channel = ManagedChannelBuilder.forAddress(targetHost, targetPort)
                .usePlaintext() // В продакшене переключается на mTLS .useTransportSecurity()
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build();
        this.asyncStub = KafkaDataTransferServiceGrpc.newStub(channel);
        this.blockingStub = KafkaDataTransferServiceGrpc.newBlockingStub(channel);
    }

    public KafkaReplicationClient(ManagedChannel channel) {
        this.channel = Objects.requireNonNull(channel, "channel must not be null");
        this.targetHost = "in-process";
        this.targetPort = 0;
        this.asyncStub = KafkaDataTransferServiceGrpc.newStub(channel);
        this.blockingStub = KafkaDataTransferServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Открывает двунаправленный стрим отправки батчей записей с получением подтверждений.
     *
     * @param ackObserver обработчик подтверждений от удаленного узла
     * @return отправляющий StreamObserver для пуша батчей
     */
    public StreamObserver<KafkaBatchRequest> openBatchStream(StreamObserver<KafkaBatchAck> ackObserver) {
        return asyncStub.streamBatches(ackObserver);
    }

    /**
     * Синхронная отправка одиночного батча (удобно для Smoke-тестов и проверок связи).
     */
    public KafkaBatchAck sendBatchSync(KafkaBatchRequest batch, long timeoutSeconds) throws Exception {
        CompletableFuture<KafkaBatchAck> future = new CompletableFuture<>();
        StreamObserver<KafkaBatchRequest> reqStream = openBatchStream(new StreamObserver<>() {
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
     * Синхронизирует смещения консьюмер-группы с удаленным кластером (блокирующий вызов с таймаутом).
     */
    public OffsetSyncResponse syncOffsets(OffsetSyncRequest request, long timeoutSeconds) {
        return blockingStub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                .syncConsumerOffsets(request);
    }

    /**
     * Проверяет доступность удаленного агента и замеряет RTT.
     */
    public long ping(String localDcId) {
        long sendTime = System.currentTimeMillis();
        KafkaPing ping = KafkaPing.newBuilder()
                .setSenderId(localDcId)
                .setTimestampMs(sendTime)
                .build();

        KafkaPong pong = blockingStub.withDeadlineAfter(3, TimeUnit.SECONDS).heartbeat(ping);
        long rtt = System.currentTimeMillis() - sendTime;
        log.debug("Heartbeat с узлом {}:{} успешен (RTT: {} мс, удаленный ЦОД: {})",
                targetHost, targetPort, rtt, pong.getReceiverId());
        return rtt;
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
