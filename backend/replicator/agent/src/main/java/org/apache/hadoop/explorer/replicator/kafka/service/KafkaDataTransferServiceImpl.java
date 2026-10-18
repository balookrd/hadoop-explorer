package org.apache.hadoop.explorer.replicator.kafka.service;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.generated.*;
import org.apache.hadoop.explorer.replicator.kafka.offset.SafetyCommitManager;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Реализация gRPC-сервиса KafkaDataTransferService на узле-приемнике.
 * <p>
 * Обеспечивает прием потоковых пакетов записей, вызов безопасного коммита оффсетов и heartbeat.
 */
public class KafkaDataTransferServiceImpl extends KafkaDataTransferServiceGrpc.KafkaDataTransferServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(KafkaDataTransferServiceImpl.class);

    private final KafkaBatchProducer batchProducer;
    private final SafetyCommitManager safetyCommitManager;
    private final String localDcId;

    public KafkaDataTransferServiceImpl(KafkaBatchProducer batchProducer,
                                       SafetyCommitManager safetyCommitManager,
                                       String localDcId) {
        this.batchProducer = Objects.requireNonNull(batchProducer, "batchProducer must not be null");
        this.safetyCommitManager = safetyCommitManager;
        this.localDcId = localDcId != null ? localDcId : "LOCAL_DC";
    }

    @Override
    public StreamObserver<KafkaBatchRequest> streamBatches(StreamObserver<KafkaBatchAck> responseObserver) {
        if (responseObserver instanceof ServerCallStreamObserver<KafkaBatchAck> serverCallStreamObserver) {
            serverCallStreamObserver.setOnCancelHandler(() -> {
                log.warn("Клиент прервал (CANCELLED) gRPC соединение streamBatches");
            });
        }

        final java.util.concurrent.atomic.AtomicInteger inFlightBatches = new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicBoolean clientCompleted = new java.util.concurrent.atomic.AtomicBoolean(false);

        return new StreamObserver<>() {
            @Override
            public void onNext(KafkaBatchRequest batch) {
                log.trace("Получен входящий gRPC пакет {} (топик {}, {} записей)",
                        batch.getBatchId(), batch.getTopic(), batch.getRecordsCount());

                inFlightBatches.incrementAndGet();
                batchProducer.writeBatchAsync(batch).thenAccept(ack -> {
                    synchronized (responseObserver) {
                        responseObserver.onNext(ack);
                    }
                    checkCompletion();
                }).exceptionally(t -> {
                    log.error("Сбой асинхронной обработки пакета {}: {}", batch.getBatchId(), t.getMessage(), t);
                    KafkaBatchAck errorAck = KafkaBatchAck.newBuilder()
                            .setBatchId(batch.getBatchId())
                            .setTopic(batch.getTopic())
                            .setSuccess(false)
                            .setErrorMessage(t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName())
                            .setReceiveTimestampMs(System.currentTimeMillis())
                            .build();
                    synchronized (responseObserver) {
                        responseObserver.onNext(errorAck);
                    }
                    checkCompletion();
                    return null;
                });
            }

            private void checkCompletion() {
                if (inFlightBatches.decrementAndGet() == 0 && clientCompleted.get()) {
                    batchProducer.flush();
                    synchronized (responseObserver) {
                        responseObserver.onCompleted();
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Ошибка в gRPC потоке streamBatches: {}", t.getMessage());
                synchronized (responseObserver) {
                    responseObserver.onError(t);
                }
            }

            @Override
            public void onCompleted() {
                clientCompleted.set(true);
                if (inFlightBatches.get() == 0) {
                    batchProducer.flush();
                    synchronized (responseObserver) {
                        responseObserver.onCompleted();
                    }
                }
            }
        };
    }

    @Override
    public void syncConsumerOffsets(OffsetSyncRequest request, StreamObserver<OffsetSyncResponse> responseObserver) {
        if (safetyCommitManager == null) {
            responseObserver.onNext(OffsetSyncResponse.newBuilder()
                    .setGroupId(request.getGroupId())
                    .setSuccess(false)
                    .setErrorMessage("SafetyCommitManager is not configured on this node")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        OffsetSyncResponse response = safetyCommitManager.applyOffsetsSafely(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(KafkaPing request, StreamObserver<KafkaPong> responseObserver) {
        long now = System.currentTimeMillis();
        KafkaPong pong = KafkaPong.newBuilder()
                .setReceiverId(localDcId)
                .setPingTimestampMs(request.getTimestampMs())
                .setPongTimestampMs(now)
                .build();

        responseObserver.onNext(pong);
        responseObserver.onCompleted();
    }
}
