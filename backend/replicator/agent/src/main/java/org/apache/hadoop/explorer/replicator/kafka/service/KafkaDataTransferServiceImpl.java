package org.apache.hadoop.explorer.replicator.kafka.service;

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
        return new StreamObserver<>() {
            @Override
            public void onNext(KafkaBatchRequest batch) {
                log.trace("Получен входящий gRPC пакет {} (топик {}, {} записей)",
                        batch.getBatchId(), batch.getTopic(), batch.getRecordsCount());

                KafkaBatchAck ack = batchProducer.writeBatch(batch);
                responseObserver.onNext(ack);
            }

            @Override
            public void onError(Throwable t) {
                log.error("Ошибка в gRPC потоке streamBatches: {}", t.getMessage());
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                batchProducer.flush();
                responseObserver.onCompleted();
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
