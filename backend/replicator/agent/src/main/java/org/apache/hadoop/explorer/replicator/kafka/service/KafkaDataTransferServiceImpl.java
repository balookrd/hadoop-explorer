package org.apache.hadoop.explorer.replicator.kafka.service;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.generated.*;
import org.apache.hadoop.explorer.replicator.kafka.offset.SafetyCommitManager;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.apache.hadoop.explorer.replicator.kafka.schema.CachedSchemaRegistryResolver;
import org.apache.hadoop.explorer.replicator.kafka.schema.SchemaIdResolver;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.HwxRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Реализация gRPC-сервиса KafkaDataTransferService на узле-приемнике.
 * <p>
 * Обеспечивает высокопроизводительный потоковый прием пакетов записей с поддержкой
 * скользящего окна упреждения (Pipelining / Sliding Window Flow Control),
 * вызов безопасного коммита оффсетов, авторегистрацию схем и heartbeat.
 */
public class KafkaDataTransferServiceImpl extends KafkaDataTransferServiceGrpc.KafkaDataTransferServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(KafkaDataTransferServiceImpl.class);
    public static final int DEFAULT_PIPELINE_WINDOW = 16;

    private final KafkaBatchProducer batchProducer;
    private final SafetyCommitManager safetyCommitManager;
    private final String localDcId;
    private final int pipelineWindow;
    private final SchemaIdResolver schemaIdResolver;
    private final ConfluentRegistryClient targetConfluentClient;
    private final HwxRegistryClient targetHwxClient;

    public KafkaDataTransferServiceImpl(KafkaBatchProducer batchProducer,
                                        SafetyCommitManager safetyCommitManager,
                                        String localDcId) {
        this(batchProducer, safetyCommitManager, localDcId, DEFAULT_PIPELINE_WINDOW, null, null, null);
    }

    public KafkaDataTransferServiceImpl(KafkaBatchProducer batchProducer,
                                        SafetyCommitManager safetyCommitManager,
                                        String localDcId,
                                        int pipelineWindow) {
        this(batchProducer, safetyCommitManager, localDcId, pipelineWindow, null, null, null);
    }

    public KafkaDataTransferServiceImpl(KafkaBatchProducer batchProducer,
                                        SafetyCommitManager safetyCommitManager,
                                        String localDcId,
                                        int pipelineWindow,
                                        SchemaIdResolver schemaIdResolver) {
        this(batchProducer, safetyCommitManager, localDcId, pipelineWindow, schemaIdResolver,
                schemaIdResolver instanceof CachedSchemaRegistryResolver csrr ? csrr.getTargetConfluentClient() : null,
                schemaIdResolver instanceof CachedSchemaRegistryResolver csrr ? csrr.getTargetHwxClient() : null);
    }

    public KafkaDataTransferServiceImpl(KafkaBatchProducer batchProducer,
                                        SafetyCommitManager safetyCommitManager,
                                        String localDcId,
                                        int pipelineWindow,
                                        SchemaIdResolver schemaIdResolver,
                                        ConfluentRegistryClient targetConfluentClient,
                                        HwxRegistryClient targetHwxClient) {
        this.batchProducer = Objects.requireNonNull(batchProducer, "batchProducer must not be null");
        this.safetyCommitManager = safetyCommitManager;
        this.localDcId = localDcId != null ? localDcId : "LOCAL_DC";
        this.pipelineWindow = Math.max(1, pipelineWindow);
        this.schemaIdResolver = schemaIdResolver;
        this.targetConfluentClient = targetConfluentClient != null ? targetConfluentClient :
                (schemaIdResolver instanceof CachedSchemaRegistryResolver csrr ? csrr.getTargetConfluentClient() : null);
        this.targetHwxClient = targetHwxClient != null ? targetHwxClient :
                (schemaIdResolver instanceof CachedSchemaRegistryResolver csrr ? csrr.getTargetHwxClient() : null);
    }

    @Override
    public StreamObserver<KafkaBatchRequest> streamBatches(StreamObserver<KafkaBatchAck> responseObserver) {
        final ServerCallStreamObserver<KafkaBatchAck> serverCallStreamObserver =
                (responseObserver instanceof ServerCallStreamObserver<KafkaBatchAck> scso) ? scso : null;

        if (serverCallStreamObserver != null) {
            serverCallStreamObserver.disableAutoInboundFlowControl();
            serverCallStreamObserver.setOnCancelHandler(() -> {
                log.warn("Клиент прервал (CANCELLED) gRPC соединение streamBatches");
            });
            // Инициализируем скользящее окно (sliding window) упреждающих пакетов для устранения stop-and-wait задержек
            serverCallStreamObserver.request(pipelineWindow);
            log.debug("gRPC streamBatches: инициализирован конвейер со скользящим окном pipelineWindow={}", pipelineWindow);
        }

        final java.util.concurrent.atomic.AtomicInteger inFlightBatches = new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicBoolean clientCompleted = new java.util.concurrent.atomic.AtomicBoolean(false);
        final ReentrantLock responseLock = new ReentrantLock();

        return new StreamObserver<>() {
            @Override
            public void onNext(KafkaBatchRequest batch) {
                log.trace("Получен входящий gRPC пакет {} (топик {}, {} записей)",
                        batch.getBatchId(), batch.getTopic(), batch.getRecordsCount());

                inFlightBatches.incrementAndGet();
                batchProducer.writeBatchAsync(batch).thenAccept(ack -> {
                    responseLock.lock();
                    try {
                        responseObserver.onNext(ack);
                    } finally {
                        responseLock.unlock();
                    }
                    if (serverCallStreamObserver != null && !serverCallStreamObserver.isCancelled()) {
                        serverCallStreamObserver.request(1);
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
                    responseLock.lock();
                    try {
                        responseObserver.onNext(errorAck);
                    } finally {
                        responseLock.unlock();
                    }
                    if (serverCallStreamObserver != null && !serverCallStreamObserver.isCancelled()) {
                        serverCallStreamObserver.request(1);
                    }
                    checkCompletion();
                    return null;
                });
            }

            // flush() здесь не нужен и опасен: ack уходит только после подтверждения записей
            // брокером, а цепочка подтверждения может выполняться в сетевом потоке продюсера,
            // где KafkaProducer.flush() ждет сам себя и останавливает запись навсегда
            private void checkCompletion() {
                if (inFlightBatches.decrementAndGet() == 0 && clientCompleted.get()) {
                    responseLock.lock();
                    try {
                        responseObserver.onCompleted();
                    } finally {
                        responseLock.unlock();
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Ошибка в gRPC потоке streamBatches: {}", t.getMessage());
                responseLock.lock();
                try {
                    responseObserver.onError(t);
                } finally {
                    responseLock.unlock();
                }
            }

            @Override
            public void onCompleted() {
                clientCompleted.set(true);
                if (inFlightBatches.get() == 0) {
                    responseLock.lock();
                    try {
                        responseObserver.onCompleted();
                    } finally {
                        responseLock.unlock();
                    }
                }
            }
        };
    }

    @Override
    public void syncSchemaMetadata(SchemaSyncRequest request, StreamObserver<SchemaSyncResponse> responseObserver) {
        String subject = request.getSubject();
        String schemaText = request.getSchemaText();
        long sourceSchemaId = request.getSourceSchemaId();
        String targetRegType = request.getTargetRegistryType();

        log.info("Получен запрос предварительной синхронизации схемы: subject='{}', sourceId={}, srcType='{}', targetType='{}'",
                subject, sourceSchemaId, request.getSourceRegistryType(), targetRegType);

        try {
            long targetSchemaId;

            // Определяем целевой тип реестра
            boolean isConfluentTarget = "CONFLUENT".equalsIgnoreCase(targetRegType) ||
                    (targetRegType.isBlank() && targetConfluentClient != null);
            boolean isHwxTarget = "HORTONWORKS".equalsIgnoreCase(targetRegType) ||
                    (targetRegType.isBlank() && targetHwxClient != null && !isConfluentTarget);

            if (schemaText != null && !schemaText.isBlank()) {
                if (isConfluentTarget && targetConfluentClient != null) {
                    int assignedId = targetConfluentClient.registerSchema(subject, schemaText);
                    targetSchemaId = assignedId;
                    if (schemaIdResolver instanceof CachedSchemaRegistryResolver csrr) {
                        if ("HORTONWORKS".equalsIgnoreCase(request.getSourceRegistryType())) {
                            csrr.putHwxToConfluent(sourceSchemaId, assignedId);
                        } else {
                            csrr.putConfluentToConfluent((int) sourceSchemaId, assignedId);
                        }
                    }
                    log.info("Схема для subject '{}' успешно зарегистрирована в целевом Confluent Schema Registry (id={})",
                            subject, assignedId);
                } else if (isHwxTarget && targetHwxClient != null) {
                    long assignedVersionId = targetHwxClient.registerSchemaVersion(subject, schemaText);
                    targetSchemaId = assignedVersionId;
                    if (schemaIdResolver instanceof CachedSchemaRegistryResolver csrr) {
                        if ("HORTONWORKS".equalsIgnoreCase(request.getSourceRegistryType())) {
                            csrr.putHwxToHwx(sourceSchemaId, assignedVersionId);
                        } else {
                            csrr.putHwxToConfluent(assignedVersionId, (int) sourceSchemaId);
                        }
                    }
                    log.info("Схема для subject '{}' успешно зарегистрирована в целевом Hortonworks Schema Registry (versionId={})",
                            subject, assignedVersionId);
                } else {
                    // Клиент реестра не сконфигурирован — graceful fallback (сохраняем исходный ID)
                    targetSchemaId = sourceSchemaId;
                    log.debug("Клиенты целевого реестра схем не сконфигурированы на данном узле, используется passthrough ID: {}", targetSchemaId);
                }
            } else {
                targetSchemaId = sourceSchemaId;
            }

            responseObserver.onNext(SchemaSyncResponse.newBuilder()
                    .setSubject(subject)
                    .setSuccess(true)
                    .setTargetSchemaId(targetSchemaId)
                    .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            String errorMsg = "Ошибка регистрации схемы в целевом реестре для subject '" + subject + "': " + e.getMessage();
            log.error(errorMsg, e);
            responseObserver.onNext(SchemaSyncResponse.newBuilder()
                    .setSubject(subject)
                    .setSuccess(false)
                    .setTargetSchemaId(-1)
                    .setErrorMessage(errorMsg)
                    .build());
            responseObserver.onCompleted();
        }
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
