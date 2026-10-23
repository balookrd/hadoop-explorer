package org.apache.hadoop.explorer.replicator.kafka.consumer;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecord;
import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier;
import org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Высокопроизводительный многопоточный пул консьюмеров для непрерывного чтения сообщений
 * из локальной Kafka, шардирования партиций через Consumer Group, фильтрации циклов (Loop Prevention),
 * транскодинга схем и стриминга батчей в удаленный ДЦ с балансировкой по партициям (Partition-Pinned Routing).
 */
public class KafkaBatchConsumerPool implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchConsumerPool.class);

    private final List<KafkaConsumer<byte[], byte[]>> consumers;
    private final KafkaReplicationClient replicationClient;
    private final LoopPreventionFilter loopFilter;
    private final PayloadFormatClassifier formatClassifier;
    private final ZeroCopyWireTranscoder transcoder;

    private final String sourceDcId;
    private final String targetDcId;
    private final PayloadFormat defaultTopicFormat;
    private final PayloadFormat targetFormat;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<TopicPartition, Long> lastObservedTimestamps = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong totalMessagesReplicated = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong totalBytesReplicated = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong currentLagMs = new java.util.concurrent.atomic.AtomicLong(0);

    private int maxBatchSize = 2000;
    private int maxInFlightBatches = 100;
    private ExecutorService executor;
    private volatile org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter bandwidthLimiter;
    private final java.util.concurrent.atomic.AtomicReference<List<String>> subscribedTopics =
            new java.util.concurrent.atomic.AtomicReference<>(Collections.emptyList());
    private final java.util.concurrent.atomic.AtomicLong topicsVersion =
            new java.util.concurrent.atomic.AtomicLong(1);

    public void setBandwidthLimiter(org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter bandwidthLimiter) {
        this.bandwidthLimiter = bandwidthLimiter;
    }

    public void setBandwidthLimitMb(double limitMb) {
        if (limitMb > 0) {
            if (this.bandwidthLimiter == null) {
                this.bandwidthLimiter = new org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter(limitMb);
            } else {
                this.bandwidthLimiter.setLimit(limitMb);
            }
        }
    }

    public org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter getBandwidthLimiter() {
        return bandwidthLimiter;
    }

    public KafkaBatchConsumerPool(KafkaConsumer<byte[], byte[]> consumer,
                                  KafkaReplicationClient replicationClient,
                                  LoopPreventionFilter loopFilter,
                                  PayloadFormatClassifier formatClassifier,
                                  ZeroCopyWireTranscoder transcoder,
                                  String sourceDcId,
                                  String targetDcId,
                                  PayloadFormat defaultTopicFormat,
                                  PayloadFormat targetFormat) {
        this(List.of(Objects.requireNonNull(consumer, "consumer must not be null")),
                replicationClient, loopFilter, formatClassifier, transcoder,
                sourceDcId, targetDcId, defaultTopicFormat, targetFormat);
    }

    public KafkaBatchConsumerPool(List<KafkaConsumer<byte[], byte[]>> consumers,
                                  KafkaReplicationClient replicationClient,
                                  LoopPreventionFilter loopFilter,
                                  PayloadFormatClassifier formatClassifier,
                                  ZeroCopyWireTranscoder transcoder,
                                  String sourceDcId,
                                  String targetDcId,
                                  PayloadFormat defaultTopicFormat,
                                  PayloadFormat targetFormat) {
        if (consumers == null || consumers.isEmpty()) {
            throw new IllegalArgumentException("consumers list must not be null or empty");
        }
        this.consumers = new ArrayList<>(consumers);
        this.replicationClient = Objects.requireNonNull(replicationClient, "replicationClient must not be null");
        this.loopFilter = Objects.requireNonNull(loopFilter, "loopFilter must not be null");
        this.formatClassifier = Objects.requireNonNull(formatClassifier, "formatClassifier must not be null");
        this.transcoder = Objects.requireNonNull(transcoder, "transcoder must not be null");
        this.sourceDcId = sourceDcId;
        this.targetDcId = targetDcId;
        this.defaultTopicFormat = defaultTopicFormat;
        this.targetFormat = targetFormat;

        Set<String> initialTopics = new HashSet<>();
        for (KafkaConsumer<byte[], byte[]> c : this.consumers) {
            try {
                Set<String> sub = c.subscription();
                if (sub != null) {
                    initialTopics.addAll(sub);
                }
            } catch (Exception ignored) {
            }
        }
        if (!initialTopics.isEmpty()) {
            this.subscribedTopics.set(new ArrayList<>(initialTopics));
        }
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxInFlightBatches(int maxInFlightBatches) {
        this.maxInFlightBatches = Math.max(1, maxInFlightBatches);
    }

    public int getMaxInFlightBatches() {
        return maxInFlightBatches;
    }

    public int getPoolSize() {
        return consumers.size();
    }

    /**
     * Запускает фоновый пул воркеров репликации в отдельном пуле потоков.
     */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Запуск пула KafkaBatchConsumerPool из {} воркеров ({} -> {}, batchSize={}, targets={})",
                    consumers.size(), sourceDcId, targetDcId, maxBatchSize, replicationClient.getEndpointCount());
            if (executor == null || executor.isShutdown()) {
                executor = Executors.newFixedThreadPool(consumers.size(), new ThreadFactory() {
                    private final AtomicInteger count = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "kafka-consumer-worker-" + count.getAndIncrement());
                        t.setDaemon(true);
                        return t;
                    }
                });
            }
            for (int i = 0; i < consumers.size(); i++) {
                executor.submit(new ConsumerWorker(i + 1, consumers.get(i)));
            }
        }
    }

    @Override
    public void run() {
        if (consumers.size() == 1 && executor == null) {
            running.set(true);
            new ConsumerWorker(1, consumers.get(0)).run();
        } else {
            start();
            while (running.get()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private class ConsumerWorker implements Runnable {
        private final int workerId;
        private final KafkaConsumer<byte[], byte[]> workerConsumer;
        private final Map<Integer, StreamObserver<KafkaBatchRequest>> activeStreams = new ConcurrentHashMap<>();

        private record InFlightBatch(String batchId, TopicPartition topicPartition, long maxSourceOffset, int recordCount, long bytesSize) {}
        private final Map<String, InFlightBatch> inFlightBatches = new ConcurrentHashMap<>();
        private final Map<TopicPartition, Long> pendingCommittedOffsets = new ConcurrentHashMap<>();
        private final AtomicInteger inFlightCount = new AtomicInteger(0);
        private long localTopicsVersion = 1;

        ConsumerWorker(int workerId, KafkaConsumer<byte[], byte[]> workerConsumer) {
            this.workerId = workerId;
            this.workerConsumer = workerConsumer;
        }

        private StreamObserver<KafkaBatchRequest> getOrCreateStream(int targetIdx) {
            return activeStreams.computeIfAbsent(targetIdx, idx -> {
                StreamObserver<KafkaBatchAck> ackObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(KafkaBatchAck ack) {
                        InFlightBatch inFlight = inFlightBatches.remove(ack.getBatchId());
                        if (inFlight != null) {
                            inFlightCount.decrementAndGet();
                        }
                        if (ack.getSuccess()) {
                            int records = inFlight != null ? inFlight.recordCount() : ack.getCommittedRecords();
                            long bytes = inFlight != null ? inFlight.bytesSize() : 0L;
                            totalMessagesReplicated.addAndGet(records);
                            totalBytesReplicated.addAndGet(bytes);

                            log.trace("[Worker {} -> Target {}] Успешно зафиксирован пакет {} в топике {}: {} записей ({} байт)",
                                    workerId, idx, ack.getBatchId(), ack.getTopic(), records, bytes);
                            if (inFlight != null) {
                                // Безопасное смещение: maxSourceOffset + 1 (At-least-once watermark)
                                pendingCommittedOffsets.merge(
                                        inFlight.topicPartition(),
                                        inFlight.maxSourceOffset() + 1,
                                        Math::max
                                );
                            }
                        } else {
                            log.error("[Worker {} -> Target {}] Удаленный узел вернул ошибку для пакета {}: {}",
                                    workerId, idx, ack.getBatchId(), ack.getErrorMessage());
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("[Worker {} -> Target {}] Сбой gRPC стрима: {}", workerId, idx, t.getMessage());
                        replicationClient.markTargetSuspect(idx, t);
                        activeStreams.remove(idx);
                    }

                    @Override
                    public void onCompleted() {
                        log.info("[Worker {} -> Target {}] gRPC стрим закрыт удаленным узлом", workerId, idx);
                        activeStreams.remove(idx);
                    }
                };

                if (replicationClient.getEndpointCount() <= 1) {
                    return replicationClient.openBatchStream(ackObserver);
                } else {
                    return replicationClient.openBatchStream(idx, ackObserver);
                }
            });
        }

        @Override
        public void run() {
            log.info("Запущен поток ConsumerWorker-{} ({} -> {})", workerId, sourceDcId, targetDcId);

            try {
                while (running.get()) {
                    // 1. Фиксируем оффсеты, успешно подтвержденные целевым узлом через Ack
                    drainAndCommitPendingOffsets();

                    // Контроль Backpressure: притормаживаем вычитку, если буфер in-flight переполнен
                    while (running.get() && inFlightCount.get() >= maxInFlightBatches) {
                        drainAndCommitPendingOffsets();
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    if (localTopicsVersion != topicsVersion.get()) {
                        localTopicsVersion = topicsVersion.get();
                        List<String> newTopics = subscribedTopics.get();
                        drainAndCommitPendingOffsets();
                        if (newTopics != null && !newTopics.isEmpty()) {
                            workerConsumer.subscribe(newTopics);
                            log.info("[Worker {}] Динамически переподписан на топики: {}", workerId, newTopics);
                        } else {
                            workerConsumer.unsubscribe();
                            log.info("[Worker {}] Отписан от топиков (список пуст)", workerId);
                        }
                    }


                    ConsumerRecords<byte[], byte[]> records;
                    try {
                        records = workerConsumer.poll(Duration.ofMillis(100));
                    } catch (WakeupException we) {
                        if (!running.get()) {
                            break;
                        }
                        if (localTopicsVersion != topicsVersion.get()) {
                            localTopicsVersion = topicsVersion.get();
                            List<String> newTopics = subscribedTopics.get();
                            drainAndCommitPendingOffsets();
                            if (newTopics != null && !newTopics.isEmpty()) {
                                workerConsumer.subscribe(newTopics);
                                log.info("[Worker {}] Обработан сигнал Wakeup: переподписка на топики: {}", workerId, newTopics);
                            } else {
                                workerConsumer.unsubscribe();
                                log.info("[Worker {}] Обработан сигнал Wakeup: отписка от топиков", workerId);
                            }
                        }
                        continue;
                    } catch (IllegalStateException ise) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        continue;
                    }
                    if (records.isEmpty()) {
                        currentLagMs.set(0);
                        continue;
                    }

                    long now = System.currentTimeMillis();
                    long maxRecordLag = 0;

                    // Группировка строго по TopicPartition для сохранения порядка при балансировке по агентам
                    Map<TopicPartition, List<KafkaRecord>> recordsByTp = new LinkedHashMap<>();

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        lastObservedTimestamps.put(tp, record.timestamp());
                        long recLag = Math.max(0, now - record.timestamp());
                        if (recLag > maxRecordLag) {
                            maxRecordLag = recLag;
                        }

                        // 1. Проверяем на бесконечную петлю (Loop / Echo)
                        if (!loopFilter.shouldReplicate(record)) {
                            continue;
                        }

                        // 2. Определяем формат и обрабатываем полезную нагрузку
                        byte[] originalPayload = record.value();
                        boolean isTombstone = (originalPayload == null);

                        byte[] transformedPayload = originalPayload;
                        if (!isTombstone) {
                            PayloadFormat detectedFormat = formatClassifier.classify(originalPayload, defaultTopicFormat);
                            transformedPayload = transcoder.transcode(record.topic(), originalPayload, detectedFormat, targetFormat);
                        }

                        // 3. Формируем Protobuf запись с сохранением партиции и обогащением заголовков
                        KafkaRecord.Builder recBuilder = KafkaRecord.newBuilder()
                                .setPartition(record.partition())
                                .setOriginalOffset(record.offset())
                                .setTimestamp(record.timestamp())
                                .setIsTombstone(isTombstone)
                                .addAllHeaders(loopFilter.enrichHeaders(record.headers()));

                        if (record.key() != null) {
                            recBuilder.setKey(ByteString.copyFrom(record.key()));
                        }

                        if (transformedPayload != null) {
                            recBuilder.setPayload(ByteString.copyFrom(transformedPayload));
                        }

                        recordsByTp.computeIfAbsent(tp, k -> new ArrayList<>())
                                .add(recBuilder.build());
                    }
                    currentLagMs.set(maxRecordLag);

                    // 4. Отправляем батчи по партициям в соответствующие gRPC-стримы
                    for (Map.Entry<TopicPartition, List<KafkaRecord>> entry : recordsByTp.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        List<KafkaRecord> partitionRecords = entry.getValue();

                        int targetIdx = replicationClient.selectTargetIndex(tp.partition());
                        StreamObserver<KafkaBatchRequest> targetStream = getOrCreateStream(targetIdx);

                        // Дробим на пачки до maxBatchSize
                        for (int i = 0; i < partitionRecords.size(); i += maxBatchSize) {
                            List<KafkaRecord> chunk = partitionRecords.subList(i, Math.min(i + maxBatchSize, partitionRecords.size()));
                            long maxChunkOffset = chunk.get(chunk.size() - 1).getOriginalOffset();

                            KafkaBatchRequest batchRequest = KafkaBatchRequest.newBuilder()
                                    .setBatchId(UUID.randomUUID().toString())
                                    .setSourceDc(sourceDcId)
                                    .setTargetDc(targetDcId)
                                    .setTopic(tp.topic())
                                    .setPayloadFormat(targetFormat)
                                    .setSendTimestampMs(System.currentTimeMillis())
                                    .addAllRecords(chunk)
                                    .build();

                            // Проверка готовности сетевого буфера gRPC (Client-side Backpressure Flow Control)
                            if (targetStream instanceof io.grpc.stub.ClientCallStreamObserver<?> clientCallStreamObserver) {
                                while (running.get() && !clientCallStreamObserver.isReady()) {
                                    drainAndCommitPendingOffsets();
                                    try {
                                        Thread.sleep(5);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        break;
                                    }
                                }
                            }

                            long batchBytes = batchRequest.getSerializedSize();
                            inFlightBatches.put(batchRequest.getBatchId(),
                                    new InFlightBatch(batchRequest.getBatchId(), tp, maxChunkOffset, chunk.size(), batchBytes));
                            inFlightCount.incrementAndGet();

                            if (bandwidthLimiter != null) {
                                bandwidthLimiter.throttle(batchBytes);
                            }

                            try {
                                targetStream.onNext(batchRequest);
                            } catch (Exception ex) {
                                inFlightBatches.remove(batchRequest.getBatchId());
                                inFlightCount.decrementAndGet();
                                log.warn("[Worker {} -> Target {}] Ошибка отправки батча, failover: {}",
                                        workerId, targetIdx, ex.getMessage());
                                replicationClient.markTargetSuspect(targetIdx, ex);
                                activeStreams.remove(targetIdx);
                                int fallbackIdx = replicationClient.selectTargetIndex(tp.partition());
                                inFlightBatches.put(batchRequest.getBatchId(),
                                        new InFlightBatch(batchRequest.getBatchId(), tp, maxChunkOffset, chunk.size(), batchBytes));
                                inFlightCount.incrementAndGet();
                                getOrCreateStream(fallbackIdx).onNext(batchRequest);
                            }
                        }
                    }

                    // Фиксируем подтвержденные смещения
                    drainAndCommitPendingOffsets();
                }
            } catch (WakeupException e) {
                log.debug("[Worker {}] Получен сигнал Wakeup, штатное завершение", workerId);
            } catch (Exception e) {
                log.error("[Worker {}] Фатальная ошибка в цикле вычитки Kafka: {}", workerId, e.getMessage(), e);
            } finally {
                drainAndCommitPendingOffsets();
                for (StreamObserver<KafkaBatchRequest> s : activeStreams.values()) {
                    try {
                        s.onCompleted();
                    } catch (Exception ignored) {
                    }
                }
                activeStreams.clear();
                log.info("Остановлен рабочий поток ConsumerWorker-{}", workerId);
            }
        }

        private void drainAndCommitPendingOffsets() {
            if (pendingCommittedOffsets.isEmpty()) {
                return;
            }
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> toCommit = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : pendingCommittedOffsets.entrySet()) {
                toCommit.put(entry.getKey(), new org.apache.kafka.clients.consumer.OffsetAndMetadata(entry.getValue()));
            }
            if (!toCommit.isEmpty()) {
                workerConsumer.commitAsync(toCommit, (offsets, exception) -> {
                    if (exception != null) {
                        log.warn("[Worker {}] Предупреждение фиксации подтвержденных оффсетов: {}", workerId, exception.getMessage());
                    } else {
                        log.trace("[Worker {}] Успешно зафиксированы подтвержденные оффсеты: {}", workerId, offsets);
                    }
                });
            }
        }
    }

    public long getTotalMessagesReplicated() {
        return totalMessagesReplicated.get();
    }

    public long getTotalBytesReplicated() {
        return totalBytesReplicated.get();
    }

    public long getCurrentLagMs() {
        return currentLagMs.get();
    }

    public Map<TopicPartition, Long> getLastObservedTimestamps() {
        return Collections.unmodifiableMap(lastObservedTimestamps);
    }

    public void updateSubscribedTopics(List<String> newTopics) {
        if (newTopics != null) {
            List<String> sorted = new ArrayList<>(newTopics);
            Collections.sort(sorted);
            List<String> current = subscribedTopics.get();
            if (current != null) {
                List<String> sortedCurrent = new ArrayList<>(current);
                Collections.sort(sortedCurrent);
                if (sortedCurrent.equals(sorted)) {
                    return;
                }
            }
            this.subscribedTopics.set(new ArrayList<>(newTopics));
            this.topicsVersion.incrementAndGet();
            for (KafkaConsumer<byte[], byte[]> c : consumers) {
                try {
                    c.wakeup();
                } catch (Exception ignored) {
                }
            }
        }
    }

    public List<String> getSubscribedTopics() {
        return Collections.unmodifiableList(subscribedTopics.get());
    }

    @Override
    public void close() {
        running.set(false);
        for (KafkaConsumer<byte[], byte[]> c : consumers) {
            try {
                c.wakeup();
            } catch (Exception ignored) {
            }
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        for (KafkaConsumer<byte[], byte[]> c : consumers) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }
}
