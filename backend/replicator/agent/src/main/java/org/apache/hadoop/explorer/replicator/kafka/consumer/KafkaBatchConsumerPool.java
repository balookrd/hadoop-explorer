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

    private int maxBatchSize = 2000;
    private ExecutorService executor;

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
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
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

        ConsumerWorker(int workerId, KafkaConsumer<byte[], byte[]> workerConsumer) {
            this.workerId = workerId;
            this.workerConsumer = workerConsumer;
        }

        private StreamObserver<KafkaBatchRequest> getOrCreateStream(int targetIdx) {
            return activeStreams.computeIfAbsent(targetIdx, idx -> {
                StreamObserver<KafkaBatchAck> ackObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(KafkaBatchAck ack) {
                        if (ack.getSuccess()) {
                            log.trace("[Worker {} -> Target {}] Успешно зафиксирован пакет {} в топике {}: {} записей",
                                    workerId, idx, ack.getBatchId(), ack.getTopic(), ack.getCommittedRecords());
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
                    ConsumerRecords<byte[], byte[]> records = workerConsumer.poll(Duration.ofMillis(100));
                    if (records.isEmpty()) {
                        continue;
                    }

                    // Группировка строго по TopicPartition для сохранения порядка при балансировке по агентам
                    Map<TopicPartition, List<KafkaRecord>> recordsByTp = new LinkedHashMap<>();

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        lastObservedTimestamps.put(tp, record.timestamp());

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

                    // 4. Отправляем батчи по партициям в соответствующие gRPC-стримы
                    for (Map.Entry<TopicPartition, List<KafkaRecord>> entry : recordsByTp.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        List<KafkaRecord> partitionRecords = entry.getValue();

                        int targetIdx = replicationClient.selectTargetIndex(tp.partition());
                        StreamObserver<KafkaBatchRequest> targetStream = getOrCreateStream(targetIdx);

                        // Дробим на пачки до maxBatchSize
                        for (int i = 0; i < partitionRecords.size(); i += maxBatchSize) {
                            List<KafkaRecord> chunk = partitionRecords.subList(i, Math.min(i + maxBatchSize, partitionRecords.size()));

                            KafkaBatchRequest batchRequest = KafkaBatchRequest.newBuilder()
                                    .setBatchId(UUID.randomUUID().toString())
                                    .setSourceDc(sourceDcId)
                                    .setTargetDc(targetDcId)
                                    .setTopic(tp.topic())
                                    .setPayloadFormat(targetFormat)
                                    .setSendTimestampMs(System.currentTimeMillis())
                                    .addAllRecords(chunk)
                                    .build();

                            try {
                                targetStream.onNext(batchRequest);
                            } catch (Exception ex) {
                                log.warn("[Worker {} -> Target {}] Ошибка отправки батча, failover: {}",
                                        workerId, targetIdx, ex.getMessage());
                                replicationClient.markTargetSuspect(targetIdx, ex);
                                activeStreams.remove(targetIdx);
                                int fallbackIdx = replicationClient.selectTargetIndex(tp.partition());
                                getOrCreateStream(fallbackIdx).onNext(batchRequest);
                            }
                        }
                    }

                    // 5. Коммитим смещения локально
                    workerConsumer.commitAsync();
                }
            } catch (WakeupException e) {
                log.debug("[Worker {}] Получен сигнал Wakeup, штатное завершение", workerId);
            } catch (Exception e) {
                log.error("[Worker {}] Фатальная ошибка в цикле вычитки Kafka: {}", workerId, e.getMessage(), e);
            } finally {
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
    }

    public Map<TopicPartition, Long> getLastObservedTimestamps() {
        return Collections.unmodifiableMap(lastObservedTimestamps);
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
