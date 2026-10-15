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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Пул консьюмеров для непрерывного чтения сообщений из локальной Kafka,
 * фильтрации циклов (Loop Prevention), транскодинга схем и стриминга в удаленный ДЦ.
 */
public class KafkaBatchConsumerPool implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchConsumerPool.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
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

    private int maxBatchSize = 500;

    public KafkaBatchConsumerPool(KafkaConsumer<byte[], byte[]> consumer,
                                  KafkaReplicationClient replicationClient,
                                  LoopPreventionFilter loopFilter,
                                  PayloadFormatClassifier formatClassifier,
                                  ZeroCopyWireTranscoder transcoder,
                                  String sourceDcId,
                                  String targetDcId,
                                  PayloadFormat defaultTopicFormat,
                                  PayloadFormat targetFormat) {
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
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

    @Override
    public void run() {
        running.set(true);
        log.info("Запущен поток KafkaBatchConsumerPool ({} -> {})", sourceDcId, targetDcId);

        // Открываем двунаправленный gRPC стрим
        StreamObserver<KafkaBatchRequest> requestStream = replicationClient.openBatchStream(new StreamObserver<>() {
            @Override
            public void onNext(KafkaBatchAck ack) {
                if (ack.getSuccess()) {
                    log.trace("Успешно зафиксирован пакет {} в топике {}: {} записей",
                            ack.getBatchId(), ack.getTopic(), ack.getCommittedRecords());
                } else {
                    log.error("Удаленный узел вернул ошибку для пакета {}: {}", ack.getBatchId(), ack.getErrorMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Сбой gRPC стрима в KafkaBatchConsumerPool: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                log.info("gRPC стрим закрыт удаленным узлом");
            }
        });

        try {
            while (running.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    continue;
                }

                Map<String, List<KafkaRecord>> recordsByTopic = new HashMap<>();

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

                    // 3. Формируем Protobuf запись с обогащением заголовков
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

                    recordsByTopic.computeIfAbsent(record.topic(), k -> new ArrayList<>())
                            .add(recBuilder.build());
                }

                // 4. Отправляем батчи по топикам в gRPC-стрим
                for (Map.Entry<String, List<KafkaRecord>> entry : recordsByTopic.entrySet()) {
                    String topic = entry.getKey();
                    List<KafkaRecord> batchRecords = entry.getValue();

                    // Дробим на пачки до maxBatchSize
                    for (int i = 0; i < batchRecords.size(); i += maxBatchSize) {
                        List<KafkaRecord> chunk = batchRecords.subList(i, Math.min(i + maxBatchSize, batchRecords.size()));

                        KafkaBatchRequest batchRequest = KafkaBatchRequest.newBuilder()
                                .setBatchId(UUID.randomUUID().toString())
                                .setSourceDc(sourceDcId)
                                .setTargetDc(targetDcId)
                                .setTopic(topic)
                                .setPayloadFormat(targetFormat)
                                .setSendTimestampMs(System.currentTimeMillis())
                                .addAllRecords(chunk)
                                .build();

                        requestStream.onNext(batchRequest);
                    }
                }

                // 5. Коммитим смещения локально
                consumer.commitAsync();
            }
        } catch (Exception e) {
            log.error("Фатальная ошибка в цикле репликации Kafka: {}", e.getMessage(), e);
        } finally {
            try {
                requestStream.onCompleted();
            } catch (Exception ignored) {
            }
            log.info("Остановлен поток KafkaBatchConsumerPool");
        }
    }

    public Map<TopicPartition, Long> getLastObservedTimestamps() {
        return Collections.unmodifiableMap(lastObservedTimestamps);
    }

    @Override
    public void close() {
        running.set(false);
    }
}
