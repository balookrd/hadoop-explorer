package org.apache.hadoop.explorer.replicator.kafka.producer;

import com.google.protobuf.ByteString;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecord;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecordHeader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Высокопроизводительный продюсер для асинхронной и неблокирующей фиксации батчей
 * реплицированных записей в локальной Kafka с минимальной задержкой.
 */
public class KafkaBatchProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchProducer.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private final ExecutorService executor;

    public KafkaBatchProducer(KafkaProducer<byte[], byte[]> producer) {
        this(producer, Executors.newVirtualThreadPerTaskExecutor());
    }

    public KafkaBatchProducer(KafkaProducer<byte[], byte[]> producer, ExecutorService executor) {
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
    }

    /**
     * Асинхронно записывает батч KafkaRecord в локальный кластер с сохранением номеров партиций,
     * ключей, временных меток и Tombstone-маркеров без блокировки вызывающего потока.
     *
     * @param batch входящий пакет записей из gRPC
     * @return CompletableFuture с подтверждением фиксации пакета (Ack)
     */
    public CompletableFuture<KafkaBatchAck> writeBatchAsync(KafkaBatchRequest batch) {
        String topic = batch.getTopic();
        List<KafkaRecord> records = batch.getRecordsList();
        long receiveTime = System.currentTimeMillis();

        if (records.isEmpty()) {
            return CompletableFuture.completedFuture(
                    KafkaBatchAck.newBuilder()
                            .setBatchId(batch.getBatchId())
                            .setTopic(topic)
                            .setSuccess(true)
                            .setCommittedRecords(0)
                            .setReceiveTimestampMs(receiveTime)
                            .build()
            );
        }

        CompletableFuture<KafkaBatchAck> resultFuture = new CompletableFuture<>();
        int totalRecords = records.size();
        AtomicInteger remainingCount = new AtomicInteger(totalRecords);
        AtomicInteger committedCount = new AtomicInteger(0);
        AtomicLong maxOffset = new AtomicLong(-1);
        AtomicReference<Throwable> firstError = new AtomicReference<>(null);

        executor.submit(() -> {
            for (KafkaRecord r : records) {
                byte[] key = r.getKey().isEmpty() ? null : r.getKey().toByteArray();
                byte[] val = r.getIsTombstone() || !r.hasPayload() ? null : r.getPayload().toByteArray();
                Integer partition = r.getPartition() >= 0 ? r.getPartition() : null;
                Long timestamp = r.getTimestamp() > 0 ? r.getTimestamp() : null;

                ProducerRecord<byte[], byte[]> prodRecord = new ProducerRecord<>(
                        topic,
                        partition,
                        timestamp,
                        key,
                        val
                );

                // Добавляем заголовки
                for (KafkaRecordHeader h : r.getHeadersList()) {
                    prodRecord.headers().add(new RecordHeader(
                            h.getKey(),
                            h.getValue().toByteArray()
                    ));
                }

                try {
                    producer.send(prodRecord, (rm, ex) -> {
                        if (ex != null) {
                            firstError.compareAndSet(null, ex);
                            log.error("Ошибка при отправке записи в топик {}: {}", topic, ex.getMessage());
                        } else if (rm != null) {
                            committedCount.incrementAndGet();
                            maxOffset.accumulateAndGet(rm.offset(), Math::max);
                        }

                        if (remainingCount.decrementAndGet() == 0) {
                            finishAck(resultFuture, batch, topic, receiveTime, committedCount.get(), maxOffset.get(), firstError.get());
                        }
                    });
                } catch (Exception e) {
                    firstError.compareAndSet(null, e);
                    log.error("Синхронное исключение producer.send() для топика {}: {}", topic, e.getMessage());
                    if (remainingCount.decrementAndGet() == 0) {
                        finishAck(resultFuture, batch, topic, receiveTime, committedCount.get(), maxOffset.get(), firstError.get());
                    }
                }
            }
        });

        return resultFuture;
    }

    private void finishAck(CompletableFuture<KafkaBatchAck> resultFuture,
                           KafkaBatchRequest batch,
                           String topic,
                           long receiveTime,
                           int committed,
                           long maxOffset,
                           Throwable error) {
        if (error != null) {
            resultFuture.complete(KafkaBatchAck.newBuilder()
                    .setBatchId(batch.getBatchId())
                    .setTopic(topic)
                    .setSuccess(false)
                    .setCommittedRecords(committed)
                    .setMaxTargetOffset(maxOffset)
                    .setErrorMessage(error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName())
                    .setReceiveTimestampMs(receiveTime)
                    .build());
        } else {
            resultFuture.complete(KafkaBatchAck.newBuilder()
                    .setBatchId(batch.getBatchId())
                    .setTopic(topic)
                    .setSuccess(true)
                    .setCommittedRecords(committed)
                    .setMaxTargetOffset(maxOffset)
                    .setReceiveTimestampMs(receiveTime)
                    .build());
        }
    }

    /**
     * Синхронная фиксация батча (обертка для обратной совместимости).
     *
     * @param batch входящий пакет записей из gRPC
     * @return подтверждение фиксации пакета (Ack)
     */
    public KafkaBatchAck writeBatch(KafkaBatchRequest batch) {
        try {
            return writeBatchAsync(batch).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Таймаут или сбой ожидания фиксации пакета {} в топике {}: {}", batch.getBatchId(), batch.getTopic(), e.getMessage());
            return KafkaBatchAck.newBuilder()
                    .setBatchId(batch.getBatchId())
                    .setTopic(batch.getTopic())
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .setReceiveTimestampMs(System.currentTimeMillis())
                    .build();
        }
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        try {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
