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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    /**
     * Хвост цепочки отправки по каждой партиции.
     *
     * <p>Приемник запрашивает окно батчей вперед, поэтому батчи одной партиции попадали в
     * пул одновременно и {@code producer.send()} для них шел параллельно из разных потоков.
     * Идемпотентный продюсер сохраняет порядок только для последовательности send из одного
     * потока, так что записи партиции могли лечь в целевой топик в обратном порядке — для
     * compacted-топиков и CDC это испорченное состояние. Отправка каждой партиции
     * выстраивается в цепочку, а разные партиции по-прежнему идут параллельно.
     */
    private final ConcurrentMap<String, CompletableFuture<Void>> partitionChains = new ConcurrentHashMap<>();

    /**
     * Наибольшее смещение источника в последнем принятом батче партиции.
     *
     * <p>Батч с {@code prev_max_source_offset} принимается, только если продолжает это значение.
     * Отказ батча откатывает его к предшественнику отказавшего: батчи, которые отправитель
     * успел послать следом, отклоняются, и он переотправляет их по порядку после повтора.
     */
    private final ConcurrentMap<String, Long> acceptedMaxOffsets = new ConcurrentHashMap<>();

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

        String partitionKey = topic + "#" + partitionOf(records);
        long batchMaxOffset = records.get(records.size() - 1).getOriginalOffset();
        boolean sequenced = batch.hasPrevMaxSourceOffset();
        long prevMaxOffset = batch.getPrevMaxSourceOffset();
        Runnable sendAll = () -> {
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
                            // Одна строка на батч: сбой брокера отклоняет все записи батча разом,
                            // и строка на каждую давала тысячи ERROR в секунду поверх нагрузки
                            if (firstError.compareAndSet(null, ex)) {
                                log.error("Ошибка при отправке записей батча {} в топик {}: {}",
                                        batch.getBatchId(), topic, ex.getMessage());
                            }
                        } else if (rm != null) {
                            committedCount.incrementAndGet();
                            maxOffset.accumulateAndGet(rm.offset(), Math::max);
                        }

                        if (remainingCount.decrementAndGet() == 0) {
                            finishBatch(resultFuture, batch, topic, receiveTime, committedCount.get(), maxOffset.get(),
                                    firstError.get(), partitionKey);
                        }
                    });
                } catch (Exception e) {
                    firstError.compareAndSet(null, e);
                    log.error("Синхронное исключение producer.send() для топика {}: {}", topic, e.getMessage());
                    if (remainingCount.decrementAndGet() == 0) {
                        finishBatch(resultFuture, batch, topic, receiveTime, committedCount.get(), maxOffset.get(),
                                firstError.get(), partitionKey);
                    }
                }
            }
        };

        // Следующий батч этой партиции начинает отправку только после того, как предыдущий
        // передал все свои записи продюсеру
        // Проверка последовательности и постановка в цепочку — одним атомарным шагом по партиции
        String[] outOfSequence = new String[1];
        partitionChains.compute(partitionKey, (key, previous) -> {
            Long accepted = acceptedMaxOffsets.get(partitionKey);
            if (sequenced && accepted != null && accepted != prevMaxOffset) {
                outOfSequence[0] = "батч вне последовательности партиции " + partitionKey
                        + ": продолжает смещение " + prevMaxOffset + ", последнее принятое " + accepted;
                return previous;
            }
            acceptedMaxOffsets.put(partitionKey, batchMaxOffset);
            CompletableFuture<Void> base = (previous == null || previous.isDone())
                    ? CompletableFuture.completedFuture(null)
                    : previous;
            return base.thenRunAsync(sendAll, executor);
        });
        if (outOfSequence[0] != null) {
            log.debug("Отклонен пакет {}: {}", batch.getBatchId(), outOfSequence[0]);
            return CompletableFuture.completedFuture(buildAck(batch, topic, receiveTime, 0, -1L,
                    new IllegalStateException(outOfSequence[0])));
        }

        return resultFuture;
    }

    /**
     * Завершение батча: при отказе последовательность партиции откатывается к его
     * предшественнику, и все, что отправитель послал следом, отклоняется до повтора отказавшего.
     * Откат только назад: отказ более позднего батча не перекрывает откат раннего.
     */
    private void finishBatch(CompletableFuture<KafkaBatchAck> resultFuture,
                             KafkaBatchRequest batch,
                             String topic,
                             long receiveTime,
                             int committed,
                             long maxOffset,
                             Throwable error,
                             String partitionKey) {
        if (error != null) {
            if (batch.hasPrevMaxSourceOffset()) {
                long rollbackTo = batch.getPrevMaxSourceOffset();
                acceptedMaxOffsets.merge(partitionKey, rollbackTo, Math::min);
            } else {
                acceptedMaxOffsets.remove(partitionKey);
            }
        }
        finishAck(resultFuture, batch, topic, receiveTime, committed, maxOffset, error);
    }

    /** Партиция батча: записи батча всегда относятся к одной партиции источника. */
    private static int partitionOf(List<KafkaRecord> records) {
        return records.isEmpty() ? -1 : records.get(0).getPartition();
    }

    /**
     * Завершение future батча.
     *
     * <p>Вызывается из колбэка {@code send()}, то есть в сетевом потоке продюсера (Sender).
     * Завершать future прямо здесь нельзя: зависимые стадии — ответ в gRPC-стрим, закрытие
     * стрима — выполнились бы в этом же потоке. Блокирующий вызов продюсера оттуда
     * ({@code flush()}) ждал бы сам себя, и запись в Kafka останавливалась навсегда; даже без
     * блокировки отправка ответов под локом в этом потоке тормозила всю запись. Поэтому
     * завершение уходит в пул продюсера.
     */
    private void finishAck(CompletableFuture<KafkaBatchAck> resultFuture,
                           KafkaBatchRequest batch,
                           String topic,
                           long receiveTime,
                           int committed,
                           long maxOffset,
                           Throwable error) {
        KafkaBatchAck ack = buildAck(batch, topic, receiveTime, committed, maxOffset, error);
        try {
            executor.execute(() -> resultFuture.complete(ack));
        } catch (java.util.concurrent.RejectedExecutionException shuttingDown) {
            // Пул уже остановлен: подтвердить батч важнее, чем уйти из потока продюсера
            resultFuture.complete(ack);
        }
    }

    private static KafkaBatchAck buildAck(KafkaBatchRequest batch,
                                          String topic,
                                          long receiveTime,
                                          int committed,
                                          long maxOffset,
                                          Throwable error) {
        KafkaBatchAck.Builder ack = KafkaBatchAck.newBuilder()
                .setBatchId(batch.getBatchId())
                .setTopic(topic)
                .setSuccess(error == null)
                .setCommittedRecords(committed)
                .setMaxTargetOffset(maxOffset)
                .setReceiveTimestampMs(receiveTime);
        if (error != null) {
            ack.setErrorMessage(error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName());
        }
        return ack.build();
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
