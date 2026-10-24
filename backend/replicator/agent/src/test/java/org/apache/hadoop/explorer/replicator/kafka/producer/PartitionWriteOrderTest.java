package org.apache.hadoop.explorer.replicator.kafka.producer;

import com.google.protobuf.ByteString;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Порядок записи батчей одной партиции на приемнике.
 *
 * <p>Приемник запрашивает окно батчей вперед и отдает каждый батч в
 * {@code newVirtualThreadPerTaskExecutor()}, поэтому {@code producer.send()} для батчей одной
 * партиции шли параллельно из разных потоков. Идемпотентный продюсер порядок при этом не
 * спасает: он гарантирует его только для последовательности send из одного потока. Для
 * compacted-топиков и CDC-потоков перестановка записей — это испорченное состояние.
 */
@DisplayName("Kafka: порядок записи батчей одной партиции")
class PartitionWriteOrderTest {

    private static KafkaRecord record(int partition, long offset) {
        return KafkaRecord.newBuilder()
                .setPartition(partition)
                .setOriginalOffset(offset)
                .setKey(ByteString.copyFrom(("k" + offset).getBytes(StandardCharsets.UTF_8)))
                .setPayload(ByteString.copyFrom(("v" + offset).getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    private static KafkaBatchRequest batch(String topic, int partition, long... offsets) {
        KafkaBatchRequest.Builder builder = KafkaBatchRequest.newBuilder()
                .setBatchId(UUID.randomUUID().toString())
                .setTopic(topic);
        for (long offset : offsets) {
            builder.addRecords(record(partition, offset));
        }
        return builder.build();
    }

    @Test
    @DisplayName("Батчи одной партиции пишутся в порядке поступления, даже если первый медленный")
    @SuppressWarnings("unchecked")
    void testBatchesOfSamePartitionKeepArrivalOrder() throws Exception {
        List<String> writeOrder = new CopyOnWriteArrayList<>();
        CountDownLatch firstSendEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);

        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(inv -> {
            ProducerRecord<byte[], byte[]> rec = inv.getArgument(0);
            String value = new String(rec.value(), StandardCharsets.UTF_8);
            if (value.equals("v100")) {
                // Первая запись первого батча задерживается: имитация медленного брокера
                firstSendEntered.countDown();
                releaseFirst.await(10, TimeUnit.SECONDS);
            }
            writeOrder.add(value);
            Callback callback = inv.getArgument(1);
            callback.onCompletion(new RecordMetadata(new TopicPartition(rec.topic(), 0), 0L, 0, 0L, 0, 0), null);
            return CompletableFuture.completedFuture(null);
        });

        KafkaBatchProducer batchProducer = new KafkaBatchProducer(producer);
        try {
            CompletableFuture<KafkaBatchAck> first = batchProducer.writeBatchAsync(batch("orders", 0, 100L, 101L));
            assertTrue(firstSendEntered.await(5, TimeUnit.SECONDS), "Первый батч должен начать запись");

            // Второй батч той же партиции приходит, пока первый еще пишется
            CompletableFuture<KafkaBatchAck> second = batchProducer.writeBatchAsync(batch("orders", 0, 200L, 201L));
            Thread.sleep(300);

            assertTrue(writeOrder.isEmpty() || writeOrder.stream().noneMatch(v -> v.startsWith("v2")),
                    "Записи второго батча ушли в брокер раньше первого: порядок в партиции нарушен, "
                            + "фактический порядок " + writeOrder);

            releaseFirst.countDown();
            assertTrue(first.get(10, TimeUnit.SECONDS).getSuccess());
            assertTrue(second.get(10, TimeUnit.SECONDS).getSuccess());

            assertEquals(List.of("v100", "v101", "v200", "v201"), writeOrder,
                    "Порядок записи в партиции обязан совпадать с порядком поступления батчей");
        } finally {
            releaseFirst.countDown();
            batchProducer.close();
        }
    }

    @Test
    @DisplayName("Батчи разных партиций пишутся параллельно")
    @SuppressWarnings("unchecked")
    void testBatchesOfDifferentPartitionsRunInParallel() throws Exception {
        CountDownLatch bothInFlight = new CountDownLatch(2);

        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(inv -> {
            ProducerRecord<byte[], byte[]> rec = inv.getArgument(0);
            bothInFlight.countDown();
            // Ждем, пока вторая партиция тоже окажется в полете: при сериализации по всем
            // партициям этот тест не прошел бы
            bothInFlight.await(5, TimeUnit.SECONDS);
            Callback callback = inv.getArgument(1);
            callback.onCompletion(new RecordMetadata(new TopicPartition(rec.topic(), 0), 0L, 0, 0L, 0, 0), null);
            return CompletableFuture.completedFuture(null);
        });

        KafkaBatchProducer batchProducer = new KafkaBatchProducer(producer);
        try {
            CompletableFuture<KafkaBatchAck> p0 = batchProducer.writeBatchAsync(batch("orders", 0, 10L));
            CompletableFuture<KafkaBatchAck> p1 = batchProducer.writeBatchAsync(batch("orders", 1, 20L));

            assertTrue(bothInFlight.await(5, TimeUnit.SECONDS),
                    "Разные партиции должны писаться независимо, иначе теряется параллелизм");
            assertTrue(p0.get(10, TimeUnit.SECONDS).getSuccess());
            assertTrue(p1.get(10, TimeUnit.SECONDS).getSuccess());
        } finally {
            batchProducer.close();
        }
    }
}
