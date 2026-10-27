package org.apache.hadoop.explorer.replicator.kafka.producer;

import com.google.protobuf.ByteString;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecord;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecordHeader;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("Unit Test: KafkaBatchProducer с асинхронным конвейером")
class KafkaBatchProducerTest {

    private KafkaProducer<byte[], byte[]> mockProducer;
    private KafkaBatchProducer batchProducer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockProducer = mock(KafkaProducer.class);
        batchProducer = new KafkaBatchProducer(mockProducer);
    }

    @Test
    @DisplayName("writeBatchAsync: пустой батч возвращает мгновенный успех с 0 записями")
    void shouldHandleEmptyBatch() throws Exception {
        KafkaBatchRequest batch = KafkaBatchRequest.newBuilder()
                .setBatchId("empty-batch")
                .setTopic("test-topic")
                .build();

        CompletableFuture<KafkaBatchAck> future = batchProducer.writeBatchAsync(batch);
        assertTrue(future.isDone());

        KafkaBatchAck ack = future.get();
        assertTrue(ack.getSuccess());
        assertEquals("empty-batch", ack.getBatchId());
        assertEquals("test-topic", ack.getTopic());
        assertEquals(0, ack.getCommittedRecords());
    }

    @Test
    @DisplayName("writeBatchAsync: успешная фиксация пачки из нескольких сообщений")
    void shouldProduceRecordsAsyncSuccessfully() throws Exception {
        when(mockProducer.send(any(ProducerRecord.class), any(Callback.class)))
                .thenAnswer(invocation -> {
                    Callback cb = invocation.getArgument(1);
                    RecordMetadata rm = new RecordMetadata(new TopicPartition("test-topic", 0), 105L, 0, System.currentTimeMillis(), 0, 0);
                    cb.onCompletion(rm, null);
                    return CompletableFuture.completedFuture(rm);
                });

        KafkaBatchRequest batch = KafkaBatchRequest.newBuilder()
                .setBatchId("batch-123")
                .setTopic("test-topic")
                .addRecords(KafkaRecord.newBuilder()
                        .setPartition(0)
                        .setKey(ByteString.copyFromUtf8("k1"))
                        .setPayload(ByteString.copyFromUtf8("v1"))
                        .addHeaders(KafkaRecordHeader.newBuilder().setKey("traceId").setValue(ByteString.copyFromUtf8("t1")).build())
                        .build())
                .addRecords(KafkaRecord.newBuilder()
                        .setPartition(1)
                        .setKey(ByteString.copyFromUtf8("k2"))
                        .setPayload(ByteString.copyFromUtf8("v2"))
                        .build())
                .build();

        CompletableFuture<KafkaBatchAck> future = batchProducer.writeBatchAsync(batch);
        KafkaBatchAck ack = future.get(5, TimeUnit.SECONDS);

        assertTrue(ack.getSuccess());
        assertEquals(2, ack.getCommittedRecords());
        assertEquals(105L, ack.getMaxTargetOffset());
        assertEquals("batch-123", ack.getBatchId());

        verify(mockProducer, times(2)).send(any(ProducerRecord.class), any(Callback.class));
    }

    @Test
    @DisplayName("writeBatchAsync: возврат ошибки при сбое на стороне брокера")
    void shouldHandleBrokerException() throws Exception {
        when(mockProducer.send(any(ProducerRecord.class), any(Callback.class)))
                .thenAnswer(invocation -> {
                    Callback cb = invocation.getArgument(1);
                    cb.onCompletion(null, new RuntimeException("Kafka broker leader not available"));
                    return CompletableFuture.failedFuture(new RuntimeException("Kafka broker leader not available"));
                });

        KafkaBatchRequest batch = KafkaBatchRequest.newBuilder()
                .setBatchId("failed-batch")
                .setTopic("test-topic")
                .addRecords(KafkaRecord.newBuilder().setPayload(ByteString.copyFromUtf8("val")).build())
                .build();

        CompletableFuture<KafkaBatchAck> future = batchProducer.writeBatchAsync(batch);
        KafkaBatchAck ack = future.get(5, TimeUnit.SECONDS);

        assertFalse(ack.getSuccess());
        assertTrue(ack.getErrorMessage().contains("Kafka broker leader not available"));
    }

    @Test
    @DisplayName("writeBatch: синхронная обертка корректно ожидает выполнение")
    void shouldSupportSyncWrapper() {
        when(mockProducer.send(any(ProducerRecord.class), any(Callback.class)))
                .thenAnswer(invocation -> {
                    Callback cb = invocation.getArgument(1);
                    RecordMetadata rm = new RecordMetadata(new TopicPartition("test-topic", 0), 42L, 0, System.currentTimeMillis(), 0, 0);
                    cb.onCompletion(rm, null);
                    return CompletableFuture.completedFuture(rm);
                });

        KafkaBatchRequest batch = KafkaBatchRequest.newBuilder()
                .setBatchId("sync-batch")
                .setTopic("test-topic")
                .addRecords(KafkaRecord.newBuilder().setPayload(ByteString.copyFromUtf8("hello")).build())
                .build();

        KafkaBatchAck ack = batchProducer.writeBatch(batch);
        assertTrue(ack.getSuccess());
        assertEquals(1, ack.getCommittedRecords());
        assertEquals(42L, ack.getMaxTargetOffset());
    }

    @Test
    @DisplayName("Подтверждение батча не выполняется в сетевом потоке продюсера")
    void ackContinuationDoesNotRunOnProducerIoThread() throws Exception {
        // Колбэк send() Kafka вызывает в своем сетевом потоке Sender. Если future батча
        // завершается прямо там, зависимые стадии (ответ в gRPC, flush при закрытии стрима)
        // тоже выполняются в нем, и KafkaProducer.flush() ждет сам себя — вечная блокировка.
        java.util.concurrent.atomic.AtomicReference<Callback> captured = new java.util.concurrent.atomic.AtomicReference<>();
        when(mockProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(inv -> {
            captured.set(inv.getArgument(1));
            return CompletableFuture.completedFuture(null);
        });

        KafkaBatchRequest batch = KafkaBatchRequest.newBuilder()
                .setBatchId("io-thread-batch")
                .setTopic("orders")
                .addRecords(org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecord.newBuilder()
                        .setPartition(0).setOriginalOffset(1).build())
                .build();
        CompletableFuture<KafkaBatchAck> future = batchProducer.writeBatchAsync(batch);
        java.util.concurrent.atomic.AtomicReference<String> continuationThread = new java.util.concurrent.atomic.AtomicReference<>();
        CompletableFuture<Void> continuation = future.thenAccept(ack ->
                continuationThread.set(Thread.currentThread().getName()));

        long deadline = System.currentTimeMillis() + 5000;
        while (captured.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertNotNull(captured.get(), "send() должен быть вызван");

        Thread sender = new Thread(() -> captured.get().onCompletion(
                new org.apache.kafka.clients.producer.RecordMetadata(
                        new org.apache.kafka.common.TopicPartition("orders", 0), 0, 0, 0L, 0, 0), null),
                "kafka-producer-network-thread | test");
        sender.start();
        sender.join(5000);
        continuation.get(5, java.util.concurrent.TimeUnit.SECONDS);

        assertNotEquals("kafka-producer-network-thread | test", continuationThread.get(),
                "Стадии после подтверждения батча обязаны выполняться вне сетевого потока продюсера");
    }

    private static KafkaBatchRequest seqBatch(String id, long firstOffset, long lastOffset, Long prevMax) {
        KafkaBatchRequest.Builder b = KafkaBatchRequest.newBuilder().setBatchId(id).setTopic("orders");
        for (long o = firstOffset; o <= lastOffset; o++) {
            b.addRecords(KafkaRecord.newBuilder().setPartition(3).setOriginalOffset(o)
                    .setPayload(ByteString.copyFromUtf8("v" + o)).build());
        }
        if (prevMax != null) {
            b.setPrevMaxSourceOffset(prevMax);
        }
        return b.build();
    }

    private KafkaBatchAck write(KafkaBatchRequest batch) throws Exception {
        return batchProducer.writeBatchAsync(batch).get(5, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("Последовательность партиции: батч, обогнавший отказавший, отклоняется до его повтора")
    void shouldRejectBatchesThatOvertakeFailedOne() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean failNext = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.List<Long> written = new java.util.concurrent.CopyOnWriteArrayList<>();
        when(mockProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(inv -> {
            ProducerRecord<byte[], byte[]> rec = inv.getArgument(0);
            Callback cb = inv.getArgument(1);
            if (failNext.get()) {
                cb.onCompletion(null, new RuntimeException("NOT_ENOUGH_REPLICAS"));
            } else {
                written.add(Long.parseLong(new String(rec.value()).substring(1)));
                cb.onCompletion(new RecordMetadata(new TopicPartition("orders", 3), 0L, 0, 0L, 0, 0), null);
            }
            return CompletableFuture.completedFuture(null);
        });

        assertTrue(write(seqBatch("b1", 0, 9, null)).getSuccess(), "Первый батч начинает последовательность");
        failNext.set(true);
        assertFalse(write(seqBatch("b2", 10, 19, 9L)).getSuccess());
        failNext.set(false);

        KafkaBatchAck overtaking = write(seqBatch("b3", 20, 29, 19L));
        assertFalse(overtaking.getSuccess(), "Батч за отказавшим не должен лечь раньше его повтора");
        assertTrue(overtaking.getErrorMessage().contains("вне последовательности"));

        assertTrue(write(seqBatch("b2-retry", 10, 19, 9L)).getSuccess(), "Повтор отказавшего продолжает последовательность");
        assertTrue(write(seqBatch("b3-retry", 20, 29, 19L)).getSuccess());

        java.util.List<Long> expected = new java.util.ArrayList<>();
        for (long o = 0; o <= 29; o++) {
            expected.add(o);
        }
        assertEquals(expected, written, "В целевой топик записи легли в порядке источника");
    }

    @Test
    @DisplayName("Батч без предшественника начинает последовательность заново: новый владелец партиции, DLQ")
    void shouldRestartSequenceWithoutPredecessor() throws Exception {
        when(mockProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(inv -> {
            ((Callback) inv.getArgument(1)).onCompletion(
                    new RecordMetadata(new TopicPartition("orders", 3), 0L, 0, 0L, 0, 0), null);
            return CompletableFuture.completedFuture(null);
        });
        assertTrue(write(seqBatch("b1", 0, 9, null)).getSuccess());
        assertFalse(write(seqBatch("stale", 30, 39, 29L)).getSuccess());
        assertTrue(write(seqBatch("restart", 40, 49, null)).getSuccess());
        assertTrue(write(seqBatch("next", 50, 59, 49L)).getSuccess());
        assertTrue(write(seqBatch("legacy", 60, 69, null)).getSuccess(),
                "Батч старого отправителя без поля принимается как раньше");
    }
}
