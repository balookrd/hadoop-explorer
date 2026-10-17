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
}
