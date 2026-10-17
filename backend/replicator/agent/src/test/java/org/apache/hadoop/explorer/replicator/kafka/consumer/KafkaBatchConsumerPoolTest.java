package org.apache.hadoop.explorer.replicator.kafka.consumer;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier;
import org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("Unit Test: KafkaBatchConsumerPool с многопоточным пулом воркеров")
class KafkaBatchConsumerPoolTest {

    private KafkaReplicationClient mockClient;
    private LoopPreventionFilter loopFilter;
    private PayloadFormatClassifier formatClassifier;
    private ZeroCopyWireTranscoder transcoder;
    private StreamObserver<KafkaBatchRequest> mockRequestStream;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockClient = mock(KafkaReplicationClient.class);
        mockRequestStream = mock(StreamObserver.class);
        when(mockClient.openBatchStream(any())).thenReturn(mockRequestStream);

        loopFilter = new LoopPreventionFilter("DC2");
        formatClassifier = mock(PayloadFormatClassifier.class);
        when(formatClassifier.classify(any(), any())).thenReturn(PayloadFormat.PAYLOAD_PASSTHROUGH);

        transcoder = mock(ZeroCopyWireTranscoder.class);
        when(transcoder.transcode(any(), any(), any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
    }

    @Test
    @DisplayName("Инициализация пула: конструктор одиночного консьюмера и списочный конструктор")
    @SuppressWarnings("unchecked")
    void shouldInitializeSingleAndMultiConsumerPool() {
        KafkaConsumer<byte[], byte[]> c1 = mock(KafkaConsumer.class);
        KafkaConsumer<byte[], byte[]> c2 = mock(KafkaConsumer.class);

        KafkaBatchConsumerPool singlePool = new KafkaBatchConsumerPool(
                c1, mockClient, loopFilter, formatClassifier, transcoder,
                "DC1", "DC2", PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH
        );
        assertEquals(1, singlePool.getPoolSize());
        assertEquals(2000, singlePool.getMaxBatchSize());

        KafkaBatchConsumerPool multiPool = new KafkaBatchConsumerPool(
                List.of(c1, c2), mockClient, loopFilter, formatClassifier, transcoder,
                "DC1", "DC2", PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH
        );
        assertEquals(2, multiPool.getPoolSize());

        multiPool.setMaxBatchSize(5000);
        assertEquals(5000, multiPool.getMaxBatchSize());
    }

    @Test
    @DisplayName("Многопоточный запуск: воркеры вычитывают записи и передают батчи в gRPC-стримы")
    @SuppressWarnings("unchecked")
    void shouldStartMultiThreadedWorkersAndProcessRecords() throws Exception {
        KafkaConsumer<byte[], byte[]> c1 = mock(KafkaConsumer.class);
        KafkaConsumer<byte[], byte[]> c2 = mock(KafkaConsumer.class);

        TopicPartition tp0 = new TopicPartition("orders", 0);
        ConsumerRecord<byte[], byte[]> rec1 = new ConsumerRecord<>("orders", 0, 10L, 1000L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, "k1".getBytes(), "v1".getBytes(), new RecordHeaders(), Optional.empty());

        TopicPartition tp1 = new TopicPartition("orders", 1);
        ConsumerRecord<byte[], byte[]> rec2 = new ConsumerRecord<>("orders", 1, 20L, 2000L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, "k2".getBytes(), "v2".getBytes(), new RecordHeaders(), Optional.empty());

        ConsumerRecords<byte[], byte[]> records1 = new ConsumerRecords<>(Map.of(tp0, List.of(rec1)));
        ConsumerRecords<byte[], byte[]> records2 = new ConsumerRecords<>(Map.of(tp1, List.of(rec2)));

        CountDownLatch pollLatch = new CountDownLatch(2);

        when(c1.poll(any(Duration.class))).thenAnswer(inv -> {
            pollLatch.countDown();
            return records1;
        });

        when(c2.poll(any(Duration.class))).thenAnswer(inv -> {
            pollLatch.countDown();
            return records2;
        });

        KafkaBatchConsumerPool pool = new KafkaBatchConsumerPool(
                List.of(c1, c2), mockClient, loopFilter, formatClassifier, transcoder,
                "DC1", "DC2", PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH
        );

        pool.start();

        assertTrue(pollLatch.await(3, TimeUnit.SECONDS), "Оба воркера должны были выполнить poll");

        // Даем короткое время на формирование и отправку батча в мок
        Thread.sleep(100);

        verify(mockRequestStream, atLeast(2)).onNext(any(KafkaBatchRequest.class));

        // Проверяем фиксацию таймстампов
        Map<TopicPartition, Long> timestamps = pool.getLastObservedTimestamps();
        assertTrue(timestamps.containsKey(tp0));
        assertTrue(timestamps.containsKey(tp1));

        pool.close();

        verify(c1, atLeastOnce()).wakeup();
        verify(c2, atLeastOnce()).wakeup();
        verify(c1, atLeastOnce()).close();
        verify(c2, atLeastOnce()).close();
    }
}
