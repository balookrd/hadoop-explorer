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
import org.mockito.ArgumentCaptor;

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

    @Test
    @DisplayName("Гарантия надежности At-Least-Once: смещения коммитятся только после успешного KafkaBatchAck")
    @SuppressWarnings("unchecked")
    void shouldCommitOffsetsOnlyAfterSuccessfulAck() throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp = new TopicPartition("orders", 0);
        ConsumerRecord<byte[], byte[]> rec = new ConsumerRecord<>("orders", 0, 42L, 1000L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, "k".getBytes(), "v".getBytes(), new RecordHeaders(), Optional.empty());
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(tp, List.of(rec)));

        CountDownLatch pollLatch = new CountDownLatch(1);
        when(consumer.poll(any(Duration.class))).thenAnswer(inv -> {
            pollLatch.countDown();
            return records;
        });

        List<StreamObserver<KafkaBatchAck>> ackObservers = new ArrayList<>();
        List<KafkaBatchRequest> sentRequests = new ArrayList<>();

        when(mockClient.openBatchStream(any())).thenAnswer(inv -> {
            StreamObserver<KafkaBatchAck> ackObs = inv.getArgument(0);
            ackObservers.add(ackObs);
            return new StreamObserver<KafkaBatchRequest>() {
                @Override
                public void onNext(KafkaBatchRequest req) {
                    sentRequests.add(req);
                }
                @Override public void onError(Throwable t) {}
                @Override public void onCompleted() {}
            };
        });

        KafkaBatchConsumerPool pool = new KafkaBatchConsumerPool(
                consumer, mockClient, loopFilter, formatClassifier, transcoder,
                "DC1", "DC2", PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH
        );

        pool.start();
        assertTrue(pollLatch.await(3, TimeUnit.SECONDS));

        // Ждем формирования и отправки батча
        long deadline = System.currentTimeMillis() + 2000;
        while (sentRequests.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertFalse(sentRequests.isEmpty(), "Батч должен быть отправлен");
        KafkaBatchRequest sentBatch = sentRequests.get(0);
        assertEquals("orders", sentBatch.getTopic());

        // До подтверждения Ack смещения не должны коммититься
        verify(consumer, never()).commitAsync(anyMap(), any());

        // Отправляем успешный Ack
        StreamObserver<KafkaBatchAck> ackObs = ackObservers.get(0);
        ackObs.onNext(KafkaBatchAck.newBuilder()
                .setBatchId(sentBatch.getBatchId())
                .setTopic(sentBatch.getTopic())
                .setSuccess(true)
                .setCommittedRecords(1)
                .build());

        // Даем воркеру время зафиксировать смещения в Kafka
        Thread.sleep(300);

        ArgumentCaptor<Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata>> captor =
                ArgumentCaptor.forClass(Map.class);
        verify(consumer, atLeastOnce()).commitAsync(captor.capture(), any());

        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed = captor.getValue();
        assertNotNull(committed.get(tp));
        assertEquals(43L, committed.get(tp).offset(), "Смещение должно быть сдвинуто на 42 + 1 = 43");
        assertEquals(1L, pool.getTotalMessagesReplicated(), "Счетчик переданных сообщений должен быть равен 1");
        assertTrue(pool.getTotalBytesReplicated() > 0, "Счетчик переданных байт должен быть больше 0");

        pool.close();
    }

    @Test
    @DisplayName("Динамическое обновление подписки на топики через updateSubscribedTopics")
    void testDynamicTopicResubscription() throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        KafkaReplicationClient client = mock(KafkaReplicationClient.class);
        when(consumer.subscription()).thenReturn(new HashSet<>(List.of("old-topic")));

        KafkaBatchConsumerPool pool = new KafkaBatchConsumerPool(
                consumer,
                client,
                new LoopPreventionFilter("dc1", "dc2"),
                new PayloadFormatClassifier(),
                transcoder,
                "dc1",
                "dc2",
                PayloadFormat.PAYLOAD_PASSTHROUGH,
                PayloadFormat.PAYLOAD_PASSTHROUGH
        );

        assertEquals(List.of("old-topic"), pool.getSubscribedTopics());

        // Обновляем топики
        pool.updateSubscribedTopics(List.of("new-topic-1", "new-topic-2"));

        assertEquals(List.of("new-topic-1", "new-topic-2"), pool.getSubscribedTopics());
        verify(consumer, atLeastOnce()).wakeup();

        // Повторный вызов с тем же составом не должен вызывать wakeup повторно
        reset(consumer);
        pool.updateSubscribedTopics(List.of("new-topic-2", "new-topic-1"));
        verify(consumer, never()).wakeup();

        pool.close();
    }
}
