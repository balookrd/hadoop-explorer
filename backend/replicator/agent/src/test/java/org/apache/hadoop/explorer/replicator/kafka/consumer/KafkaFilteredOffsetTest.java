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
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * Смещения партиции, записи которой целиком отфильтрованы.
 *
 * <p>В active-active фильтр петель отбрасывает эхо — записи, пришедшие из соседнего ДЦ. Если за
 * опрос в партиции не осталось ни одной записи на перенос, батч не отправляется, подтверждения
 * ждать неоткуда, и смещение не фиксируется никогда: лаг растет монотонно, а при перезапуске
 * топик перечитывается с начала.
 */
@DisplayName("Kafka: целиком отфильтрованная партиция фиксирует смещение")
class KafkaFilteredOffsetTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);

    private MockConsumer<byte[], byte[]> consumer;
    private KafkaBatchConsumerPool pool;
    private LoopPreventionFilter loopFilter;
    private final List<KafkaBatchRequest> sent = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        MockConsumer<byte[], byte[]> real = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        real.subscribe(List.of("orders"));
        consumer = spy(real);

        KafkaReplicationClient client = mock(KafkaReplicationClient.class);
        when(client.selectTargetIndex(anyInt())).thenReturn(0);
        when(client.getEndpointCount()).thenReturn(1);
        when(client.openBatchStream(any())).thenAnswer(inv -> new StreamObserver<KafkaBatchRequest>() {
            @Override
            public void onNext(KafkaBatchRequest req) {
                sent.add(req);
            }

            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        loopFilter = mock(LoopPreventionFilter.class);
        when(loopFilter.enrichHeaders(any())).thenReturn(List.of());
        PayloadFormatClassifier classifier = mock(PayloadFormatClassifier.class);
        when(classifier.classify(any(), any())).thenReturn(PayloadFormat.PAYLOAD_PASSTHROUGH);
        ZeroCopyWireTranscoder transcoder = mock(ZeroCopyWireTranscoder.class);
        when(transcoder.transcode(any(), any(), any(), any())).thenAnswer(inv -> inv.getArgument(1));

        pool = new KafkaBatchConsumerPool(consumer, client, loopFilter, classifier, transcoder,
                "DC1", "DC2", PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH);
        pool.setMaxBatchSize(10);
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    private void assign() {
        consumer.rebalance(List.of(P0));
        Map<TopicPartition, Long> beginning = new HashMap<>();
        beginning.put(P0, 0L);
        consumer.updateBeginningOffsets(beginning);
    }

    private void addRecord(long offset) {
        consumer.addRecord(new ConsumerRecord<>(P0.topic(), P0.partition(), offset,
                ("k" + offset).getBytes(), ("v" + offset).getBytes()));
    }

    @SuppressWarnings("unchecked")
    private Long awaitCommittedOffset() throws InterruptedException {
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                verify(consumer, atLeastOnce()).commitAsync(captor.capture(), any());
                for (Map<TopicPartition, OffsetAndMetadata> committed : captor.getAllValues()) {
                    OffsetAndMetadata offset = committed.get(P0);
                    if (offset != null) {
                        return offset.offset();
                    }
                }
            } catch (AssertionError ignored) {
                // фиксации еще не было
            }
            Thread.sleep(50);
        }
        return null;
    }

    @Test
    @DisplayName("Эхо соседнего ДЦ отфильтровано целиком — смещение все равно фиксируется")
    void fullyFilteredPartitionShouldCommitOffset() throws Exception {
        when(loopFilter.shouldReplicate(any())).thenReturn(false);
        assign();
        pool.start();
        addRecord(0);
        addRecord(1);
        addRecord(2);

        Long committed = awaitCommittedOffset();

        assertNotNull(committed, "Смещение отфильтрованной партиции должно фиксироваться");
        assertEquals(3L, committed, "Фиксируется позиция за последней отфильтрованной записью");
        assertTrue(sent.isEmpty(), "Отфильтрованные записи не отправляются");
    }

    @Test
    @DisplayName("Записи на перенос фиксируются по подтверждению, а не по факту чтения")
    void recordsToReplicateShouldNotBeCommittedBeforeAck() throws Exception {
        when(loopFilter.shouldReplicate(any())).thenReturn(true);
        assign();
        pool.start();
        addRecord(0);

        Thread.sleep(500);

        assertTrue(sent.size() >= 1, "Запись должна уйти на перенос");
        assertNull(awaitCommittedOffsetQuickly(),
                "Без подтверждения приемника смещение фиксировать нельзя");
    }

    @SuppressWarnings("unchecked")
    private Long awaitCommittedOffsetQuickly() {
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
        try {
            verify(consumer, atLeastOnce()).commitAsync(captor.capture(), any());
        } catch (AssertionError e) {
            return null;
        }
        for (Map<TopicPartition, OffsetAndMetadata> committed : captor.getAllValues()) {
            OffsetAndMetadata offset = committed.get(P0);
            if (offset != null) {
                return offset.offset();
            }
        }
        return null;
    }
}
