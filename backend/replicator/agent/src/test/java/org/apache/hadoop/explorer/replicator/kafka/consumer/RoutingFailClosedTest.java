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
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Маршрутизируемый топик без своих приемников в пуле не уезжает в чужой кластер.
 *
 * <p>Маршруты «топик — приемники его задачи» вводились потому, что HRW-балансировщик раскидывал
 * партиции по всем приемникам ЦОД, и при двух задачах с разными целевыми кластерами часть данных
 * уходила не туда. Но при промахе маршрута выбор молча падал на весь пул — то есть именно в чужой
 * кластер, против чего маршруты и были нужны, причем комментарий в коде утверждал обратное.
 *
 * <p>Теперь выбор приемника возвращает {@code NO_TARGET}, партиция снимается с вычитки со
 * сдвигом позиции назад, а оффсеты не фиксируются: репликация этой партиции встает до появления
 * своих приемников, но данные не теряются и не уезжают чужому кластеру.
 */
@DisplayName("Kafka: промах маршрута не уводит данные в чужой кластер")
class RoutingFailClosedTest {

    private static final TopicPartition ORDERS_0 = new TopicPartition("orders", 0);

    private final List<KafkaBatchRequest> sent = new CopyOnWriteArrayList<>();
    private KafkaBatchConsumerPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @SuppressWarnings("unchecked")
    private KafkaConsumer<byte[], byte[]> consumerWithOneBatch() {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "orders", 0, 100L, 1_700_000_000_000L, TimestampType.CREATE_TIME, 0, 0,
                "k".getBytes(), "v".getBytes(), new RecordHeaders(), Optional.empty());
        ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(Map.of(ORDERS_0, List.of(record)));
        when(consumer.poll(any(Duration.class))).thenReturn(batch, ConsumerRecords.empty());
        return consumer;
    }

    private KafkaBatchConsumerPool poolFor(KafkaConsumer<byte[], byte[]> consumer,
                                           KafkaReplicationClient client) {
        LoopPreventionFilter loopFilter = mock(LoopPreventionFilter.class);
        when(loopFilter.shouldReplicate(any())).thenReturn(true);
        when(loopFilter.enrichHeaders(any())).thenReturn(List.of());
        PayloadFormatClassifier classifier = mock(PayloadFormatClassifier.class);
        when(classifier.classify(any(), any())).thenReturn(PayloadFormat.PAYLOAD_PASSTHROUGH);
        ZeroCopyWireTranscoder transcoder = mock(ZeroCopyWireTranscoder.class);
        when(transcoder.transcode(any(), any(), any(), any())).thenAnswer(inv -> inv.getArgument(1));

        KafkaBatchConsumerPool created = new KafkaBatchConsumerPool(consumer, client, loopFilter,
                classifier, transcoder, "DC1", "DC2",
                PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH);
        created.setMaxBatchSize(1);
        return created;
    }

    private KafkaReplicationClient clientWithoutRoute() {
        KafkaReplicationClient client = mock(KafkaReplicationClient.class);
        when(client.getEndpointCount()).thenReturn(1);
        when(client.selectTargetIndex(anyString(), anyInt())).thenReturn(KafkaReplicationClient.NO_TARGET);
        when(client.selectTargetIndex(anyInt())).thenReturn(0);
        when(client.openBatchStream(any())).thenAnswer(inv -> new StreamObserver<KafkaBatchRequest>() {
            @Override
            public void onNext(KafkaBatchRequest value) {
                sent.add(value);
            }

            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });
        return client;
    }

    @Test
    @DisplayName("Батч не отправляется, партиция приостановлена, оффсеты не зафиксированы")
    void testBatchIsHeldWhenRouteHasNoTargets() throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = consumerWithOneBatch();
        pool = poolFor(consumer, clientWithoutRoute());

        pool.start();
        verify(consumer, timeout(5_000)).pause(List.of(ORDERS_0));

        assertTrue(sent.isEmpty(),
                "Батч маршрутизируемого топика ушел в чужой кластер: " + sent);
        verify(consumer).seek(ORDERS_0, 100L);
        verify(consumer, never()).commitAsync(any(), any());
        verify(consumer, never()).commitSync(anyMap());
    }

    @Test
    @DisplayName("После появления приемников маршрута вычитка партиции возобновляется")
    void testPartitionResumesWhenRouteAppears() throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = consumerWithOneBatch();
        KafkaReplicationClient client = clientWithoutRoute();
        pool = poolFor(consumer, client);

        pool.start();
        verify(consumer, timeout(5_000)).pause(List.of(ORDERS_0));

        // Приемники задачи появились в пуле
        when(client.selectTargetIndex(anyString(), anyInt())).thenReturn(0);

        verify(consumer, timeout(5_000)).resume(List.of(ORDERS_0));
    }
}
