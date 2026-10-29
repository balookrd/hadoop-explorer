package org.apache.hadoop.explorer.replicator.kafka.consumer;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier;
import org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Предел памяти отправителя на неподтвержденные батчи.
 *
 * <p>Предел был только по числу батчей (100 на поток), а батч бывает до 16 МБ: пока приемник
 * тормозит, батчи копились в хипе отправителя до OutOfMemoryError.
 */
@DisplayName("Kafka: отправитель не держит неподтвержденных батчей сверх бюджета памяти")
class SenderMemoryBudgetTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);

    private KafkaBatchConsumerPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    @DisplayName("Без подтверждений вычитка встает на бюджете байтов, а не на 100 батчах")
    void testSenderStopsAtByteBudget() throws Exception {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.subscribe(List.of("orders"));

        // Приемник не подтверждает ничего
        List<KafkaBatchRequest> sent = new CopyOnWriteArrayList<>();
        KafkaReplicationClient client = mock(KafkaReplicationClient.class);
        when(client.selectTargetIndex(anyInt())).thenReturn(0);
        when(client.getEndpointCount()).thenReturn(1);
        when(client.openBatchStream(any())).thenAnswer(inv -> new StreamObserver<KafkaBatchRequest>() {
            @Override public void onNext(KafkaBatchRequest req) {
                sent.add(req);
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        LoopPreventionFilter loopFilter = mock(LoopPreventionFilter.class);
        when(loopFilter.shouldReplicate(any())).thenReturn(true);
        when(loopFilter.enrichHeaders(any())).thenReturn(List.of());
        PayloadFormatClassifier classifier = mock(PayloadFormatClassifier.class);
        when(classifier.classify(any(), any())).thenReturn(PayloadFormat.PAYLOAD_PASSTHROUGH);
        ZeroCopyWireTranscoder transcoder = mock(ZeroCopyWireTranscoder.class);
        when(transcoder.transcode(any(), any(), any(), any())).thenAnswer(inv -> inv.getArgument(1));

        pool = new KafkaBatchConsumerPool(consumer, client, loopFilter, classifier, transcoder,
                "DC1", "DC2", PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH);
        pool.setMaxBatchSize(1);
        pool.setMaxInFlightBytes(3 * 1024);
        consumer.rebalance(List.of(P0));
        consumer.updateBeginningOffsets(Map.of(P0, 0L));
        pool.start();

        byte[] payload = new byte[1024];
        for (long offset = 0; offset < 30; offset++) {
            consumer.addRecord(new ConsumerRecord<>("orders", 0, offset, "k".getBytes(), payload));
            Thread.sleep(30);
        }

        assertTrue(sent.size() >= 3, "до бюджета батчи уходят: " + sent.size());
        assertTrue(sent.size() <= 5,
                "по ~1 КБ при бюджете 3 КБ в полете не больше нескольких батчей, а отправлено: " + sent.size());
        assertTrue(pool.getInFlightBytes() >= 3 * 1024);
    }
}
