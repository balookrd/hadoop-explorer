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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * Частота асинхронных коммитов оффсетов.
 *
 * <p>Подтвержденные оффсеты коммитились на каждой итерации цикла вычитки, в том числе когда
 * ничего не изменилось. Простаивающий или ждущий backpressure консьюмер слал OffsetCommit
 * десятки раз в секунду, а 72 консьюмера задачи на 12 агентах нагружали координатор группы
 * боевого кластера источника десятками тысяч запросов в секунду.
 */
@DisplayName("Kafka: коммит оффсетов только при изменении и с ограничением частоты")
class KafkaCommitRateTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);

    private KafkaBatchConsumerPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    @DisplayName("Неизменившаяся позиция не коммитится повторно, пока консьюмер простаивает")
    @SuppressWarnings("unchecked")
    void testIdleConsumerDoesNotRecommitSameOffset() throws Exception {
        MockConsumer<byte[], byte[]> real = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        real.subscribe(List.of("orders"));
        MockConsumer<byte[], byte[]> consumer = spy(real);

        AtomicReference<StreamObserver<KafkaBatchAck>> acks = new AtomicReference<>();
        KafkaReplicationClient client = mock(KafkaReplicationClient.class);
        when(client.selectTargetIndex(anyInt())).thenReturn(0);
        when(client.getEndpointCount()).thenReturn(1);
        when(client.openBatchStream(any())).thenAnswer(inv -> {
            acks.set(inv.getArgument(0));
            return new StreamObserver<KafkaBatchRequest>() {
                @Override
                public void onNext(KafkaBatchRequest req) {
                    acks.get().onNext(KafkaBatchAck.newBuilder()
                            .setBatchId(req.getBatchId()).setTopic(req.getTopic())
                            .setSuccess(true).setCommittedRecords(req.getRecordsCount()).build());
                }

                @Override public void onError(Throwable t) {}
                @Override public void onCompleted() {}
            };
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
        consumer.rebalance(List.of(P0));
        consumer.updateBeginningOffsets(Map.of(P0, 0L));
        consumer.addRecord(new ConsumerRecord<>("orders", 0, 0L, "k".getBytes(), "v".getBytes()));
        pool.start();

        // Запись подтверждена, дальше консьюмер только простаивает
        Thread.sleep(1_500);

        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
        verify(consumer, atLeastOnce()).commitAsync(captor.capture(), any());
        List<Map<TopicPartition, OffsetAndMetadata>> commits = captor.getAllValues();
        assertEquals(1L, commits.get(commits.size() - 1).get(P0).offset());
        assertEquals(1, commits.size(), "одна и та же позиция коммитилась повторно: " + commits.size() + " раз");
    }
}
