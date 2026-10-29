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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Партиция, вставшая после отказов приемника, возвращается к вычитке сама.
 *
 * <p>Повторы отвергнутого батча идут сразу, и короткий сбой записи на приемнике исчерпывал их
 * за миллисекунды. Без DLQ партиция стояла до перезапуска пула, а лаг задачи этого не показывал.
 */
@DisplayName("Kafka: остановленная партиция перечитывается после паузы")
class StalledPartitionRecoveryTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);

    private KafkaBatchConsumerPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    @DisplayName("После исчерпания повторов партиция видна как стоящая и затем доставляется")
    void testStalledPartitionIsReportedAndRecovered() throws Exception {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.subscribe(List.of("orders"));

        // Первые четыре отправки (исходная и три повтора) приемник отвергает
        AtomicInteger rejectionsLeft = new AtomicInteger(4);
        List<KafkaBatchRequest> delivered = new CopyOnWriteArrayList<>();
        AtomicReference<StreamObserver<KafkaBatchAck>> acks = new AtomicReference<>();
        KafkaReplicationClient client = mock(KafkaReplicationClient.class);
        when(client.selectTargetIndex(anyInt())).thenReturn(0);
        when(client.getEndpointCount()).thenReturn(1);
        when(client.openBatchStream(any())).thenAnswer(inv -> {
            acks.set(inv.getArgument(0));
            return new StreamObserver<KafkaBatchRequest>() {
                @Override
                public void onNext(KafkaBatchRequest req) {
                    boolean reject = rejectionsLeft.getAndDecrement() > 0;
                    if (!reject) {
                        delivered.add(req);
                    }
                    acks.get().onNext(KafkaBatchAck.newBuilder()
                            .setBatchId(req.getBatchId()).setTopic(req.getTopic())
                            .setSuccess(!reject).setCommittedRecords(reject ? 0 : req.getRecordsCount())
                            .setErrorMessage(reject ? "TOPIC_AUTHORIZATION_FAILED" : "").build());
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
        pool.setStallRecoveryMs(500);
        consumer.rebalance(List.of(P0));
        consumer.updateBeginningOffsets(Map.of(P0, 0L));
        consumer.addRecord(new ConsumerRecord<>("orders", 0, 0L, "k0".getBytes(), "v0".getBytes()));
        consumer.addRecord(new ConsumerRecord<>("orders", 0, 1L, "k1".getBytes(), "v1".getBytes()));
        pool.start();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (pool.getStalledPartitions().isEmpty() && delivered.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(java.util.Set.of(P0), pool.getStalledPartitions(),
                "партиция с недоставленным батчем должна быть видна как стоящая");

        // Партиция возвращается к вычитке; MockConsumer не отдает повторно уже вычитанное после
        // seek, поэтому записи с недоставленного оффсета подкладываются, как их отдал бы брокер
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!pool.getStalledPartitions().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(pool.getStalledPartitions().isEmpty(), "партиция не вернулась к вычитке после паузы");
        assertEquals(0L, consumer.position(P0), "вычитка повторяется с недоставленного батча");
        consumer.addRecord(new ConsumerRecord<>("orders", 0, 0L, "k0".getBytes(), "v0".getBytes()));
        consumer.addRecord(new ConsumerRecord<>("orders", 0, 1L, "k1".getBytes(), "v1".getBytes()));

        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (delivered.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertFalse(delivered.isEmpty(), "партиция так и не вернулась к вычитке");
        KafkaBatchRequest retried = delivered.get(0);
        assertEquals(0L, retried.getRecords(0).getOriginalOffset(), "вычитка повторяется с недоставленного батча");
        assertFalse(retried.hasPrevMaxSourceOffset(), "приемник начинает последовательность партиции заново");
        assertTrue(pool.getStalledPartitions().isEmpty());
    }
}
