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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

/**
 * Непрерывность коммита оффсетов Kafka.
 *
 * <p>Ключевая гарантия (DATA-LOSS): committed offset партиции может продвигаться только до
 * конца непрерывно подтвержденного префикса батчей. Если батч с меньшими оффсетами не
 * подтвержден (ack success=false, обрыв стрима), коммит более позднего батча безвозвратно
 * теряет записи между ними: при перезапуске консьюмер начнет читать уже за ними.
 */
@DisplayName("Kafka: непрерывность коммита оффсетов и обработка неуспешного ack")
class KafkaOffsetContinuityTest {

    private static ConsumerRecord<byte[], byte[]> record(String topic, int partition, long offset) {
        return new ConsumerRecord<>(topic, partition, offset, 1000L + offset,
                TimestampType.CREATE_TIME, 0, 0,
                ("k" + offset).getBytes(), ("v" + offset).getBytes(),
                new RecordHeaders(), Optional.empty());
    }

    /** Окружение: один консьюмер, одна партиция, по одному батчу на запись. */
    private static final class Harness implements AutoCloseable {
        final KafkaConsumer<byte[], byte[]> consumer;
        final KafkaReplicationClient client;
        final KafkaBatchConsumerPool pool;
        final List<KafkaBatchRequest> sent = new CopyOnWriteArrayList<>();
        final List<StreamObserver<KafkaBatchAck>> ackObservers = new CopyOnWriteArrayList<>();
        final TopicPartition tp = new TopicPartition("orders", 0);

        @SuppressWarnings("unchecked")
        Harness(long... offsets) {
            consumer = mock(KafkaConsumer.class);
            client = mock(KafkaReplicationClient.class);

            List<ConsumerRecord<byte[], byte[]>> recs = new ArrayList<>();
            for (long offset : offsets) {
                recs.add(record("orders", 0, offset));
            }
            ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(Map.of(tp, recs));
            AtomicBoolean delivered = new AtomicBoolean(false);
            when(consumer.poll(any(Duration.class)))
                    .thenAnswer(inv -> delivered.compareAndSet(false, true) ? batch : ConsumerRecords.empty());

            when(client.selectTargetIndex(anyInt())).thenReturn(0);
            when(client.openBatchStream(any())).thenAnswer(inv -> {
                ackObservers.add(inv.getArgument(0));
                return new StreamObserver<KafkaBatchRequest>() {
                    @Override
                    public void onNext(KafkaBatchRequest req) {
                        sent.add(req);
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
            when(transcoder.transcode(any(), any(), any(), any()))
                    .thenAnswer(inv -> inv.getArgument(1));

            pool = new KafkaBatchConsumerPool(consumer, client, loopFilter, classifier, transcoder,
                    "DC1", "DC2", PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH);
            pool.setMaxBatchSize(1);
        }

        void awaitSent(int count) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 3_000;
            while (sent.size() < count && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertTrue(sent.size() >= count,
                    "Ожидалось минимум " + count + " отправленных батчей, получено " + sent.size());
        }

        void ack(KafkaBatchRequest request, boolean success) {
            ackObservers.get(0).onNext(KafkaBatchAck.newBuilder()
                    .setBatchId(request.getBatchId())
                    .setTopic(request.getTopic())
                    .setSuccess(success)
                    .setCommittedRecords(success ? request.getRecordsCount() : 0)
                    .setErrorMessage(success ? "" : "target partition unavailable")
                    .build());
        }

        @SuppressWarnings("unchecked")
        long highestCommittedOffset() {
            ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
            try {
                verify(consumer, atLeastOnce()).commitAsync(captor.capture(), any());
            } catch (AssertionError noCommit) {
                return -1L;
            }
            long highest = -1L;
            for (Map<TopicPartition, OffsetAndMetadata> committed : captor.getAllValues()) {
                OffsetAndMetadata offset = committed.get(tp);
                if (offset != null) {
                    highest = Math.max(highest, offset.offset());
                }
            }
            return highest;
        }

        @Override
        public void close() {
            pool.close();
        }
    }

    @Test
    @DisplayName("Неуспешный ack раннего батча блокирует коммит более позднего успешного")
    void testFailedEarlierBatchBlocksLaterCommit() throws Exception {
        try (Harness h = new Harness(100L, 101L)) {
            h.pool.setMaxBatchRetries(0);
            h.pool.start();
            h.awaitSent(2);

            KafkaBatchRequest first = h.sent.get(0);
            KafkaBatchRequest second = h.sent.get(1);
            assertEquals(100L, first.getRecords(0).getOriginalOffset());
            assertEquals(101L, second.getRecords(0).getOriginalOffset());

            // Поздний батч подтвержден, ранний — отвергнут приемником
            h.ack(second, true);
            h.ack(first, false);
            Thread.sleep(400);

            long committed = h.highestCommittedOffset();
            assertTrue(committed <= 100L,
                    "Коммит ушел на " + committed + ": записи неподтвержденного батча (offset 100) потеряны");
        }
    }

    @Test
    @DisplayName("Непрерывный префикс коммитится целиком после подтверждения пропущенного батча")
    void testCommitAdvancesWhenGapIsFilled() throws Exception {
        try (Harness h = new Harness(100L, 101L)) {
            h.pool.setMaxBatchRetries(0);
            h.pool.start();
            h.awaitSent(2);

            KafkaBatchRequest first = h.sent.get(0);
            KafkaBatchRequest second = h.sent.get(1);

            h.ack(second, true);
            Thread.sleep(200);
            assertTrue(h.highestCommittedOffset() <= 100L, "До подтверждения раннего батча коммит не двигается");

            h.ack(first, true);
            Thread.sleep(400);

            assertEquals(102L, h.highestCommittedOffset(),
                    "После заполнения пропуска должен коммититься весь непрерывный префикс");
        }
    }

    @Test
    @DisplayName("Неуспешный ack приводит к повторной отправке батча")
    void testFailedAckTriggersRetry() throws Exception {
        try (Harness h = new Harness(100L)) {
            h.pool.setMaxBatchRetries(2);
            h.pool.start();
            h.awaitSent(1);

            KafkaBatchRequest first = h.sent.get(0);
            h.ack(first, false);

            h.awaitSent(2);
            KafkaBatchRequest retried = h.sent.get(1);
            assertEquals(100L, retried.getRecords(0).getOriginalOffset(),
                    "Повторно должен уйти тот же набор записей");
            assertTrue(h.highestCommittedOffset() <= 100L, "До успешного ack коммит не двигается");
        }
    }

    @Test
    @DisplayName("После исчерпания попыток батч уходит в DLQ, и только тогда оффсет подтверждается")
    void testExhaustedRetriesGoToDeadLetterHandler() throws Exception {
        try (Harness h = new Harness(100L)) {
            List<KafkaBatchRequest> deadLettered = new CopyOnWriteArrayList<>();
            h.pool.setMaxBatchRetries(1);
            h.pool.setDeadLetterHandler((request, reason) -> deadLettered.add(request));
            h.pool.start();
            h.awaitSent(1);

            h.ack(h.sent.get(0), false);
            h.awaitSent(2);
            h.ack(h.sent.get(1), false);

            long deadline = System.currentTimeMillis() + 3_000;
            while (deadLettered.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }

            assertEquals(1, deadLettered.size(), "Батч обязан попасть в DLQ после исчерпания попыток");
            assertEquals(100L, deadLettered.get(0).getRecords(0).getOriginalOffset());

            Thread.sleep(300);
            assertEquals(101L, h.highestCommittedOffset(),
                    "После изоляции батча в DLQ оффсет можно двигать дальше");
        }
    }

    @Test
    @DisplayName("Без DLQ-обработчика исчерпание попыток не двигает оффсет")
    void testExhaustedRetriesWithoutDeadLetterHandlerKeepOffset() throws Exception {
        try (Harness h = new Harness(100L)) {
            h.pool.setMaxBatchRetries(1);
            h.pool.start();
            h.awaitSent(1);

            h.ack(h.sent.get(0), false);
            h.awaitSent(2);
            h.ack(h.sent.get(1), false);
            Thread.sleep(400);

            assertTrue(h.highestCommittedOffset() <= 100L,
                    "Без DLQ потерянный батч обязан блокировать коммит, а не исчезать молча");
        }
    }
}
