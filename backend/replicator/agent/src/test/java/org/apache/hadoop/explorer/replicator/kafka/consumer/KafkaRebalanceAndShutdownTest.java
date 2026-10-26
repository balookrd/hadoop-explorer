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
import org.mockito.InOrder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Ребаланс группы и остановка воркера.
 *
 * <p>Пул подписывался на топики без {@code ConsumerRebalanceListener}, поэтому отзыв партиций
 * проходил для него незаметно. Последствия двойные. Во-первых, батчи отозванных партиций
 * оставались in-flight навсегда: их ack уже не придет (партиции обслуживает другой консьюмер), а
 * счетчик in-flight не обнуляется — петля backpressure перестает вычитывать и воркер встает.
 * Во-вторых, пул продолжал фиксировать оффсеты уже не принадлежащих ему партиций, а
 * подтвержденный, но не зафиксированный префикс при отзыве не коммитился вовсе — после
 * ребаланса эти записи приезжали повторно.
 *
 * <p>Остановка пула закрывала консьюмер без финальной фиксации: {@code commitAsync} без
 * последующего {@code poll} до брокера не доходит, поэтому подтвержденный префикс терялся и
 * после перезапуска агент заново гнал через WAN уже перенесенные записи.
 */
@DisplayName("Kafka: ребаланс группы и остановка воркера")
class KafkaRebalanceAndShutdownTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    private static final TopicPartition P1 = new TopicPartition("orders", 1);

    private MockConsumer<byte[], byte[]> consumer;
    private KafkaReplicationClient client;
    private KafkaBatchConsumerPool pool;
    private final List<KafkaBatchRequest> sent = new CopyOnWriteArrayList<>();
    private final List<StreamObserver<KafkaBatchAck>> ackObservers = new CopyOnWriteArrayList<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        MockConsumer<byte[], byte[]> real = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        real.subscribe(List.of("orders"));
        // Шпион поверх MockConsumer: подписка и ребаланс работают по-настоящему,
        // а факты фиксации оффсетов можно проверить
        consumer = spy(real);

        client = mock(KafkaReplicationClient.class);
        when(client.selectTargetIndex(anyInt())).thenReturn(0);
        when(client.getEndpointCount()).thenReturn(1);
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
        when(transcoder.transcode(any(), any(), any(), any())).thenAnswer(inv -> inv.getArgument(1));

        pool = new KafkaBatchConsumerPool(consumer, client, loopFilter, classifier, transcoder,
                "DC1", "DC2", PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH);
        pool.setMaxBatchSize(1);
        // Один батч в полете: так залипший батч сразу останавливает вычитку
        pool.setMaxInFlightBatches(1);
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    private void assign(TopicPartition... partitions) {
        List<TopicPartition> list = List.of(partitions);
        consumer.rebalance(list);
        Map<TopicPartition, Long> beginning = new HashMap<>();
        for (TopicPartition tp : list) {
            beginning.put(tp, 0L);
        }
        consumer.updateBeginningOffsets(beginning);
    }

    /**
     * Ребаланс в потоке консьюмера, внутри poll(), как его проводит настоящий Kafka.
     *
     * <p>MockConsumer вызывает колбэки ребаланса в потоке того, кто вызвал rebalance(). Из
     * тестового потока листенер пула работал параллельно с его же циклом вычитки — так в бою не
     * бывает, и тест изредка падал (CI): освобождение залипшего батча перемешивалось с циклом.
     */
    private void rebalanceOnPollThread(TopicPartition partition, Long recordOffset) {
        consumer.schedulePollTask(() -> {
            assign(partition);
            if (recordOffset != null) {
                addRecord(partition, recordOffset);
            }
        });
    }

    private void addRecord(TopicPartition tp, long offset) {
        consumer.addRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), offset,
                ("k" + offset).getBytes(), ("v" + offset).getBytes()));
    }

    private void awaitSent(int count) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (sent.size() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(sent.size() >= count,
                "Ожидалось минимум " + count + " отправленных батчей, получено " + sent.size());
    }

    private void ack(KafkaBatchRequest request) {
        ackObservers.get(0).onNext(KafkaBatchAck.newBuilder()
                .setBatchId(request.getBatchId())
                .setTopic(request.getTopic())
                .setSuccess(true)
                .setCommittedRecords(request.getRecordsCount())
                .build());
    }

    @Test
    @DisplayName("Отзыв партиции освобождает залипшие батчи, и воркер продолжает вычитку")
    void testRevokeReleasesStuckBatchesAndWorkerContinues() throws Exception {
        assign(P0);
        addRecord(P0, 100);
        pool.start();
        awaitSent(1);

        // Ack не приходит: партицию отзывают, пока батч в полете
        rebalanceOnPollThread(P1, 500L);

        awaitSent(2);
        assertEquals(1, sent.get(1).getRecords(0).getPartition(),
                "После ребаланса воркер обязан вычитывать новую партицию, а не стоять "
                        + "в ожидании ack по отозванной");
    }

    @Test
    @DisplayName("Отзыв партиции синхронно фиксирует подтвержденный префикс")
    void testRevokeCommitsAcknowledgedPrefix() throws Exception {
        assign(P0);
        addRecord(P0, 100);
        pool.start();
        awaitSent(1);
        ack(sent.get(0));

        rebalanceOnPollThread(P1, null);

        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
        verify(consumer, timeout(5_000).atLeastOnce()).commitSync(captor.capture());
        boolean committed = captor.getAllValues().stream()
                .map(offsets -> offsets.get(P0))
                .filter(java.util.Objects::nonNull)
                .anyMatch(offset -> offset.offset() == 101L);
        assertTrue(committed,
                "При отзыве партиции подтвержденный префикс обязан фиксироваться синхронно: "
                        + "commitAsync без последующего poll до брокера не доходит, и записи приедут "
                        + "повторно. Зафиксировано: " + captor.getAllValues());
    }

    @Test
    @DisplayName("Остановка пула фиксирует подтвержденные оффсеты до закрытия консьюмера")
    void testCloseCommitsAcknowledgedOffsets() throws Exception {
        assign(P0);
        addRecord(P0, 100);
        pool.start();
        awaitSent(1);
        ack(sent.get(0));

        pool.close();
        pool = null;

        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
        verify(consumer, atLeastOnce()).commitSync(captor.capture());
        boolean committed = captor.getAllValues().stream()
                .map(offsets -> offsets.get(P0))
                .filter(java.util.Objects::nonNull)
                .anyMatch(offset -> offset.offset() == 101L);
        assertTrue(committed,
                "Перед закрытием консьюмера пул обязан синхронно зафиксировать подтвержденный "
                        + "префикс, иначе после перезапуска агент заново гонит через WAN уже "
                        + "перенесенные записи. Зафиксировано: " + captor.getAllValues());

        InOrder order = inOrder(consumer);
        order.verify(consumer).commitSync(anyMap());
        order.verify(consumer).close();
    }
}
