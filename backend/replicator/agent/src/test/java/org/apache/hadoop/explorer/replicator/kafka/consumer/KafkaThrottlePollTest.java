package org.apache.hadoop.explorer.replicator.kafka.consumer;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier;
import org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Ограничитель полосы не должен удерживать поток опроса.
 *
 * <p>{@code throttle()} спит в том же потоке, который вызывает {@code poll()}. Пока он спит,
 * брокер не видит опроса: при узком WAN пауза выходит за {@code max.poll.interval.ms}, консьюмера
 * исключают из группы, и начинается ребаланс — ровно в тот момент, когда канал и без того
 * перегружен.
 */
@DisplayName("Kafka: ограничитель полосы не удерживает поток опроса")
class KafkaThrottlePollTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    /** Опрос обязан повторяться чаще, чем раз в это время, как бы ни тормозил ограничитель. */
    private static final long MAX_SILENCE_MS = 400;

    private MockConsumer<byte[], byte[]> consumer;
    private KafkaBatchConsumerPool pool;
    private final List<Long> pollMoments = new CopyOnWriteArrayList<>();
    private final List<KafkaBatchRequest> sent = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        MockConsumer<byte[], byte[]> real = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        real.subscribe(List.of("orders"));
        consumer = spy(real);
        doAnswer(invocation -> {
            pollMoments.add(System.nanoTime());
            return invocation.callRealMethod();
        }).when(consumer).poll(any(Duration.class));

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
        pool.setMaxInFlightBatches(100);
        // Узкая полоса: каждый батч в килобайт требует около секунды ожидания
        pool.setBandwidthLimiter(new LocalBandwidthLimiter(0.001));
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    @DisplayName("Пока ограничитель держит батч, опрос продолжается")
    void throttlingDoesNotSilenceThePollLoop() throws Exception {
        consumer.rebalance(List.of(P0));
        Map<TopicPartition, Long> beginning = new HashMap<>();
        beginning.put(P0, 0L);
        consumer.updateBeginningOffsets(beginning);

        byte[] value = new byte[1024];
        for (long offset = 0; offset < 5; offset++) {
            consumer.addRecord(new ConsumerRecord<>(P0.topic(), P0.partition(), offset,
                    ("k" + offset).getBytes(), value));
        }

        pool.start();

        long deadline = System.currentTimeMillis() + 10_000;
        while (sent.size() < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertFalse(sent.isEmpty(), "батчи должны отправляться");

        // Молчанием считается и хвост: от последнего опроса до этого момента брокер тоже
        // не видит консьюмера, и именно так он выпадает из группы посреди обработки.
        List<Long> moments = List.copyOf(pollMoments);
        long worstSilenceMs = 0;
        for (int i = 1; i < moments.size(); i++) {
            worstSilenceMs = Math.max(worstSilenceMs, (moments.get(i) - moments.get(i - 1)) / 1_000_000L);
        }
        if (!moments.isEmpty()) {
            worstSilenceMs = Math.max(worstSilenceMs,
                    (System.nanoTime() - moments.get(moments.size() - 1)) / 1_000_000L);
        }

        assertTrue(worstSilenceMs <= MAX_SILENCE_MS,
                "самая долгая пауза между опросами " + worstSilenceMs + " мс при "
                        + moments.size() + " опросах, допустимо " + MAX_SILENCE_MS);
    }
}
