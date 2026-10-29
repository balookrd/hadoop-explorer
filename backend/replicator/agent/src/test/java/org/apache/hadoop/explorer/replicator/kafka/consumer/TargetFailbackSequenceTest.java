package org.apache.hadoop.explorer.replicator.kafka.consumer;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier;
import org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder;
import org.apache.hadoop.explorer.replicator.kafka.service.KafkaDataTransferServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Возврат партиции на прежний приемник после карантина.
 *
 * <p>Приемник помнит последнее принятое смещение партиции и отклоняет батч, который не
 * продолжает его. Сбой стрима отправляет приемник в карантин на 10 с, и партиция на это время
 * уходит на соседний приемник. Когда карантин истекает, HRW возвращает партицию обратно, а
 * прежний приемник все еще помнит смещение до переезда: каждый батч отклоняется как «вне
 * последовательности», повторы упираются в ту же проверку, и без DLQ партиция встает навсегда.
 */
@DisplayName("Kafka: партиция возвращается на приемник после карантина")
class TargetFailbackSequenceTest {

    private static final String TOPIC = "events";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final ConcurrentLinkedQueue<ConsumerRecords<byte[], byte[]>> polls = new ConcurrentLinkedQueue<>();
    /** Значения, записанные в целевой кластер всеми приемниками. */
    private final List<String> written = new CopyOnWriteArrayList<>();
    private KafkaBatchConsumerPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
    }

    @SuppressWarnings("unchecked")
    private ManagedChannel receiver() throws Exception {
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(inv -> {
            ProducerRecord<byte[], byte[]> rec = inv.getArgument(0);
            written.add(new String(rec.value(), StandardCharsets.UTF_8));
            Callback callback = inv.getArgument(1);
            callback.onCompletion(new RecordMetadata(new TopicPartition(rec.topic(), 0), 0L, 0, 0L, 0, 0), null);
            return CompletableFuture.completedFuture(null);
        });
        String name = "failback-" + UUID.randomUUID();
        servers.add(InProcessServerBuilder.forName(name).directExecutor()
                .addService(new KafkaDataTransferServiceImpl(new KafkaBatchProducer(producer), null, "DC2"))
                .build().start());
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        channels.add(channel);
        return channel;
    }

    private static ConsumerRecords<byte[], byte[]> records(long from, long to) {
        List<ConsumerRecord<byte[], byte[]>> list = new ArrayList<>();
        for (long offset = from; offset <= to; offset++) {
            list.add(new ConsumerRecord<>(TOPIC, 0, offset, 1_700_000_000_000L, TimestampType.CREATE_TIME, 0, 0,
                    ("k" + offset).getBytes(StandardCharsets.UTF_8),
                    ("v" + offset).getBytes(StandardCharsets.UTF_8),
                    new RecordHeaders(), Optional.empty()));
        }
        return new ConsumerRecords<>(Map.of(TP, list));
    }

    private void awaitWritten(int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (written.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
    }

    @Test
    @DisplayName("После возврата с соседнего приемника записи партиции продолжают доезжать")
    @SuppressWarnings("unchecked")
    void testPartitionKeepsFlowingAfterFailback() throws Exception {
        KafkaReplicationClient client = new KafkaReplicationClient(
                List.of(receiver(), receiver()), List.of("agent-a:50052", "agent-b:50052"));

        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.poll(any(Duration.class))).thenAnswer(inv -> {
            ConsumerRecords<byte[], byte[]> next = polls.poll();
            return next != null ? next : ConsumerRecords.empty();
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
        pool.start();

        int home = client.selectTargetIndex(TOPIC, 0);

        // 1. Партиция пишется своим приемником
        polls.add(records(0, 9));
        awaitWritten(10);
        assertEquals(10, written.size(), "первая пачка не доехала: " + written);

        // 2. Сбой стрима: приемник в карантине, партиция переезжает на соседа
        client.markTargetSuspect(home, new RuntimeException("stream reset"));
        assertNotEquals(home, client.selectTargetIndex(TOPIC, 0));
        polls.add(records(10, 19));
        awaitWritten(20);
        assertEquals(20, written.size(), "пачка на соседнем приемнике не доехала: " + written);

        // 3. Карантин истек: партиция возвращается к прежнему приемнику
        client.getBalancer().markHealthy(home);
        assertEquals(home, client.selectTargetIndex(TOPIC, 0));
        polls.add(records(20, 29));
        awaitWritten(30);

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            expected.add("v" + i);
        }
        assertEquals(expected, written,
                "после возврата на прежний приемник записи партиции перестали доезжать");
    }
}
