package org.apache.hadoop.explorer.replicator.kafka.integration;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaDataTransferServiceGrpc;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecord;
import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.apache.hadoop.explorer.replicator.kafka.service.KafkaDataTransferServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Порядок записей партиции на настоящей Kafka при включенном конвейере приемника.
 *
 * <p>Все остальные тесты Kafka построены на {@code mock(KafkaProducer)}: они проверяют, какие
 * вызовы сделаны, но не то, в каком порядке записи оказались в журнале. Между тем приемник
 * нарочно обрабатывает до {@code pipelineWindow} батчей одновременно (по умолчанию 16), чтобы
 * убрать задержку stop-and-wait на WAN, и каждый батч раньше уходил в producer независимо.
 * Для одной партиции это означало перестановку: батч с меньшими оффсетами мог быть записан
 * после батча с большими, и журнал приемника перестает соответствовать источнику — для CDC и
 * компактированных топиков это молчаливая порча данных.
 *
 * <p>Брокер поднимается в контейнере через Docker CLI (см. {@link KafkaTestBroker}); там, где
 * Docker недоступен, тест пропускается.
 */
@org.junit.jupiter.api.condition.EnabledIf("org.apache.hadoop.explorer.replicator.kafka.integration.KafkaTestBroker#isDockerAvailable")
@DisplayName("Kafka в контейнере: порядок записей партиции при конвейере приемника")
class RealKafkaPipelineOrderTest {

    private static final String TOPIC = "orders-target";
    private static final int BATCHES = 60;
    /** Конвейер приемника: несколько батчей обрабатываются одновременно. */
    private static final int PIPELINE_WINDOW = 16;

    private static KafkaTestBroker broker;

    private KafkaProducer<byte[], byte[]> producer;
    private KafkaBatchProducer batchProducer;
    private Server server;
    private ManagedChannel channel;

    @org.junit.jupiter.api.BeforeAll
    static void startBroker() throws Exception {
        broker = KafkaTestBroker.start();
    }

    @org.junit.jupiter.api.AfterAll
    static void stopBroker() {
        if (broker != null) {
            broker.close();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        broker.createTopic(TOPIC, 2);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Несколько запросов в полете: именно так перестановка и становится возможной,
        // если батчи одной партиции не выстроены в цепочку
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        producer = new KafkaProducer<>(props);
        batchProducer = new KafkaBatchProducer(producer);

        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(new KafkaDataTransferServiceImpl(batchProducer, null, "DC2", PIPELINE_WINDOW))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (batchProducer != null) {
            batchProducer.close();
        }
    }

    @Test
    @DisplayName("Батчи одной партиции попадают в журнал в порядке отправки")
    void testPartitionOrderPreservedUnderPipelining() throws Exception {
        CountDownLatch acked = new CountDownLatch(BATCHES);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<String> rejected = new ArrayList<>();

        StreamObserver<KafkaBatchRequest> request = KafkaDataTransferServiceGrpc.newStub(channel)
                .streamBatches(new StreamObserver<>() {
                    @Override
                    public void onNext(KafkaBatchAck ack) {
                        if (!ack.getSuccess()) {
                            rejected.add(ack.getBatchId() + ": " + ack.getErrorMessage());
                        }
                        acked.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        failure.set(t);
                        while (acked.getCount() > 0) {
                            acked.countDown();
                        }
                    }

                    @Override
                    public void onCompleted() {
                    }
                });

        for (int i = 0; i < BATCHES; i++) {
            String payload = String.format("msg-%03d", i);
            request.onNext(KafkaBatchRequest.newBuilder()
                    .setBatchId("batch-" + i)
                    .setSourceDc("DC1")
                    .setTargetDc("DC2")
                    .setTopic(TOPIC)
                    .setPayloadFormat(PayloadFormat.PAYLOAD_PASSTHROUGH)
                    .addRecords(KafkaRecord.newBuilder()
                            .setPartition(0)
                            .setOriginalOffset(i)
                            .setTimestamp(System.currentTimeMillis())
                            .setKey(ByteString.copyFromUtf8("key-" + i))
                            .setPayload(ByteString.copyFromUtf8(payload))
                            .build())
                    .build());
        }
        request.onCompleted();

        assertTrue(acked.await(120, TimeUnit.SECONDS),
                "Приемник не подтвердил все батчи, осталось: " + acked.getCount());
        assertNull(failure.get(), () -> "Стрим упал: " + failure.get());
        assertTrue(rejected.isEmpty(), "Приемник отклонил батчи: " + rejected);

        List<String> written = readTopicPartition(new TopicPartition(TOPIC, 0));

        assertEquals(BATCHES, written.size(),
                "В журнале приемника не все записи: " + written.size() + " из " + BATCHES);
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < BATCHES; i++) {
            expected.add(String.format("msg-%03d", i));
        }
        assertEquals(expected, written,
                "Порядок записей партиции не сохранен: журнал приемника перестал соответствовать "
                        + "источнику, а для CDC и компактированных топиков это порча данных");
    }

    private List<String> readTopicPartition(TopicPartition partition) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-verifier");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        List<String> values = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.currentTimeMillis() + 60_000;
            while (values.size() < BATCHES && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
                    values.add(new String(record.value(), StandardCharsets.UTF_8));
                }
            }
        }
        return values;
    }
}
