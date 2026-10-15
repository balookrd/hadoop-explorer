package org.apache.hadoop.explorer.replicator.kafka;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.hadoop.explorer.replicator.kafka.service.KafkaDataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.*;
import org.apache.hadoop.explorer.replicator.kafka.offset.SafetyCommitManager;
import org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier;
import org.apache.hadoop.explorer.replicator.kafka.schema.CachedSchemaRegistryResolver;
import org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.HwxRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig;
import org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder;
import org.apache.hadoop.explorer.replicator.kafka.security.SpnegoAuthenticator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Комплексный E2E Smoke Test для Data Plane репликации Kafka через gRPC:
 * 1. Керберизованная среда Schema Registry с обязательным протоколом SPNEGO (RFC 4559 Negotiate);
 * 2. Сквозной gRPC поток батчей между дата-центрами (DC1 -> DC2);
 * 3. Zero-Copy транскодинг Avro с авторегистрацией схем;
 * 4. Прямой байтовый проброс (Passthrough) для топиков без схемы (JSON, Raw);
 * 5. Безопасная обработка Tombstone (удаление по ключу);
 * 6. Защита от бесконечных циклов (Loop Prevention Filter);
 * 7. Безопасная синхронизация смещений Consumer Groups с защитным гейтом.
 */
@DisplayName("E2E Data Plane Smoke Test: Kafka gRPC Replicator с Kerberos SASL и SPNEGO SR")
class KafkaReplicatorE2ESmokeTest {

    private static HttpServer kerberizedSrServer;
    private static int srPort;
    private static String srBaseUrl;

    private static final AtomicInteger hwxRequestsCount = new AtomicInteger(0);
    private static final AtomicInteger confluentRequestsCount = new AtomicInteger(0);
    private static final AtomicInteger spnegoChallengesCount = new AtomicInteger(0);

    private Server grpcServer;
    private ManagedChannel grpcChannel;
    private KafkaProducer<byte[], byte[]> mockKafkaProducer;
    private AdminClient mockAdminClient;
    private KafkaConsumer<byte[], byte[]> mockResolverConsumer;
    private SafetyCommitManager safetyCommitManager;
    private KafkaBatchProducer batchProducer;
    private KafkaDataTransferServiceImpl dataTransferService;
    private KafkaReplicationClient replicationClient;

    private static final String SERVER_NAME = InProcessServerBuilder.generateName();

    @BeforeAll
    static void startKerberizedSchemaRegistryServer() throws IOException {
        kerberizedSrServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srPort = kerberizedSrServer.getAddress().getPort();
        srBaseUrl = "http://127.0.0.1:" + srPort;

        // 1. Hortonworks Schema Registry с обязательной проверкой SPNEGO Negotiate
        kerberizedSrServer.createContext("/api/v1/schemaregistry", exchange -> {
            hwxRequestsCount.incrementAndGet();
            String auth = exchange.getRequestHeaders().getFirst("Authorization");

            // При отсутствии заголовка отдаем 401 challenge
            if (auth == null || !auth.regionMatches(true, 0, "Negotiate ", 0, 10)) {
                spnegoChallengesCount.incrementAndGet();
                exchange.getResponseHeaders().set("WWW-Authenticate", "Negotiate");
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }

            String path = exchange.getRequestURI().getPath();
            String responseJson;
            int statusCode = 200;

            if (path.contains("/schemas/versions/")) {
                responseJson = "{\"versionId\":1001,\"schemaText\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Order\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"string\\\"}]}\"}";
            } else if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                statusCode = 201;
                responseJson = "1001";
            } else {
                responseJson = "{\"name\":\"orders-avro-value\",\"compatibility\":\"BACKWARD\"}";
            }

            byte[] bytes = responseJson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        // 2. Confluent Schema Registry с обязательной проверкой SPNEGO Negotiate
        kerberizedSrServer.createContext("/schemas", exchange -> {
            confluentRequestsCount.incrementAndGet();
            String auth = exchange.getRequestHeaders().getFirst("Authorization");

            if (auth == null || !auth.regionMatches(true, 0, "Negotiate ", 0, 10)) {
                spnegoChallengesCount.incrementAndGet();
                exchange.getResponseHeaders().set("WWW-Authenticate", "Negotiate");
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }

            String responseJson = "{\"id\":42,\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Order\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"string\\\"}]}\"}";
            byte[] bytes = responseJson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        kerberizedSrServer.createContext("/subjects", exchange -> {
            confluentRequestsCount.incrementAndGet();
            String auth = exchange.getRequestHeaders().getFirst("Authorization");

            if (auth == null || !auth.regionMatches(true, 0, "Negotiate ", 0, 10)) {
                spnegoChallengesCount.incrementAndGet();
                exchange.getResponseHeaders().set("WWW-Authenticate", "Negotiate");
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }

            String responseJson = "{\"id\":42}";
            byte[] bytes = responseJson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        kerberizedSrServer.start();
    }

    @AfterAll
    static void stopKerberizedSchemaRegistryServer() {
        if (kerberizedSrServer != null) {
            kerberizedSrServer.stop(0);
        }
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws IOException {
        mockKafkaProducer = mock(KafkaProducer.class);
        Future<RecordMetadata> successFuture = CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("target-topic", 0), 0, 0, System.currentTimeMillis(), 0, 0)
        );
        when(mockKafkaProducer.send(any(ProducerRecord.class))).thenReturn(successFuture);

        mockAdminClient = mock(AdminClient.class);
        mockResolverConsumer = mock(KafkaConsumer.class);
        safetyCommitManager = new SafetyCommitManager(mockAdminClient, mockResolverConsumer);
        batchProducer = new KafkaBatchProducer(mockKafkaProducer);

        dataTransferService = new KafkaDataTransferServiceImpl(batchProducer, safetyCommitManager, "DC2");
        grpcServer = InProcessServerBuilder.forName(SERVER_NAME)
                .directExecutor()
                .addService(dataTransferService)
                .build()
                .start();

        grpcChannel = InProcessChannelBuilder.forName(SERVER_NAME)
                .directExecutor()
                .build();

        replicationClient = new KafkaReplicationClient(grpcChannel);
    }

    @AfterEach
    void tearDown() {
        if (grpcChannel != null) {
            grpcChannel.shutdownNow();
        }
        if (grpcServer != null) {
            grpcServer.shutdownNow();
        }
    }

    @Test
    @DisplayName("Сквозной поток репликации: Kerberos SASL + SPNEGO SR + Zero-Copy + Passthrough + Tombstones + Offsets")
    void shouldExecuteEndToEndDataPlaneReplication() throws Exception {
        // ==============================================================================
        // 1. Проверка конфигуратора Kerberos SASL GSSAPI
        // ==============================================================================
        KafkaKerberosConfig krbConfig = new KafkaKerberosConfig(true, "kafka-replicator@DC1.CORP", "/etc/security/keytabs/kafka.keytab");
        krbConfig.setServiceName("kafka");
        Properties kafkaProps = new Properties();
        KafkaSecurityConfigBuilder.applyKerberos(kafkaProps, krbConfig);
        assertEquals("SASL_PLAINTEXT", kafkaProps.getProperty("security.protocol"));
        assertEquals("GSSAPI", kafkaProps.getProperty("sasl.mechanism"));
        assertTrue(kafkaProps.getProperty("sasl.jaas.config").contains("Krb5LoginModule"));

        // ==============================================================================
        // 2. Инициализация SPNEGO HTTP клиентов Schema Registry
        // ==============================================================================
        SpnegoAuthenticator testAuthenticator = uri -> "Negotiate TEST_E2E_SPNEGO_TICKET";

        ConfluentRegistryClient confluentClient = new ConfluentRegistryClient(srBaseUrl, testAuthenticator);
        HwxRegistryClient hwxClient = new HwxRegistryClient(srBaseUrl, testAuthenticator);

        CachedSchemaRegistryResolver resolver = new CachedSchemaRegistryResolver(
                confluentClient, confluentClient, hwxClient, hwxClient
        );
        ZeroCopyWireTranscoder transcoder = new ZeroCopyWireTranscoder(resolver);

        // ==============================================================================
        // 3. Сценарий 1: Репликация топика со схемой (Hortonworks -> Confluent) через SPNEGO
        // ==============================================================================
        byte[] avroPayload = "{\"id\":\"ORD-998811\",\"amount\":4200.00}".getBytes(StandardCharsets.UTF_8);
        byte[] hwxRecordValue = new byte[9 + avroPayload.length];
        hwxRecordValue[0] = 0x02; // Hortonworks Protocol ID
        ByteBuffer.wrap(hwxRecordValue, 1, 8).putLong(1001L);
        System.arraycopy(avroPayload, 0, hwxRecordValue, 9, avroPayload.length);

        // Транскодинг заголовка полезной нагрузки
        byte[] confluentRecordValue = transcoder.transcode("orders-avro", hwxRecordValue,
                PayloadFormat.PAYLOAD_HWX_AVRO, PayloadFormat.PAYLOAD_CONFLUENT_AVRO);

        assertNotNull(confluentRecordValue);
        assertEquals(0x00, confluentRecordValue[0], "Magic Byte должен стать 0x00 (Confluent)");
        int assignedConfluentId = ByteBuffer.wrap(confluentRecordValue, 1, 4).getInt();
        assertEquals(42, assignedConfluentId, "Schema ID должен соответствовать ответу Confluent SR (42)");

        // Отправка батча в gRPC DataTransferService (DC1 -> DC2)
        KafkaRecord record1 = KafkaRecord.newBuilder()
                .setPartition(0)
                .setTimestamp(System.currentTimeMillis())
                .setKey(com.google.protobuf.ByteString.copyFromUtf8("ORD-998811"))
                .setPayload(com.google.protobuf.ByteString.copyFrom(confluentRecordValue))
                .addHeaders(KafkaRecordHeader.newBuilder().setKey("X-Repl-Origin").setValue(com.google.protobuf.ByteString.copyFromUtf8("DC1")).build())
                .build();

        KafkaBatchRequest batch1 = KafkaBatchRequest.newBuilder()
                .setBatchId("batch-orders-1")
                .setSourceDc("DC1")
                .setTargetDc("DC2")
                .setTopic("orders-avro")
                .setPayloadFormat(PayloadFormat.PAYLOAD_CONFLUENT_AVRO)
                .addRecords(record1)
                .build();

        KafkaBatchAck ack1 = replicationClient.sendBatchSync(batch1, 5);
        assertTrue(ack1.getSuccess());
        assertEquals(1, ack1.getCommittedRecords());

        // ==============================================================================
        // 4. Сценарий 2: Прямой проброс (Passthrough) без Schema Registry (Raw JSON)
        // ==============================================================================
        byte[] rawJsonPayload = "{\"metric\":\"cpu_load\",\"value\":78.5}".getBytes(StandardCharsets.UTF_8);
        PayloadFormatClassifier classifier = new PayloadFormatClassifier();
        assertEquals(PayloadFormat.PAYLOAD_PASSTHROUGH,
                classifier.classify(rawJsonPayload, PayloadFormat.PAYLOAD_PASSTHROUGH));
        assertEquals(PayloadFormat.PAYLOAD_PASSTHROUGH,
                classifier.classify(rawJsonPayload, PayloadFormat.PAYLOAD_AUTO_DETECT));

        KafkaRecord rawRecord = KafkaRecord.newBuilder()
                .setPartition(2)
                .setTimestamp(System.currentTimeMillis())
                .setKey(com.google.protobuf.ByteString.copyFromUtf8("host-101"))
                .setPayload(com.google.protobuf.ByteString.copyFrom(rawJsonPayload))
                .build();

        KafkaBatchRequest batchRaw = KafkaBatchRequest.newBuilder()
                .setBatchId("batch-metrics-1")
                .setSourceDc("DC1")
                .setTargetDc("DC2")
                .setTopic("metrics-raw")
                .setPayloadFormat(PayloadFormat.PAYLOAD_PASSTHROUGH)
                .addRecords(rawRecord)
                .build();

        KafkaBatchAck ackRaw = replicationClient.sendBatchSync(batchRaw, 5);
        assertTrue(ackRaw.getSuccess());

        // ==============================================================================
        // 5. Сценарий 3: Поддержка Tombstone (null payload) для compact-топиков
        // ==============================================================================
        KafkaRecord tombstoneRecord = KafkaRecord.newBuilder()
                .setPartition(0)
                .setTimestamp(System.currentTimeMillis())
                .setKey(com.google.protobuf.ByteString.copyFromUtf8("EXPIRED_USER"))
                .setIsTombstone(true)
                .build();

        KafkaBatchRequest batchTombstone = KafkaBatchRequest.newBuilder()
                .setBatchId("batch-compact-1")
                .setSourceDc("DC1")
                .setTargetDc("DC2")
                .setTopic("user-cache")
                .setPayloadFormat(PayloadFormat.PAYLOAD_PASSTHROUGH)
                .addRecords(tombstoneRecord)
                .build();

        KafkaBatchAck ackTombstone = replicationClient.sendBatchSync(batchTombstone, 5);
        assertTrue(ackTombstone.getSuccess());

        // Проверяем, что в целевой брокер записались все 3 сообщения, включая Tombstone
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockKafkaProducer, atLeast(3)).send(recordCaptor.capture());

        List<ProducerRecord<byte[], byte[]>> capturedRecords = recordCaptor.getAllValues();
        assertTrue(capturedRecords.stream().anyMatch(r -> r.value() == null), "Tombstone должен записаться как null value");

        // ==============================================================================
        // 6. Сценарий 4: Защита от бесконечных циклов (Loop Prevention Filter)
        // ==============================================================================
        LoopPreventionFilter loopFilter = new LoopPreventionFilter("DC2");
        RecordHeaders headersFromDc2 = new RecordHeaders();
        headersFromDc2.add("X-Repl-Origin", "DC2".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<byte[], byte[]> echoRecord = new ConsumerRecord<>("orders-avro", 0, 100, "k".getBytes(), "v".getBytes());
        headersFromDc2.forEach(h -> echoRecord.headers().add(h));

        assertFalse(loopFilter.shouldReplicate(echoRecord), "Запись из DC2 должна отбрасываться в DC2 во избежание цикла");

        // ==============================================================================
        // 7. Сценарий 5: Защитный гейт синхронизации оффсетов Consumer Groups
        // ==============================================================================
        // 7.1. Активная группа (STABLE) -> коммит смещения блокируется
        ConsumerGroupDescription stableGroupDesc = new ConsumerGroupDescription(
                "billing-group", false, Collections.emptyList(), "roundrobin", ConsumerGroupState.STABLE, null
        );
        DescribeConsumerGroupsResult describeStableResult = mock(DescribeConsumerGroupsResult.class);
        when(describeStableResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of("billing-group", stableGroupDesc)));
        when(mockAdminClient.describeConsumerGroups(List.of("billing-group"))).thenReturn(describeStableResult);

        OffsetSyncRequest stableRequest = OffsetSyncRequest.newBuilder()
                .setGroupId("billing-group")
                .setForceCommit(false)
                .addOffsets(TopicPartitionOffset.newBuilder().setTopic("orders-avro").setPartition(0).setTimestamp(System.currentTimeMillis()).build())
                .build();

        OffsetSyncResponse stableResp = safetyCommitManager.applyOffsetsSafely(stableRequest);
        assertTrue(stableResp.getBypassedActiveGroup(), "Коммит смещения для активной группы (STABLE) должен быть обойден (bypassed)");

        // 7.2. Резервная группа (EMPTY) -> коммит разрешен
        ConsumerGroupDescription emptyGroupDesc = new ConsumerGroupDescription(
                "standby-group", false, Collections.emptyList(), "roundrobin", ConsumerGroupState.EMPTY, null
        );
        DescribeConsumerGroupsResult describeEmptyResult = mock(DescribeConsumerGroupsResult.class);
        when(describeEmptyResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of("standby-group", emptyGroupDesc)));
        when(mockAdminClient.describeConsumerGroups(List.of("standby-group"))).thenReturn(describeEmptyResult);

        when(mockResolverConsumer.offsetsForTimes(any())).thenReturn(
                Map.of(new TopicPartition("orders-avro", 0), new OffsetAndTimestamp(500L, System.currentTimeMillis()))
        );

        OffsetSyncRequest emptyRequest = OffsetSyncRequest.newBuilder()
                .setGroupId("standby-group")
                .setForceCommit(false)
                .addOffsets(TopicPartitionOffset.newBuilder().setTopic("orders-avro").setPartition(0).setTimestamp(System.currentTimeMillis()).build())
                .build();

        OffsetSyncResponse emptyResp = safetyCommitManager.applyOffsetsSafely(emptyRequest);
        assertTrue(emptyResp.getSuccess(), "Коммит смещения для резервной группы (EMPTY) должен успешно выполниться");

        // ==============================================================================
        // 8. Проверка SPNEGO статистики HTTP-сервера
        // ==============================================================================
        assertTrue(hwxRequestsCount.get() > 0, "Hortonworks SR должен был получить HTTP-запросы со SPNEGO");
        assertTrue(confluentRequestsCount.get() > 0, "Confluent SR должен был получить HTTP-запросы со SPNEGO");
    }
}
