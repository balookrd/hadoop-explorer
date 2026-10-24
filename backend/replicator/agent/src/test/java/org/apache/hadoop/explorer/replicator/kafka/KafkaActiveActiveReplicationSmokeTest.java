package org.apache.hadoop.explorer.replicator.kafka;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.*;
import org.apache.hadoop.explorer.replicator.kafka.offset.SafetyCommitManager;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.apache.hadoop.explorer.replicator.kafka.service.KafkaDataTransferServiceImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Комплексный интеграционный Smoke-тест для двунаправленной (Active-Active) репликации Kafka через gRPC.
 * <p>
 * Проверяемые инварианты Active-Active топологии:
 * 1. Одновременная двусторонняя передача потоков данных (DC1 -> DC2 и DC2 -> DC1);
 * 2. Предотвращение паразитных циклов и бесконечного эха (LoopPreventionFilter);
 * 3. Обогащение служебных заголовков X-Repl-Origin и инкремент счетчика X-Repl-Hop;
 * 4. Защита от зацикливания при многоузловой топологии (Max Hops Barrier);
 * 5. Поддержка Tombstone (удаление по ключу) в обоих направлениях;
 * 6. Защитный гейт синхронизации смещений Consumer Groups в Active-Active режиме.
 */
@DisplayName("E2E Integration Smoke Test: Двунаправленная (Active-Active) репликация Kafka с защитой от циклов")
class KafkaActiveActiveReplicationSmokeTest {

    private Server serverDc1;
    private Server serverDc2;
    private ManagedChannel channelDc1;
    private ManagedChannel channelDc2;

    private KafkaProducer<byte[], byte[]> mockProducerDc1;
    private KafkaProducer<byte[], byte[]> mockProducerDc2;
    private AdminClient mockAdminDc1;
    private AdminClient mockAdminDc2;
    private KafkaConsumer<byte[], byte[]> mockConsumerDc1;
    private KafkaConsumer<byte[], byte[]> mockConsumerDc2;

    private KafkaReplicationClient clientDc1ToDc2;
    private KafkaReplicationClient clientDc2ToDc1;

    private LoopPreventionFilter loopFilterDc1;
    private LoopPreventionFilter loopFilterDc2;

    private final List<ProducerRecord<byte[], byte[]>> publishedRecordsDc1 = new CopyOnWriteArrayList<>();
    private final List<ProducerRecord<byte[], byte[]>> publishedRecordsDc2 = new CopyOnWriteArrayList<>();

    private static final String SERVER_DC1_NAME = "test-dc1-receiver-" + UUID.randomUUID();
    private static final String SERVER_DC2_NAME = "test-dc2-receiver-" + UUID.randomUUID();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        publishedRecordsDc1.clear();
        publishedRecordsDc2.clear();

        // 1. Мокирование инфраструктуры DC1
        mockProducerDc1 = mock(KafkaProducer.class);
        when(mockProducerDc1.send(any(), any())).thenAnswer(invocation -> {
            ProducerRecord<byte[], byte[]> rec = invocation.getArgument(0);
            publishedRecordsDc1.add(rec);
            org.apache.kafka.clients.producer.Callback cb = invocation.getArgument(1);
            if (cb != null) {
                cb.onCompletion(new RecordMetadata(new TopicPartition(rec.topic(), rec.partition() != null ? rec.partition() : 0),
                        0, 0, System.currentTimeMillis(), 0, 0), null);
            }
            return CompletableFuture.completedFuture(null);
        });

        mockAdminDc1 = mock(AdminClient.class);
        mockConsumerDc1 = mock(KafkaConsumer.class);
        SafetyCommitManager commitManagerDc1 = new SafetyCommitManager(mockAdminDc1, mockConsumerDc1);
        KafkaBatchProducer batchProducerDc1 = new KafkaBatchProducer(mockProducerDc1);
        KafkaDataTransferServiceImpl serviceDc1 = new KafkaDataTransferServiceImpl(batchProducerDc1, commitManagerDc1, "DC1-NODE1");

        serverDc1 = InProcessServerBuilder.forName(SERVER_DC1_NAME).directExecutor().addService(serviceDc1).build().start();
        channelDc1 = InProcessChannelBuilder.forName(SERVER_DC1_NAME).directExecutor().build();

        // 2. Мокирование инфраструктуры DC2
        mockProducerDc2 = mock(KafkaProducer.class);
        when(mockProducerDc2.send(any(), any())).thenAnswer(invocation -> {
            ProducerRecord<byte[], byte[]> rec = invocation.getArgument(0);
            publishedRecordsDc2.add(rec);
            org.apache.kafka.clients.producer.Callback cb = invocation.getArgument(1);
            if (cb != null) {
                cb.onCompletion(new RecordMetadata(new TopicPartition(rec.topic(), rec.partition() != null ? rec.partition() : 0),
                        0, 0, System.currentTimeMillis(), 0, 0), null);
            }
            return CompletableFuture.completedFuture(null);
        });

        mockAdminDc2 = mock(AdminClient.class);
        mockConsumerDc2 = mock(KafkaConsumer.class);
        SafetyCommitManager commitManagerDc2 = new SafetyCommitManager(mockAdminDc2, mockConsumerDc2);
        KafkaBatchProducer batchProducerDc2 = new KafkaBatchProducer(mockProducerDc2);
        KafkaDataTransferServiceImpl serviceDc2 = new KafkaDataTransferServiceImpl(batchProducerDc2, commitManagerDc2, "DC2-NODE1");

        serverDc2 = InProcessServerBuilder.forName(SERVER_DC2_NAME).directExecutor().addService(serviceDc2).build().start();
        channelDc2 = InProcessChannelBuilder.forName(SERVER_DC2_NAME).directExecutor().build();

        // 3. gRPC клиенты между ЦОД
        clientDc1ToDc2 = new KafkaReplicationClient(channelDc2);
        clientDc2ToDc1 = new KafkaReplicationClient(channelDc1);

        // 4. Фильтры предотвращения циклов (Active-Active)
        loopFilterDc1 = new LoopPreventionFilter("DC1", "DC2");
        loopFilterDc2 = new LoopPreventionFilter("DC2", "DC1");
    }

    @AfterEach
    void tearDown() {
        if (clientDc1ToDc2 != null) clientDc1ToDc2.close();
        if (clientDc2ToDc1 != null) clientDc2ToDc1.close();
        if (channelDc1 != null) channelDc1.shutdownNow();
        if (channelDc2 != null) channelDc2.shutdownNow();
        if (serverDc1 != null) serverDc1.shutdownNow();
        if (serverDc2 != null) serverDc2.shutdownNow();
    }

    @Test
    @DisplayName("Сквозная двунаправленная репликация DC1 <-> DC2 с гарантией отсутствия бесконечного эха")
    void testBidirectionalActiveActiveReplicationAndLoopSuppression() throws Exception {
        String topic = "orders-active";

        // =========================================================================
        // Шаг 1: Локальная публикация в DC1 и репликация в DC2
        // =========================================================================
        byte[] key1 = "ORD-DC1-001".getBytes(StandardCharsets.UTF_8);
        byte[] val1 = "{\"order\":\"ORD-DC1-001\",\"origin\":\"DC1\"}".getBytes(StandardCharsets.UTF_8);

        ConsumerRecord<byte[], byte[]> localRecordDc1 = new ConsumerRecord<>(
                topic, 0, 100L, key1, val1
        );

        // Локальное сообщение должно быть разрешено к репликации в DC1
        assertTrue(loopFilterDc1.shouldReplicate(localRecordDc1), "DC1 должен реплицировать локальное сообщение");

        // Обогащаем заголовки служебными метками
        List<KafkaRecordHeader> enrichedHeaders1 = loopFilterDc1.enrichHeaders(localRecordDc1.headers());
        KafkaRecord protoRec1 = KafkaRecord.newBuilder()
                .setPartition(0)
                .setOriginalOffset(100L)
                .setTimestamp(System.currentTimeMillis())
                .setKey(ByteString.copyFrom(key1))
                .setPayload(ByteString.copyFrom(val1))
                .addAllHeaders(enrichedHeaders1)
                .build();

        KafkaBatchRequest batch1 = KafkaBatchRequest.newBuilder()
                .setBatchId("batch-dc1-1")
                .setTopic(topic)
                .addRecords(protoRec1)
                .setSourceDc("DC1")
                .build();

        CompletableFuture<KafkaBatchAck> ackFuture1 = new CompletableFuture<>();
        StreamObserver<KafkaBatchRequest> stream1 = clientDc1ToDc2.openBatchStream(new StreamObserver<>() {
            @Override public void onNext(KafkaBatchAck ack) { ackFuture1.complete(ack); }
            @Override public void onError(Throwable t) { ackFuture1.completeExceptionally(t); }
            @Override public void onCompleted() {}
        });
        stream1.onNext(batch1);
        stream1.onCompleted();

        KafkaBatchAck ack1 = ackFuture1.get(5, TimeUnit.SECONDS);
        assertTrue(ack1.getSuccess(), "Батч из DC1 должен быть успешно зафиксирован в DC2");
        assertEquals(1, publishedRecordsDc2.size(), "В DC2 должна быть сохранена 1 запись");

        // Проверяем сохраненные заголовки в брокере DC2
        ProducerRecord<byte[], byte[]> savedInDc2 = publishedRecordsDc2.get(0);
        assertEquals("ORD-DC1-001", new String(savedInDc2.key(), StandardCharsets.UTF_8));
        Header originH1 = savedInDc2.headers().lastHeader(LoopPreventionFilter.HEADER_ORIGIN);
        Header hopH1 = savedInDc2.headers().lastHeader(LoopPreventionFilter.HEADER_HOP);
        assertNotNull(originH1, "Заголовок X-Repl-Origin должен присутствовать в DC2");
        assertEquals("DC1", new String(originH1.value(), StandardCharsets.UTF_8));
        assertEquals("1", new String(hopH1.value(), StandardCharsets.UTF_8));

        // =========================================================================
        // Шаг 2: Валидация защиты от эха в DC2 (Loop Prevention)
        // Воркер в DC2 считывает запись, прибывшую из DC1. Фильтр DC2 ОБЯЗАН ее отбросить!
        // =========================================================================
        ConsumerRecord<byte[], byte[]> readFromDc2 = new ConsumerRecord<>(
                topic, 0, 500L, 0L, null, 0, 0, savedInDc2.key(), savedInDc2.value(),
                savedInDc2.headers(), null
        );

        assertFalse(loopFilterDc2.shouldReplicate(readFromDc2),
                "КРИТИЧЕСКАЯ ПРОВЕРКА: LoopPreventionFilter в DC2 обязан отбросить эхо-запись с Origin=DC1 при targetDc=DC1!");

        // =========================================================================
        // Шаг 3: Локальная публикация в DC2 и симметричная репликация в DC1
        // =========================================================================
        byte[] key2 = "ORD-DC2-999".getBytes(StandardCharsets.UTF_8);
        byte[] val2 = "{\"order\":\"ORD-DC2-999\",\"origin\":\"DC2\"}".getBytes(StandardCharsets.UTF_8);

        ConsumerRecord<byte[], byte[]> localRecordDc2 = new ConsumerRecord<>(
                topic, 0, 200L, key2, val2
        );

        assertTrue(loopFilterDc2.shouldReplicate(localRecordDc2), "DC2 должен реплицировать локальное сообщение");

        List<KafkaRecordHeader> enrichedHeaders2 = loopFilterDc2.enrichHeaders(localRecordDc2.headers());
        KafkaRecord protoRec2 = KafkaRecord.newBuilder()
                .setPartition(0)
                .setOriginalOffset(200L)
                .setTimestamp(System.currentTimeMillis())
                .setKey(ByteString.copyFrom(key2))
                .setPayload(ByteString.copyFrom(val2))
                .addAllHeaders(enrichedHeaders2)
                .build();

        KafkaBatchRequest batch2 = KafkaBatchRequest.newBuilder()
                .setBatchId("batch-dc2-1")
                .setTopic(topic)
                .addRecords(protoRec2)
                .setSourceDc("DC2")
                .build();

        CompletableFuture<KafkaBatchAck> ackFuture2 = new CompletableFuture<>();
        StreamObserver<KafkaBatchRequest> stream2 = clientDc2ToDc1.openBatchStream(new StreamObserver<>() {
            @Override public void onNext(KafkaBatchAck ack) { ackFuture2.complete(ack); }
            @Override public void onError(Throwable t) { ackFuture2.completeExceptionally(t); }
            @Override public void onCompleted() {}
        });
        stream2.onNext(batch2);
        stream2.onCompleted();

        KafkaBatchAck ack2 = ackFuture2.get(5, TimeUnit.SECONDS);
        assertTrue(ack2.getSuccess(), "Батч из DC2 должен быть успешно зафиксирован в DC1");
        assertEquals(1, publishedRecordsDc1.size(), "В DC1 должна быть сохранена 1 запись");

        ProducerRecord<byte[], byte[]> savedInDc1 = publishedRecordsDc1.get(0);
        assertEquals("ORD-DC2-999", new String(savedInDc1.key(), StandardCharsets.UTF_8));
        Header originH2 = savedInDc1.headers().lastHeader(LoopPreventionFilter.HEADER_ORIGIN);
        Header hopH2 = savedInDc1.headers().lastHeader(LoopPreventionFilter.HEADER_HOP);
        assertNotNull(originH2, "Заголовок X-Repl-Origin должен присутствовать в DC1");
        assertEquals("DC2", new String(originH2.value(), StandardCharsets.UTF_8));
        assertEquals("1", new String(hopH2.value(), StandardCharsets.UTF_8));

        // =========================================================================
        // Шаг 4: Валидация защиты от эха в DC1
        // Воркер в DC1 считывает запись, прибывшую из DC2. Фильтр DC1 ОБЯЗАН ее отбросить!
        // =========================================================================
        ConsumerRecord<byte[], byte[]> readFromDc1 = new ConsumerRecord<>(
                topic, 0, 600L, 0L, null, 0, 0, savedInDc1.key(), savedInDc1.value(),
                savedInDc1.headers(), null
        );

        assertFalse(loopFilterDc1.shouldReplicate(readFromDc1),
                "КРИТИЧЕСКАЯ ПРОВЕРКА: LoopPreventionFilter в DC1 обязан отбросить эхо-запись с Origin=DC2 при targetDc=DC2!");

        // Итоговая верификация: точные счетчики без дублирования
        assertEquals(1, publishedRecordsDc1.size(), "В DC1 не должно возникнуть лишних записей");
        assertEquals(1, publishedRecordsDc2.size(), "В DC2 не должно возникнуть лишних записей");
    }

    @Test
    @DisplayName("Параллельная двусторонняя передача пакетов с Tombstone-сообщениями в обоих направлениях")
    void testConcurrentBidirectionalStreamingWithTombstones() throws Exception {
        String topic = "users-cache";
        int messagesPerDc = 30;

        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger successCounter = new AtomicInteger(0);

        // Поток 1: DC1 -> DC2
        CompletableFuture.runAsync(() -> {
            try {
                KafkaBatchRequest.Builder batchBuilder = KafkaBatchRequest.newBuilder()
                        .setBatchId("batch-concurrent-dc1")
                        .setTopic(topic)
                        .setSourceDc("DC1");

                for (int i = 0; i < messagesPerDc; i++) {
                    boolean isTombstone = (i % 5 == 0); // Каждое 5-е сообщение — Tombstone
                    byte[] key = ("user-dc1-" + i).getBytes(StandardCharsets.UTF_8);
                    byte[] payload = isTombstone ? null : ("data-dc1-" + i).getBytes(StandardCharsets.UTF_8);

                    RecordHeaders headers = new RecordHeaders();
                    ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
                            topic, 0, i, 0L, null, 0, 0, key, payload, headers, null
                    );

                    KafkaRecord.Builder rb = KafkaRecord.newBuilder()
                            .setPartition(0)
                            .setOriginalOffset(i)
                            .setKey(ByteString.copyFrom(key))
                            .setIsTombstone(isTombstone)
                            .addAllHeaders(loopFilterDc1.enrichHeaders(cr.headers()));

                    if (!isTombstone) {
                        rb.setPayload(ByteString.copyFrom(payload));
                    }
                    batchBuilder.addRecords(rb.build());
                }

                CompletableFuture<KafkaBatchAck> ackFut = new CompletableFuture<>();
                StreamObserver<KafkaBatchRequest> stream = clientDc1ToDc2.openBatchStream(new StreamObserver<>() {
                    @Override public void onNext(KafkaBatchAck ack) { ackFut.complete(ack); }
                    @Override public void onError(Throwable t) { ackFut.completeExceptionally(t); }
                    @Override public void onCompleted() {}
                });
                stream.onNext(batchBuilder.build());
                stream.onCompleted();

                KafkaBatchAck ack = ackFut.get(5, TimeUnit.SECONDS);
                if (ack.getSuccess() && ack.getCommittedRecords() == messagesPerDc) {
                    successCounter.incrementAndGet();
                }
            } catch (Exception e) {
                fail("Ошибка в потоке DC1 -> DC2: " + e.getMessage());
            } finally {
                latch.countDown();
            }
        });

        // Поток 2: DC2 -> DC1
        CompletableFuture.runAsync(() -> {
            try {
                KafkaBatchRequest.Builder batchBuilder = KafkaBatchRequest.newBuilder()
                        .setBatchId("batch-concurrent-dc2")
                        .setTopic(topic)
                        .setSourceDc("DC2");

                for (int i = 0; i < messagesPerDc; i++) {
                    boolean isTombstone = (i % 5 == 0);
                    byte[] key = ("user-dc2-" + i).getBytes(StandardCharsets.UTF_8);
                    byte[] payload = isTombstone ? null : ("data-dc2-" + i).getBytes(StandardCharsets.UTF_8);

                    RecordHeaders headers = new RecordHeaders();
                    ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
                            topic, 0, i, 0L, null, 0, 0, key, payload, headers, null
                    );

                    KafkaRecord.Builder rb = KafkaRecord.newBuilder()
                            .setPartition(0)
                            .setOriginalOffset(i)
                            .setKey(ByteString.copyFrom(key))
                            .setIsTombstone(isTombstone)
                            .addAllHeaders(loopFilterDc2.enrichHeaders(cr.headers()));

                    if (!isTombstone) {
                        rb.setPayload(ByteString.copyFrom(payload));
                    }
                    batchBuilder.addRecords(rb.build());
                }

                CompletableFuture<KafkaBatchAck> ackFut = new CompletableFuture<>();
                StreamObserver<KafkaBatchRequest> stream = clientDc2ToDc1.openBatchStream(new StreamObserver<>() {
                    @Override public void onNext(KafkaBatchAck ack) { ackFut.complete(ack); }
                    @Override public void onError(Throwable t) { ackFut.completeExceptionally(t); }
                    @Override public void onCompleted() {}
                });
                stream.onNext(batchBuilder.build());
                stream.onCompleted();

                KafkaBatchAck ack = ackFut.get(5, TimeUnit.SECONDS);
                if (ack.getSuccess() && ack.getCommittedRecords() == messagesPerDc) {
                    successCounter.incrementAndGet();
                }
            } catch (Exception e) {
                fail("Ошибка в потоке DC2 -> DC1: " + e.getMessage());
            } finally {
                latch.countDown();
            }
        });

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Таймаут выполнения параллельной двусторонней репликации");
        assertEquals(2, successCounter.get(), "Оба параллельных направления должны успешно зафиксировать все батчи");

        // Верификация Tombstones в DC2
        assertEquals(messagesPerDc, publishedRecordsDc2.size());
        long tombstoneCountDc2 = publishedRecordsDc2.stream().filter(r -> r.value() == null).count();
        assertEquals(6, tombstoneCountDc2, "В DC2 должно быть доставлено 6 Tombstone записей");

        // Верификация Tombstones в DC1
        assertEquals(messagesPerDc, publishedRecordsDc1.size());
        long tombstoneCountDc1 = publishedRecordsDc1.stream().filter(r -> r.value() == null).count();
        assertEquals(6, tombstoneCountDc1, "В DC1 должно быть доставлено 6 Tombstone записей");

        // Верификация того, что ВСЕ принятые записи блокируются встречным LoopPreventionFilter (0% эхо)
        for (ProducerRecord<byte[], byte[]> r : publishedRecordsDc2) {
            ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
                    r.topic(), 0, 0L, 0L, null, 0, 0, r.key(), r.value(),
                    r.headers(), null
            );
            assertFalse(loopFilterDc2.shouldReplicate(cr), "Запись из DC1 не должна реплицироваться обратно из DC2");
        }

        for (ProducerRecord<byte[], byte[]> r : publishedRecordsDc1) {
            ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
                    r.topic(), 0, 0L, 0L, null, 0, 0, r.key(), r.value(),
                    r.headers(), null
            );
            assertFalse(loopFilterDc1.shouldReplicate(cr), "Запись из DC2 не должна реплицироваться обратно из DC1");
        }
    }

    @Test
    @DisplayName("Отсечение сообщений при превышении предела прыжков (Max Hops Protection)")
    void testDropMessagesExceedingMaxHopsInMultiDcTopology() {
        LoopPreventionFilter boundedFilter = new LoopPreventionFilter("DC1", "DC2", 2);

        // 1-й прыжок (норма)
        RecordHeaders h1 = new RecordHeaders();
        h1.add(LoopPreventionFilter.HEADER_ORIGIN, "DC3".getBytes(StandardCharsets.UTF_8));
        h1.add(LoopPreventionFilter.HEADER_HOP, "1".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<byte[], byte[]> r1 = new ConsumerRecord<>("topic", 0, 1L, 0L, null, 0, 0, (byte[]) null, "val".getBytes(), h1, null);
        assertTrue(boundedFilter.shouldReplicate(r1), "Сообщение с 1-м прыжком должно быть допущено");

        // 2-й прыжок (предел достигнут: hop >= maxHops)
        RecordHeaders h2 = new RecordHeaders();
        h2.add(LoopPreventionFilter.HEADER_ORIGIN, "DC3".getBytes(StandardCharsets.UTF_8));
        h2.add(LoopPreventionFilter.HEADER_HOP, "2".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<byte[], byte[]> r2 = new ConsumerRecord<>("topic", 0, 2L, 0L, null, 0, 0, (byte[]) null, "val".getBytes(), h2, null);
        assertFalse(boundedFilter.shouldReplicate(r2), "Сообщение с hop >= maxHops должно быть отброшено для защиты от циклов");
    }

    @Test
    @DisplayName("Защитный гейт синхронизации смещений Consumer Groups в Active-Active топологии")
    void testActiveActiveConsumerGroupSafetyGate() {
        String activeGroup = "order-processor-group";

        // Симулируем в DC2 состояние STABLE (локальные консьюмеры работают на целевом кластере)
        ConsumerGroupDescription activeDesc = new ConsumerGroupDescription(
                activeGroup, true, Collections.emptyList(), "range", ConsumerGroupState.STABLE, null
        );
        DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
        KafkaFuture<ConsumerGroupDescription> future = KafkaFuture.completedFuture(activeDesc);
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(activeGroup, activeDesc)));
        when(mockAdminDc2.describeConsumerGroups(any())).thenReturn(describeResult);

        SafetyCommitManager commitManagerDc2 = new SafetyCommitManager(mockAdminDc2, mockConsumerDc2);

        // Проверяем статус группы напрямую
        assertTrue(commitManagerDc2.isGroupActiveInTarget(activeGroup), "Группа в состоянии STABLE должна определяться как активная");

        // Попытка синхронизации смещений через applyOffsetsSafely должна активировать защитный гейт (bypassedActiveGroup=true)
        OffsetSyncRequest request = OffsetSyncRequest.newBuilder()
                .setGroupId(activeGroup)
                .setForceCommit(false)
                .build();

        OffsetSyncResponse response = commitManagerDc2.applyOffsetsSafely(request);
        assertTrue(response.getSuccess());
        assertTrue(response.getBypassedActiveGroup(),
                "Защитный гейт обязан пропустить коммит (bypassedActiveGroup=true) для активной группы (STABLE) в Active-Active режиме!");
    }
}
