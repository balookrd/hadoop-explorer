package org.apache.hadoop.explorer.replicator.kafka.offset;

import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.TopicPartitionOffset;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("Юнит-тесты: ConsumerGroupOffsetScanner с защитой от перемотки отстающих групп (P0-3)")
class ConsumerGroupOffsetScannerTest {

    private AdminClient mockAdminClient;
    private ConsumerGroupOffsetScanner scanner;

    private static final String SOURCE_DC = "DC1";
    private static final String TARGET_DC = "DC2";
    private static final String GROUP_ID = "orders-consumer-group";
    private static final String TOPIC = "orders";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);

    @BeforeEach
    void setUp() {
        mockAdminClient = mock(AdminClient.class);
        scanner = new ConsumerGroupOffsetScanner(mockAdminClient, SOURCE_DC, TARGET_DC);
    }

    @Test
    @DisplayName("P0-3: Если группа отстает (lag > 0), timestamp конца топика НЕ подставляется (timestamp = 0)")
    void shouldNotSubstituteLatestTimestampWhenGroupIsLagging() {
        // Оффсет группы = 100
        Map<TopicPartition, OffsetAndMetadata> groupOffsets = Map.of(
                TP0, new OffsetAndMetadata(100L)
        );
        ListConsumerGroupOffsetsResult listGroupResult = mock(ListConsumerGroupOffsetsResult.class);
        when(listGroupResult.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(groupOffsets));
        when(mockAdminClient.listConsumerGroupOffsets(GROUP_ID)).thenReturn(listGroupResult);

        // Последний реплицированный оффсет = 100000 (лаг 99900 сообщений)
        Map<TopicPartition, Long> lastReplicatedOffsets = Map.of(TP0, 100000L);
        Map<TopicPartition, Long> lastTimestamps = Map.of(TP0, 1700000000000L);

        Optional<OffsetSyncRequest> reqOpt = scanner.scanOffsets(
                GROUP_ID,
                Set.of(TOPIC),
                lastTimestamps,
                lastReplicatedOffsets,
                null,
                null,
                false
        );

        assertTrue(reqOpt.isPresent());
        OffsetSyncRequest request = reqOpt.get();
        assertEquals(GROUP_ID, request.getGroupId());
        assertEquals(1, request.getOffsetsCount());

        TopicPartitionOffset tpo = request.getOffsets(0);
        assertEquals(TOPIC, tpo.getTopic());
        assertEquals(0, tpo.getPartition());
        assertEquals(100L, tpo.getSourceOffset());
        // КРИТИЧЕСКАЯ ПРОВЕРКА P0-3: timestamp ДОЛЖЕН быть 0, чтобы не перемотать группу на оффсет 100000!
        assertEquals(0L, tpo.getTimestamp(), "Timestamp отстающей группы не должен интерполироваться временем конца топика!");
    }

    @Test
    @DisplayName("Если группа догнала конец топика (lag == 0), timestamp конца топика применяется корректно")
    void shouldApplyLatestTimestampWhenGroupCaughtUp() {
        // Оффсет группы = 500
        Map<TopicPartition, OffsetAndMetadata> groupOffsets = Map.of(
                TP0, new OffsetAndMetadata(500L)
        );
        ListConsumerGroupOffsetsResult listGroupResult = mock(ListConsumerGroupOffsetsResult.class);
        when(listGroupResult.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(groupOffsets));
        when(mockAdminClient.listConsumerGroupOffsets(GROUP_ID)).thenReturn(listGroupResult);

        // Последний реплицированный оффсет = 500 (группа вычитала все записи)
        Map<TopicPartition, Long> lastReplicatedOffsets = Map.of(TP0, 500L);
        long expectedTimestamp = 1700000050000L;
        Map<TopicPartition, Long> lastTimestamps = Map.of(TP0, expectedTimestamp);

        Optional<OffsetSyncRequest> reqOpt = scanner.scanOffsets(
                GROUP_ID,
                Set.of(TOPIC),
                lastTimestamps,
                lastReplicatedOffsets,
                null,
                null,
                false
        );

        assertTrue(reqOpt.isPresent());
        OffsetSyncRequest request = reqOpt.get();
        assertEquals(1, request.getOffsetsCount());

        TopicPartitionOffset tpo = request.getOffsets(0);
        assertEquals(500L, tpo.getSourceOffset());
        assertEquals(expectedTimestamp, tpo.getTimestamp(), "Timestamp должен совпадать с последним временем конца топика");
    }

    @Test
    @DisplayName("Если доступна карта точных смещений (exactOffsetTimestamps), используется точный timestamp оффсета")
    void shouldUseExactOffsetTimestampWhenAvailable() {
        Map<TopicPartition, OffsetAndMetadata> groupOffsets = Map.of(
                TP0, new OffsetAndMetadata(250L)
        );
        ListConsumerGroupOffsetsResult listGroupResult = mock(ListConsumerGroupOffsetsResult.class);
        when(listGroupResult.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(groupOffsets));
        when(mockAdminClient.listConsumerGroupOffsets(GROUP_ID)).thenReturn(listGroupResult);

        long exactTsFor250 = 1699999999000L;
        Map<TopicPartition, Map<Long, Long>> exactMap = Map.of(
                TP0, Map.of(250L, exactTsFor250)
        );
        Map<TopicPartition, Long> lastReplicatedOffsets = Map.of(TP0, 1000L);
        Map<TopicPartition, Long> lastTimestamps = Map.of(TP0, 1700000000000L);

        Optional<OffsetSyncRequest> reqOpt = scanner.scanOffsets(
                GROUP_ID,
                Set.of(TOPIC),
                lastTimestamps,
                lastReplicatedOffsets,
                exactMap,
                null,
                false
        );

        assertTrue(reqOpt.isPresent());
        TopicPartitionOffset tpo = reqOpt.get().getOffsets(0);
        assertEquals(250L, tpo.getSourceOffset());
        assertEquals(exactTsFor250, tpo.getTimestamp(), "Должен использоваться точный timestamp оффсета 250");
    }

    @Test
    @DisplayName("Корректная обработка целевого оффсета 0 (начало топика) через directTargetOffsets")
    void shouldSupportTargetOffsetZero() {
        Map<TopicPartition, OffsetAndMetadata> groupOffsets = Map.of(
                TP0, new OffsetAndMetadata(0L)
        );
        ListConsumerGroupOffsetsResult listGroupResult = mock(ListConsumerGroupOffsetsResult.class);
        when(listGroupResult.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(groupOffsets));
        when(mockAdminClient.listConsumerGroupOffsets(GROUP_ID)).thenReturn(listGroupResult);

        Map<TopicPartition, Long> directTargetOffsets = Map.of(TP0, 0L);

        Optional<OffsetSyncRequest> reqOpt = scanner.scanOffsets(
                GROUP_ID,
                Set.of(TOPIC),
                null,
                null,
                null,
                directTargetOffsets,
                true
        );

        assertTrue(reqOpt.isPresent());
        OffsetSyncRequest request = reqOpt.get();
        assertTrue(request.getForceCommit());

        TopicPartitionOffset tpo = request.getOffsets(0);
        assertEquals(0L, tpo.getSourceOffset());
        assertTrue(tpo.hasTargetOffset(), "Поле target_offset должно присутствовать в protobuf сообщении");
        assertEquals(0L, tpo.getTargetOffset(), "Целевой оффсет 0 должен сохраняться без искажений");
    }

    @Test
    @DisplayName("Автоматическое определение endOffsets через AdminClient при отсутствии lastReplicatedOffsets")
    void shouldQueryAdminClientWhenLastReplicatedOffsetsIsNull() {
        Map<TopicPartition, OffsetAndMetadata> groupOffsets = Map.of(
                TP0, new OffsetAndMetadata(150L)
        );
        ListConsumerGroupOffsetsResult listGroupResult = mock(ListConsumerGroupOffsetsResult.class);
        when(listGroupResult.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(groupOffsets));
        when(mockAdminClient.listConsumerGroupOffsets(GROUP_ID)).thenReturn(listGroupResult);

        // Имитируем ответ AdminClient.listOffsets: конец топика = 300L
        ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        ListOffsetsResult.ListOffsetsResultInfo info = new ListOffsetsResult.ListOffsetsResultInfo(300L, System.currentTimeMillis(), Optional.empty());
        when(listOffsetsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(TP0, info)));
        when(mockAdminClient.listOffsets(any())).thenReturn(listOffsetsResult);

        Map<TopicPartition, Long> lastTimestamps = Map.of(TP0, 1700000000000L);

        // Вызываем метод обратной совместимости (lastReplicatedOffsets == null)
        Optional<OffsetSyncRequest> reqOpt = scanner.scanOffsets(
                GROUP_ID,
                null,
                lastTimestamps,
                false
        );

        assertTrue(reqOpt.isPresent());
        TopicPartitionOffset tpo = reqOpt.get().getOffsets(0);
        assertEquals(150L, tpo.getSourceOffset());
        // Поскольку 150 < 300, сканер определил lag и не подставил lastTimestamp
        assertEquals(0L, tpo.getTimestamp(), "Автоматически обнаружен lag через AdminClient: timestamp должен быть 0");
    }

    @Test
    @DisplayName("Исключение служебных/инфраструктурных консьюмер-групп из сканирования (Blacklist)")
    void shouldSkipBlacklistedConsumerGroups() {
        List<String> blacklistedGroups = List.of(
                "kafka-replicator-dc1",
                "kafka-replicator-time-resolver-dc1",
                "replicator-agent-01",
                "hadoop-replicator-job-99",
                "__consumer_offsets",
                "schema-registry-dc1",
                "connect-cluster-01",
                "console-consumer-94812",
                "kafka-exporter-core"
        );

        for (String blacklisted : blacklistedGroups) {
            Optional<OffsetSyncRequest> req = scanner.scanOffsets(
                    blacklisted,
                    Set.of(TOPIC),
                    Map.of(TP0, 1000L),
                    false
            );
            assertTrue(req.isEmpty(), "Группа '" + blacklisted + "' должна быть исключена из сканирования!");
        }

        // Проверяем, что к mockAdminClient для blacklisted групп не было обращений
        verify(mockAdminClient, never()).listConsumerGroupOffsets(anyString());
    }

    @Test
    @DisplayName("Пакетное сканирование групп scanOffsetsForGroups фильтрует группы черного списка")
    void shouldFilterBlacklistedGroupsInBatchScan() {
        Map<TopicPartition, OffsetAndMetadata> groupOffsets = Map.of(
                TP0, new OffsetAndMetadata(500L)
        );
        ListConsumerGroupOffsetsResult listGroupResult = mock(ListConsumerGroupOffsetsResult.class);
        when(listGroupResult.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(groupOffsets));
        when(mockAdminClient.listConsumerGroupOffsets("allowed-orders-group")).thenReturn(listGroupResult);

        List<OffsetSyncRequest> requests = scanner.scanOffsetsForGroups(
                List.of("kafka-replicator-dc1", "allowed-orders-group", "__consumer_offsets", "schema-registry-dc2"),
                Set.of(TOPIC),
                Map.of(TP0, 1000L),
                Map.of(TP0, 500L),
                null,
                null,
                false
        );

        assertEquals(1, requests.size(), "Должна быть обработана ровно одна разрешенная группа");
        assertEquals("allowed-orders-group", requests.get(0).getGroupId());
    }
}
