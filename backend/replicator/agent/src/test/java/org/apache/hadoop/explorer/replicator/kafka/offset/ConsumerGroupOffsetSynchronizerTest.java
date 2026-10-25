package org.apache.hadoop.explorer.replicator.kafka.offset;

import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncResponse;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Перенос смещений консьюмер-групп на целевой кластер.
 *
 * <p>Принимающая половина механизма была готова: RPC {@code SyncConsumerOffsets} реализован в
 * приемнике и применяет смещения через {@code SafetyCommitManager} с проверкой состояния группы и
 * отставания. Не хватало отправляющей: сканер {@code ConsumerGroupOffsetScanner} в рабочем коде
 * не создавался, клиентский вызов {@code syncOffsets} не вызывался ниоткуда, а параметр задачи
 * {@code offset_sync_interval_sec} принимался и хранился, ничего не запуская. Из-за этого после
 * переключения на целевой ЦОД группы начинали чтение с {@code auto.offset.reset}, а не с
 * перенесенных смещений.
 */
@DisplayName("Kafka: перенос смещений консьюмер-групп")
class ConsumerGroupOffsetSynchronizerTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);

    private ConsumerGroupOffsetScanner scanner;
    private KafkaReplicationClient client;
    private ConsumerGroupOffsetSynchronizer synchronizer;

    private final ConsumerGroupOffsetSynchronizer.ReplicationProgress progress =
            new ConsumerGroupOffsetSynchronizer.ReplicationProgress() {
                @Override
                public Map<TopicPartition, Long> replicatedOffsets() {
                    return Map.of(P0, 500L);
                }

                @Override
                public Map<TopicPartition, Long> lastTimestamps() {
                    return Map.of(P0, 1_700_000_000_000L);
                }
            };

    @BeforeEach
    void setUp() {
        scanner = mock(ConsumerGroupOffsetScanner.class);
        client = mock(KafkaReplicationClient.class);
        when(client.syncOffsets(any(), anyLong()))
                .thenReturn(OffsetSyncResponse.newBuilder().setSuccess(true).build());
    }

    @AfterEach
    void tearDown() {
        if (synchronizer != null) {
            synchronizer.close();
        }
    }

    private ConsumerGroupOffsetSynchronizer create(List<String> groups) {
        synchronizer = new ConsumerGroupOffsetSynchronizer(
                scanner, client, () -> groups, () -> Set.of("orders"), progress, 10);
        return synchronizer;
    }

    private static OffsetSyncRequest request(String group) {
        return OffsetSyncRequest.newBuilder()
                .setGroupId(group)
                .setSourceDc("DC1")
                .setTargetDc("DC2")
                .build();
    }

    @Test
    @DisplayName("Найденные смещения отправляются на целевой кластер")
    void testScannedOffsetsAreSent() {
        when(scanner.scanOffsetsForGroups(any(), any(), any(), any(), any(), any(), anyBoolean()))
                .thenReturn(List.of(request("analytics-etl"), request("billing")));

        var result = create(List.of("analytics-etl", "billing")).syncOnce();

        assertEquals(2, result.sent(),
                "Смещения обеих групп обязаны уйти на целевой кластер: иначе после переключения "
                        + "группы начнут чтение с auto.offset.reset");
        assertEquals(0, result.failed());
        verify(client, times(2)).syncOffsets(any(), anyLong());
    }

    @Test
    @DisplayName("Сканеру передается фактический прогресс репликации")
    void testScannerReceivesReplicationProgress() {
        when(scanner.scanOffsetsForGroups(any(), any(), any(), any(), any(), any(), anyBoolean()))
                .thenReturn(List.of());

        create(List.of("analytics-etl")).syncOnce();

        verify(scanner).scanOffsetsForGroups(
                argThat(groups -> groups.contains("analytics-etl")),
                argThat(topics -> topics.contains("orders")),
                argThat(timestamps -> timestamps.containsKey(P0)),
                argThat(offsets -> Long.valueOf(500L).equals(offsets.get(P0))),
                any(), any(), eq(false));
    }

    @Test
    @DisplayName("Сбой одной группы не мешает остальным")
    void testFailureOfOneGroupDoesNotStopOthers() {
        when(scanner.scanOffsetsForGroups(any(), any(), any(), any(), any(), any(), anyBoolean()))
                .thenReturn(List.of(request("broken"), request("healthy")));
        when(client.syncOffsets(argThat(r -> r != null && "broken".equals(r.getGroupId())), anyLong()))
                .thenThrow(new RuntimeException("target unreachable"));

        var result = assertDoesNotThrow(() -> create(List.of("broken", "healthy")).syncOnce(),
                "Перенос смещений не имеет права ронять демон репликации");

        assertEquals(1, result.sent(), "Исправная группа обязана быть обработана");
        assertEquals(1, result.failed(), "Сбойная группа обязана попасть в счетчик ошибок");
    }

    @Test
    @DisplayName("Ответ приемника с success=false считается неуспехом")
    void testRejectedSyncIsCountedAsFailure() {
        when(scanner.scanOffsetsForGroups(any(), any(), any(), any(), any(), any(), anyBoolean()))
                .thenReturn(List.of(request("lagging")));
        when(client.syncOffsets(any(), anyLong()))
                .thenReturn(OffsetSyncResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("группа активна на целевом кластере")
                        .build());

        var result = create(List.of("lagging")).syncOnce();

        assertEquals(0, result.sent());
        assertEquals(1, result.failed(),
                "Отказ защитного гейта приемника нельзя выдавать за успешный перенос");
    }

    @Test
    @DisplayName("Без групп перенос не обращается ни к сканеру, ни к приемнику")
    void testNoGroupsNoCalls() {
        var result = create(List.of()).syncOnce();

        assertEquals(0, result.sent());
        verifyNoInteractions(client);
        verify(scanner, never()).scanOffsetsForGroups(any(), any(), any(), any(), any(), any(), anyBoolean());
    }

    @Test
    @DisplayName("Периодический запуск повторяет проход по интервалу")
    void testPeriodicRunRepeats() throws Exception {
        AtomicInteger scans = new AtomicInteger();
        when(scanner.scanOffsetsForGroups(any(), any(), any(), any(), any(), any(), anyBoolean()))
                .thenAnswer(inv -> {
                    scans.incrementAndGet();
                    return List.of(request("analytics-etl"));
                });

        create(List.of("analytics-etl")).start(1);

        long deadline = System.currentTimeMillis() + 10_000;
        while (scans.get() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(scans.get() >= 2,
                "Перенос обязан повторяться по интервалу, проходов: " + scans.get());
    }
}
