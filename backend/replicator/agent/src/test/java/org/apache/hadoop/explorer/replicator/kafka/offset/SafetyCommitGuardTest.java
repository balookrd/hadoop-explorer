package org.apache.hadoop.explorer.replicator.kafka.offset;

import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncResponse;
import org.apache.hadoop.explorer.replicator.kafka.generated.TopicPartitionOffset;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Перенос смещений консьюмер-группы в целевой кластер.
 *
 * <p>Проверка «жива ли группа в целевом ДЦ» отвечала «не жива» на любую ошибку — недоступный
 * брокер, таймаут, отказ по правам. Коммит шел дальше и перезаписывал смещения группы, которая
 * в этот момент читала данные: консьюмеры целевого ДЦ откатывались назад или перепрыгивали
 * вперед. Отдельно: смещения двигались в любую сторону, то есть перенос мог сдвинуть группу
 * назад и заставить перечитать уже обработанное.
 */
@DisplayName("Kafka: перенос смещений не перезаписывает живую группу")
class SafetyCommitGuardTest {

    private static final String GROUP = "analytics-consumers";
    private static final String TOPIC = "sales-events";

    private AdminClient admin;
    private SafetyCommitManager manager;

    @BeforeEach
    void setUp() {
        admin = mock(AdminClient.class);
        manager = new SafetyCommitManager(admin, mock(KafkaConsumer.class));
    }

    private void describeFails(Throwable cause) {
        DescribeConsumerGroupsResult result = mock(DescribeConsumerGroupsResult.class);
        KafkaFutureImpl<Map<String, org.apache.kafka.clients.admin.ConsumerGroupDescription>> future = new KafkaFutureImpl<>();
        future.completeExceptionally(cause);
        when(result.all()).thenReturn(future);
        when(admin.describeConsumerGroups(any())).thenReturn(result);
    }

    private void currentCommitted(Map<TopicPartition, OffsetAndMetadata> offsets) {
        ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
        when(result.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(offsets));
        when(admin.listConsumerGroupOffsets(anyString())).thenReturn(result);
    }

    private AlterConsumerGroupOffsetsResult acceptCommit() {
        AlterConsumerGroupOffsetsResult result = mock(AlterConsumerGroupOffsetsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterConsumerGroupOffsets(anyString(), any())).thenReturn(result);
        return result;
    }

    private OffsetSyncRequest request(int partition, long targetOffset) {
        return OffsetSyncRequest.newBuilder()
                .setGroupId(GROUP)
                .addOffsets(TopicPartitionOffset.newBuilder()
                        .setTopic(TOPIC)
                        .setPartition(partition)
                        .setSourceOffset(targetOffset)
                        .setTargetOffset(targetOffset)
                        .build())
                .build();
    }

    @Test
    @DisplayName("Неизвестное состояние группы останавливает перенос")
    void unknownGroupStateShouldStopTheCommit() {
        describeFails(new TimeoutException("брокер целевого ДЦ не ответил"));
        acceptCommit();

        OffsetSyncResponse response = manager.applyOffsetsSafely(request(0, 500));

        assertFalse(response.getSuccess(),
                "Пока неизвестно, читает ли кто-то группу, переносить смещения нельзя");
        verify(admin, never()).alterConsumerGroupOffsets(anyString(), any());
    }

    @Test
    @DisplayName("Отсутствующая группа переносу не мешает")
    void missingGroupShouldAllowTheCommit() {
        describeFails(new GroupIdNotFoundException("группы еще нет"));
        currentCommitted(Map.of());
        acceptCommit();

        OffsetSyncResponse response = manager.applyOffsetsSafely(request(0, 500));

        assertTrue(response.getSuccess(), "Группы в целевом ДЦ нет — перезаписывать нечего");
        verify(admin).alterConsumerGroupOffsets(anyString(), any());
    }

    @Test
    @DisplayName("Смещение не двигается назад")
    void offsetShouldNotMoveBackwards() {
        describeFails(new GroupIdNotFoundException("группы еще нет"));
        currentCommitted(Map.of(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(900)));
        acceptCommit();

        OffsetSyncResponse response = manager.applyOffsetsSafely(request(0, 300));

        assertTrue(response.getSuccess());
        verify(admin, never()).alterConsumerGroupOffsets(anyString(), any());
        assertEquals(0, response.getAppliedOffsetsCount(),
                "Откат назад заставил бы группу перечитать уже обработанное");
    }

    @Test
    @DisplayName("Движение вперед проходит")
    void offsetMovingForwardShouldBeApplied() {
        describeFails(new GroupIdNotFoundException("группы еще нет"));
        currentCommitted(Map.of(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(100)));
        acceptCommit();

        OffsetSyncResponse response = manager.applyOffsetsSafely(request(0, 700));

        assertTrue(response.getSuccess());
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).alterConsumerGroupOffsets(anyString(), captor.capture());
        assertEquals(700L, captor.getValue().get(new TopicPartition(TOPIC, 0)).offset());
    }
}
