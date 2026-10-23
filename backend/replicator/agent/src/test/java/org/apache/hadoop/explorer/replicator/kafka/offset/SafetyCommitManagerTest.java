package org.apache.hadoop.explorer.replicator.kafka.offset;

import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncResponse;
import org.apache.hadoop.explorer.replicator.kafka.generated.TopicPartitionOffset;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

@DisplayName("Тестирование менеджера безопасного применения оффсетов (SafetyCommitManager)")
class SafetyCommitManagerTest {

    private AdminClient mockAdminClient;
    private KafkaConsumer<?, ?> mockTimeResolver;
    private SafetyCommitManager manager;

    @BeforeEach
    void setUp() {
        mockAdminClient = mock(AdminClient.class);
        mockTimeResolver = mock(KafkaConsumer.class);
        manager = new SafetyCommitManager(mockAdminClient, mockTimeResolver);
    }

    @Test
    @DisplayName("Отклонение коммита смещений для служебных групп репликатора и инфраструктуры")
    void shouldRejectBlacklistedConsumerGroups() {
        List<String> blacklisted = List.of(
                "kafka-replicator-dc1",
                "kafka-replicator-time-resolver-dc2",
                "replicator-agent-01",
                "__consumer_offsets",
                "schema-registry-dc1",
                "connect-cluster-01",
                "console-consumer-12345"
        );

        for (String groupId : blacklisted) {
            OffsetSyncRequest request = OffsetSyncRequest.newBuilder()
                    .setGroupId(groupId)
                    .setSourceDc("DC1")
                    .setTargetDc("DC2")
                    .addOffsets(TopicPartitionOffset.newBuilder()
                            .setTopic("orders")
                            .setPartition(0)
                            .setSourceOffset(100L)
                            .build())
                    .build();

            OffsetSyncResponse response = manager.applyOffsetsSafely(request);

            assertFalse(response.getSuccess(), "Коммит для черного списка должен быть отклонен!");
            assertFalse(response.getBypassedActiveGroup());
            assertTrue(response.getErrorMessage().contains("Group is blacklisted"),
                    "Сообщение об ошибке должно указывать на черный список: " + response.getErrorMessage());
        }

        verifyNoInteractions(mockAdminClient);
    }

    @Test
    @DisplayName("Пропуск коммита для активной группы в целевом кластере (isGroupActiveInTarget)")
    void shouldBypassActiveGroupInTargetCluster() {
        String groupId = "active-orders-consumer";

        DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
        ConsumerGroupDescription desc = new ConsumerGroupDescription(
                groupId, false, Collections.emptyList(), "roundrobin",
                ConsumerGroupState.STABLE, new Node(1, "b1", 9092)
        );
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, desc)));
        when(mockAdminClient.describeConsumerGroups(anyCollection())).thenReturn(describeResult);

        OffsetSyncRequest request = OffsetSyncRequest.newBuilder()
                .setGroupId(groupId)
                .setSourceDc("DC1")
                .setTargetDc("DC2")
                .setForceCommit(false)
                .addOffsets(TopicPartitionOffset.newBuilder()
                        .setTopic("orders")
                        .setPartition(0)
                        .setSourceOffset(200L)
                        .build())
                .build();

        OffsetSyncResponse response = manager.applyOffsetsSafely(request);

        assertTrue(response.getSuccess());
        assertTrue(response.getBypassedActiveGroup(), "Должен быть выставлен флаг bypassedActiveGroup");
        verify(mockAdminClient, never()).alterConsumerGroupOffsets(any(), any());
    }

    @Test
    @DisplayName("Успешное применение смещений для неактивной бизнес-группы с прямым оффсетом (direct target offset)")
    void shouldApplyOffsetsForInactiveGroup() {
        String groupId = "standby-analytics-group";

        DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
        ConsumerGroupDescription desc = new ConsumerGroupDescription(
                groupId, false, Collections.emptyList(), "roundrobin",
                ConsumerGroupState.EMPTY, new Node(1, "b1", 9092)
        );
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, desc)));
        when(mockAdminClient.describeConsumerGroups(anyCollection())).thenReturn(describeResult);

        AlterConsumerGroupOffsetsResult alterResult = mock(AlterConsumerGroupOffsetsResult.class);
        when(alterResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockAdminClient.alterConsumerGroupOffsets(eq(groupId), anyMap())).thenReturn(alterResult);

        OffsetSyncRequest request = OffsetSyncRequest.newBuilder()
                .setGroupId(groupId)
                .setSourceDc("DC1")
                .setTargetDc("DC2")
                .addOffsets(TopicPartitionOffset.newBuilder()
                        .setTopic("analytics")
                        .setPartition(0)
                        .setSourceOffset(500L)
                        .setTargetOffset(480L)
                        .build())
                .build();

        OffsetSyncResponse response = manager.applyOffsetsSafely(request);

        assertTrue(response.getSuccess());
        assertFalse(response.getBypassedActiveGroup());
        assertEquals(1, response.getAppliedOffsetsCount());
        assertEquals(480L, response.getAppliedOffsets(0).getTargetOffset());

        verify(mockAdminClient).alterConsumerGroupOffsets(eq(groupId), anyMap());
    }
}
