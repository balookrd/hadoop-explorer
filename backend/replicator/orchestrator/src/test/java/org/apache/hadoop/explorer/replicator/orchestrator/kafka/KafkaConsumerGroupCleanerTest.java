package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("Kafka: консьюмер-группы удаленной задачи удаляются с кластера")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KafkaConsumerGroupCleanerTest {

    private static DeleteConsumerGroupsResult failing(Throwable error) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        DeleteConsumerGroupsResult result = mock(DeleteConsumerGroupsResult.class);
        when(result.all()).thenReturn(future);
        return result;
    }

    private static DeleteConsumerGroupsResult succeeded() {
        DeleteConsumerGroupsResult result = mock(DeleteConsumerGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        return result;
    }

    private static final KafkaConsumerGroupCleaner.GroupTarget TARGET = new KafkaConsumerGroupCleaner.GroupTarget(
            "kafka-dc1:9092", true, "replicator@PROD", "/etc/replicator.keytab", "kafka",
            "kafka-replicator-dc1-kafka-job-1");

    @Test
    @DisplayName("Группа еще не пуста: удаление повторяется, пока брокер его не примет")
    void testRetriesUntilGroupIsEmpty() {
        AdminClient admin = mock(AdminClient.class);
        AtomicInteger calls = new AtomicInteger();
        when(admin.deleteConsumerGroups(anyCollection())).thenAnswer(inv ->
                calls.incrementAndGet() < 3 ? failing(new GroupNotEmptyException("members remain")) : succeeded());
        KafkaAdminClientFactory factory = mock(KafkaAdminClientFactory.class);
        when(factory.createAdminClient(anyString(), anyBoolean(), any(), any(), any())).thenReturn(admin);

        new KafkaConsumerGroupCleaner(factory, 20L, 5).scheduleDeletion("kafka-job-1", List.of(TARGET));

        verify(admin, timeout(5_000).times(3)).deleteConsumerGroups(List.of("kafka-replicator-dc1-kafka-job-1"));
        verify(factory, atLeastOnce()).createAdminClient("kafka-dc1:9092", true, "replicator@PROD",
                "/etc/replicator.keytab", "kafka");
        verify(admin, timeout(5_000).times(3)).close();
    }

    @Test
    @DisplayName("Группы нет — повторов нет; число попыток ограничено")
    void testStopsOnMissingGroupAndAfterMaxAttempts() throws Exception {
        AdminClient gone = mock(AdminClient.class);
        when(gone.deleteConsumerGroups(anyCollection())).thenAnswer(inv -> failing(new GroupIdNotFoundException("no group")));
        KafkaAdminClientFactory goneFactory = mock(KafkaAdminClientFactory.class);
        when(goneFactory.createAdminClient(anyString(), anyBoolean(), any(), any(), any())).thenReturn(gone);
        new KafkaConsumerGroupCleaner(goneFactory, 20L, 5).scheduleDeletion("kafka-job-1", List.of(TARGET));

        AdminClient busy = mock(AdminClient.class);
        when(busy.deleteConsumerGroups(anyCollection())).thenAnswer(inv -> failing(new GroupNotEmptyException("busy")));
        KafkaAdminClientFactory busyFactory = mock(KafkaAdminClientFactory.class);
        when(busyFactory.createAdminClient(anyString(), anyBoolean(), any(), any(), any())).thenReturn(busy);
        new KafkaConsumerGroupCleaner(busyFactory, 20L, 4).scheduleDeletion("kafka-job-1", List.of(TARGET));

        verify(busy, timeout(5_000).times(4)).deleteConsumerGroups(anyCollection());
        Thread.sleep(200);
        verify(gone, times(1)).deleteConsumerGroups(anyCollection());
        verify(busy, times(4)).deleteConsumerGroups(anyCollection());
    }
}
