package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.UpdateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService.JobOverlapException;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService.JobStateException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("Kafka: изменение задачи и сброс смещений")
class KafkaJobEditAndResetTest {

    private final List<KafkaReplicationJobEntity> jobs = new ArrayList<>();
    private KafkaReplicationJobRepository repository;
    private KafkaAdminClientFactory adminFactory;
    private KafkaReplicationOrchestratorService service;

    @BeforeEach
    void setUp() {
        jobs.clear();
        repository = mock(KafkaReplicationJobRepository.class);
        adminFactory = mock(KafkaAdminClientFactory.class);
        when(repository.findAll()).thenReturn(jobs);
        when(repository.findById(anyString())).thenAnswer(inv ->
                jobs.stream().filter(j -> j.getId().equals(inv.getArgument(0))).findFirst());
        service = new KafkaReplicationOrchestratorService(repository, mock(KafkaTopicProvisioner.class), null, adminFactory);
    }

    private KafkaReplicationJobEntity job(String id, String status, String... topics) {
        KafkaReplicationJobEntity job = new KafkaReplicationJobEntity();
        job.setId(id);
        job.setName(id);
        job.setSourceDc("dc1");
        job.setTargetDc("dc2");
        job.setDirection("UNIDIRECTIONAL");
        job.setStatus(status);
        job.setSourceBootstrapServers("kafka-dc1:9092");
        job.setAutoCreateTopics(false);
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < topics.length; i++) {
            json.append(i > 0 ? "," : "").append("{\"topic\":\"").append(topics[i]).append("\"}");
        }
        job.setTopicsJson(json.append("]").toString());
        job.setConsumerThreads(4);
        job.setBatchSize(2000);
        job.setBandwidthLimitMb(50.0);
        job.setOffsetSyncIntervalSec(5);
        jobs.add(job);
        return job;
    }

    private static CreateKafkaJobRequest.TopicSetting topic(String name) {
        CreateKafkaJobRequest.TopicSetting t = new CreateKafkaJobRequest.TopicSetting();
        t.setTopic(name);
        return t;
    }

    @Test
    @DisplayName("Изменение пишет только настройки; незаданные поля остаются прежними")
    void testUpdateWritesSettingsOnly() {
        job("job-1", "ACTIVE", "orders");
        UpdateKafkaJobRequest req = new UpdateKafkaJobRequest();
        req.setConsumerThreads(8);
        req.setBandwidthLimitMb(0.0);

        assertTrue(service.updateJob("job-1", req).isPresent());

        verify(repository).updateSettings(eq("job-1"), eq("job-1"), eq("[{\"topic\":\"orders\"}]"), any(),
                eq(8), eq(2000), eq(0.0), eq(5), any());
        verify(repository, never()).save(any());
    }

    @Test
    @DisplayName("Недопустимые значения и пустой список топиков отклоняются, пересечение — 409")
    void testUpdateValidation() {
        job("job-1", "ACTIVE", "orders");
        job("job-2", "ACTIVE", "payments");

        UpdateKafkaJobRequest badBatch = new UpdateKafkaJobRequest();
        badBatch.setBatchSize(0);
        assertThrows(IllegalArgumentException.class, () -> service.updateJob("job-1", badBatch));

        UpdateKafkaJobRequest noTopics = new UpdateKafkaJobRequest();
        noTopics.setTopics(List.of(topic(" ")));
        assertThrows(IllegalArgumentException.class, () -> service.updateJob("job-1", noTopics));

        UpdateKafkaJobRequest overlap = new UpdateKafkaJobRequest();
        overlap.setTopics(List.of(topic("orders"), topic("payments")));
        assertThrows(JobOverlapException.class, () -> service.updateJob("job-1", overlap));

        verify(repository, never()).updateSettings(any(), any(), any(), any(), any(), any(), any(), any(), any());
        assertTrue(service.updateJob("missing", new UpdateKafkaJobRequest()).isEmpty());
    }

    @Test
    @DisplayName("Сброс смещений только на паузе и после остановки пулов агентами")
    void testResetRequiresStoppedJob() {
        job("job-active", "ACTIVE", "orders");
        assertThrows(JobStateException.class, () -> service.resetOffsets("job-active", "LATEST"));

        job("job-paused", "PAUSED", "orders");
        service.updateJobMetricsFromHeartbeat("agent-1",
                List.of(new AgentHeartbeatRequest.KafkaJobStats("job-paused", 1, 1, 0)));
        JobStateException stillRunning = assertThrows(JobStateException.class,
                () -> service.resetOffsets("job-paused", "LATEST"));
        assertTrue(stillRunning.getMessage().contains("agent-1"));

        assertThrows(IllegalArgumentException.class, () -> service.resetOffsets("job-paused", "YESTERDAY"));
        verifyNoInteractions(adminFactory);
    }

    @Test
    @DisplayName("Сброс выставляет смещения группы задачи на конец всех партиций, убрав зависших участников")
    void testResetMovesJobGroupToLatest() {
        job("job-paused", "PAUSED", "orders");
        AdminClient admin = mock(AdminClient.class);
        when(adminFactory.createAdminClient(eq("kafka-dc1:9092"), anyBoolean(), any(), any(), any())).thenReturn(admin);

        Node node = new Node(1, "b", 9092);
        TopicDescription orders = new TopicDescription("orders", false, List.of(
                new TopicPartitionInfo(0, node, List.of(node), List.of(node)),
                new TopicPartitionInfo(1, node, List.of(node), List.of(node))));
        DescribeTopicsResult describe = mock(DescribeTopicsResult.class);
        when(describe.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("orders", orders)));
        when(admin.describeTopics(anyCollection())).thenReturn(describe);

        ListOffsetsResult listed = mock(ListOffsetsResult.class);
        when(listed.all()).thenReturn(KafkaFuture.completedFuture(Map.of(
                new TopicPartition("orders", 0), new ListOffsetsResult.ListOffsetsResultInfo(120, 0, Optional.empty()),
                new TopicPartition("orders", 1), new ListOffsetsResult.ListOffsetsResultInfo(340, 0, Optional.empty()))));
        when(admin.listOffsets(anyMap())).thenReturn(listed);
        RemoveMembersFromConsumerGroupResult removed = mock(RemoveMembersFromConsumerGroupResult.class);
        when(removed.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.removeMembersFromConsumerGroup(anyString(), any())).thenReturn(removed);
        AlterConsumerGroupOffsetsResult altered = mock(AlterConsumerGroupOffsetsResult.class);
        when(altered.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterConsumerGroupOffsets(anyString(), anyMap())).thenReturn(altered);

        Map<String, Integer> result = service.resetOffsets("job-paused", "latest").orElseThrow();

        assertEquals(Map.of("kafka-replicator-dc1-job-paused", 2), result);
        verify(admin).removeMembersFromConsumerGroup(eq("kafka-replicator-dc1-job-paused"), any());
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).alterConsumerGroupOffsets(eq("kafka-replicator-dc1-job-paused"), captor.capture());
        assertEquals(120L, captor.getValue().get(new TopicPartition("orders", 0)).offset());
        assertEquals(340L, captor.getValue().get(new TopicPartition("orders", 1)).offset());
        verify(admin).close();
    }
}
