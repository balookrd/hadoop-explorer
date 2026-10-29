package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.kafka.KafkaTopicProvisioner;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Авторасчет лимита полосы Kafka-задачи.
 *
 * <p>Задача из интерфейса с настройками по умолчанию получала 40 МБ/с на всю задачу: реестр
 * топологии подставлял незаданному каналу 100 МБ/с, а расчет брал от них 40%. Для 2 млн
 * событий/с это в десятки раз меньше нужного. Агенты без {@code --max-bandwidth-mb-s} тоже
 * считались по 100 МБ/с и ограничивали задачу суммой этих выдуманных значений.
 */
@DisplayName("Kafka: авторасчет лимита задачи опирается только на заданные лимиты")
class KafkaAutoBandwidthTest {

    private static List<AgentRegistry.AgentEntry> senders(int count, Double bandwidth) {
        return IntStream.range(0, count)
                .mapToObj(i -> new AgentRegistry.AgentEntry("agent-" + i, "dc1", "agent-" + i, 50052,
                        "agent-" + i + ":50052", "dual", bandwidth))
                .toList();
    }

    private static KafkaReplicationJobEntity create(TopologyRegistry topology, List<AgentRegistry.AgentEntry> senders) {
        KafkaReplicationJobRepository repository = mock(KafkaReplicationJobRepository.class);
        when(repository.findAll()).thenReturn(new ArrayList<>());
        when(repository.save(any(KafkaReplicationJobEntity.class))).thenAnswer(inv -> inv.getArgument(0));
        AgentRegistry agents = mock(AgentRegistry.class);
        when(agents.getLiveKafkaSendersForCluster(anyString())).thenReturn(senders);
        KafkaReplicationOrchestratorService service = new KafkaReplicationOrchestratorService(
                repository, mock(KafkaTopicProvisioner.class), agents, null, topology);

        CreateKafkaJobRequest request = new CreateKafkaJobRequest();
        request.setName("events");
        request.setSourceDc("dc1");
        request.setTargetDc("dc2");
        CreateKafkaJobRequest.TopicSetting topic = new CreateKafkaJobRequest.TopicSetting();
        topic.setTopic("events");
        request.setTopics(List.of(topic));
        request.setAutoCreateTopics(false);
        service.createJob(request);

        var captor = org.mockito.ArgumentCaptor.forClass(KafkaReplicationJobEntity.class);
        verify(repository, atLeastOnce()).save(captor.capture());
        return captor.getValue();
    }

    @Test
    @DisplayName("Без лимита канала и лимитов агентов задача создается без лимита")
    void testNoConfiguredLimitsMeansUnlimited() {
        KafkaReplicationJobEntity job = create(new TopologyRegistry(new ReplicatorProperties()), senders(12, null));
        assertEquals(0.0, job.getBandwidthLimitMb(),
                "незаданный канал и агенты без --max-bandwidth-mb-s не ограничивают задачу");
    }

    @Test
    @DisplayName("Лимит канала по умолчанию (ЦОД есть в топологии, лимит не задан) задачу не ограничивает")
    void testDefaultDcLimitIsNotAQuota() {
        ReplicatorProperties properties = new ReplicatorProperties();
        ReplicatorProperties.DatacenterConfig dc1 = new ReplicatorProperties.DatacenterConfig();
        dc1.setId("dc1");
        ReplicatorProperties.DatacenterConfig dc2 = new ReplicatorProperties.DatacenterConfig();
        dc2.setId("dc2");
        properties.setDatacenters(List.of(dc1, dc2));
        TopologyRegistry topology = new TopologyRegistry(properties);
        assertTrue(topology.getDcLimit("dc1", "dc2").isPresent(), "троттлер HDFS по-прежнему видит лимит по умолчанию");
        assertTrue(topology.getConfiguredDcLimit("dc1", "dc2").isEmpty());
        assertTrue(topology.getDcLimitsList().stream().noneMatch(l -> l.configured()));

        KafkaReplicationJobEntity job = create(topology, senders(12, null));
        assertEquals(0.0, job.getBandwidthLimitMb(), "подставленные 100 МБ/с давали задаче 40 МБ/с");
    }

    @Test
    @DisplayName("Заданный лимит канала дает 40% квоты, а агенты без лимита ее не уменьшают")
    void testConfiguredDcLimitIsUsed() {
        TopologyRegistry topology = new TopologyRegistry(new ReplicatorProperties());
        topology.setDcLimit("dc1", "dc2", 10_000L * 1024 * 1024);
        KafkaReplicationJobEntity job = create(topology, senders(12, null));
        assertEquals(4000.0, job.getBandwidthLimitMb(), 0.01);
    }

    @Test
    @DisplayName("Лимиты, заданные всем агентам, суммируются в емкость отправителей")
    void testExplicitAgentLimitsCapTheJob() {
        KafkaReplicationJobEntity job = create(new TopologyRegistry(new ReplicatorProperties()), senders(12, 250.0));
        assertEquals(3000.0, job.getBandwidthLimitMb(), 0.01);
    }
}
