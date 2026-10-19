package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.kafka.KafkaAdminClientFactory;
import org.apache.hadoop.explorer.replicator.orchestrator.kafka.KafkaTopicProvisioner;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Модульные тесты для Live Discovery топиков и консьюмер-групп в Control Plane
 * с валидацией запросов к Kafka AdminClient и graceful fallback.
 */
public class KafkaLiveDiscoveryTest {

    private KafkaAdminClientFactory adminClientFactory;
    private AdminClient adminClient;
    private KafkaReplicationOrchestratorService service;

    @BeforeEach
    void setUp() {
        adminClientFactory = mock(KafkaAdminClientFactory.class);
        adminClient = mock(AdminClient.class);
        KafkaReplicationJobRepository repository = mock(KafkaReplicationJobRepository.class);
        KafkaTopicProvisioner provisioner = mock(KafkaTopicProvisioner.class);

        when(adminClientFactory.createAdminClient(eq("live-kafka:9092"), anyBoolean(), any(), any(), any()))
                .thenReturn(adminClient);

        service = new KafkaReplicationOrchestratorService(repository, provisioner, null, adminClientFactory);
    }

    @Test
    @DisplayName("Live Discovery: успешное обнаружение топиков из живого кластера Kafka с фильтрацией системных")
    void shouldDiscoverTopicsFromLiveCluster() throws Exception {
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        KafkaFuture<Set<String>> namesFuture = mock(KafkaFuture.class);
        when(namesFuture.get(anyLong(), any())).thenReturn(Set.of("telemetry-events", "orders-avro", "__consumer_offsets"));
        when(listTopicsResult.names()).thenReturn(namesFuture);
        when(adminClient.listTopics(any(ListTopicsOptions.class))).thenReturn(listTopicsResult);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        KafkaFuture<Map<String, TopicDescription>> descFuture = mock(KafkaFuture.class);

        Node node = new Node(1, "broker", 9092);
        TopicPartitionInfo p0 = new TopicPartitionInfo(0, node, List.of(node), List.of(node));
        TopicPartitionInfo p1 = new TopicPartitionInfo(1, node, List.of(node), List.of(node));

        TopicDescription desc1 = new TopicDescription("telemetry-events", false, List.of(p0, p1));
        TopicDescription desc2 = new TopicDescription("orders-avro", false, List.of(p0));
        TopicDescription descSystem = new TopicDescription("__consumer_offsets", true, List.of(p0));

        Map<String, TopicDescription> descMap = Map.of(
                "telemetry-events", desc1,
                "orders-avro", desc2,
                "__consumer_offsets", descSystem
        );
        when(descFuture.get(anyLong(), any())).thenReturn(descMap);
        when(describeTopicsResult.allTopicNames()).thenReturn(descFuture);
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenReturn(describeTopicsResult);

        List<Map<String, Object>> topics = service.discoverTopics("live-kafka:9092", false, null, null, null);

        assertEquals(2, topics.size(), "Системный топик __consumer_offsets должен быть отфильтрован");

        Map<String, Object> ordersTopic = topics.stream()
                .filter(t -> "orders-avro".equals(t.get("name")))
                .findFirst()
                .orElseThrow();
        assertEquals(1, ordersTopic.get("partitions"));
        assertEquals("SCHEMA_REGISTRY", ordersTopic.get("detectedFormat"));
        assertTrue((Boolean) ordersTopic.get("hasSchema"));

        Map<String, Object> telemetryTopic = topics.stream()
                .filter(t -> "telemetry-events".equals(t.get("name")))
                .findFirst()
                .orElseThrow();
        assertEquals(2, telemetryTopic.get("partitions"));
        assertEquals("AUTO_DETECT", telemetryTopic.get("detectedFormat"));
        assertFalse((Boolean) telemetryTopic.get("hasSchema"));
    }

    @Test
    @DisplayName("Live Discovery: успешное обнаружение консьюмер-групп из кластера Kafka")
    void shouldDiscoverGroupsFromLiveCluster() throws Exception {
        ListConsumerGroupsResult listGroupsResult = mock(ListConsumerGroupsResult.class);
        KafkaFuture<Collection<ConsumerGroupListing>> listingFuture = mock(KafkaFuture.class);
        ConsumerGroupListing listing1 = new ConsumerGroupListing("billing-worker-grp", false);
        ConsumerGroupListing listing2 = new ConsumerGroupListing("analytics-grp", false);
        when(listingFuture.get(anyLong(), any())).thenReturn(List.of(listing1, listing2));
        when(listGroupsResult.all()).thenReturn(listingFuture);
        when(adminClient.listConsumerGroups(any(ListConsumerGroupsOptions.class))).thenReturn(listGroupsResult);

        DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
        KafkaFuture<Map<String, ConsumerGroupDescription>> descFuture = mock(KafkaFuture.class);

        MemberDescription member = new MemberDescription("m1", Optional.empty(), "client-1", "host-1", null);
        ConsumerGroupDescription g1 = new ConsumerGroupDescription("billing-worker-grp", false, List.of(member), "range", ConsumerGroupState.STABLE, new Node(1, "b", 9092));
        ConsumerGroupDescription g2 = new ConsumerGroupDescription("analytics-grp", false, Collections.emptyList(), "range", ConsumerGroupState.EMPTY, new Node(1, "b", 9092));

        when(descFuture.get(anyLong(), any())).thenReturn(Map.of("billing-worker-grp", g1, "analytics-grp", g2));
        when(describeResult.all()).thenReturn(descFuture);
        when(adminClient.describeConsumerGroups(anyCollection(), any(DescribeConsumerGroupsOptions.class))).thenReturn(describeResult);

        List<Map<String, Object>> groups = service.discoverConsumerGroups("live-kafka:9092", false, null, null, null);

        assertEquals(2, groups.size());
        Map<String, Object> billingGroup = groups.stream()
                .filter(g -> "billing-worker-grp".equals(g.get("groupId")))
                .findFirst()
                .orElseThrow();
        assertEquals("STABLE", billingGroup.get("state"));
        assertEquals(1, billingGroup.get("members"));

        Map<String, Object> analyticsGroup = groups.stream()
                .filter(g -> "analytics-grp".equals(g.get("groupId")))
                .findFirst()
                .orElseThrow();
        assertEquals("EMPTY", analyticsGroup.get("state"));
        assertEquals(0, analyticsGroup.get("members"));
    }

    @Test
    @DisplayName("Безопасная обработка ошибок: возврат пустого списка топиков при сетевой ошибке или недоступности брокера")
    void shouldReturnEmptyListWhenClusterUnreachable() {
        when(adminClientFactory.createAdminClient(eq("unreachable:9092"), anyBoolean(), any(), any(), any()))
                .thenThrow(new org.apache.kafka.common.KafkaException("Connection refused to unreachable:9092"));

        List<Map<String, Object>> topics = service.discoverTopics("unreachable:9092", false, null, null, null);

        assertNotNull(topics);
        assertTrue(topics.isEmpty(), "При недоступности брокера должен возвращаться пустой список без мок-данных");
    }

    @Test
    @DisplayName("Безопасная обработка ошибок: возврат пустого списка консьюмер-групп при недоступности брокера")
    void shouldReturnEmptyListWhenGroupsUnreachable() {
        when(adminClientFactory.createAdminClient(eq("unreachable:9092"), anyBoolean(), any(), any(), any()))
                .thenThrow(new org.apache.kafka.common.KafkaException("Connection refused to unreachable:9092"));

        List<Map<String, Object>> groups = service.discoverConsumerGroups("unreachable:9092", false, null, null, null);

        assertNotNull(groups);
        assertTrue(groups.isEmpty(), "При недоступности брокера должен возвращаться пустой список без мок-данных");
    }

    @Test
    @DisplayName("Валидация параметров: возврат пустого списка при пустом bootstrapServers без обращения к KafkaAdminClientFactory")
    void shouldReturnEmptyImmediatelyWhenBootstrapServersIsEmpty() {
        List<Map<String, Object>> topics = service.discoverTopics("", false, null, null, null);
        assertTrue(topics.isEmpty());

        List<Map<String, Object>> groups = service.discoverConsumerGroups(null, false, null, null, null);
        assertTrue(groups.isEmpty());

        verifyNoInteractions(adminClientFactory);
    }
}
