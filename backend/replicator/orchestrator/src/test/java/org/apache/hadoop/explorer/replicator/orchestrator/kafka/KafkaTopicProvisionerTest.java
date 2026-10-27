package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Модульные и интеграционные тесты для механизма автосоздания топиков Kafka
 * с проверкой и сохранением топологии партиционирования.
 */
public class KafkaTopicProvisionerTest {

    private KafkaAdminClientFactory adminClientFactory;
    private AdminClient sourceAdmin;
    private AdminClient targetAdmin;
    private KafkaTopicProvisioner provisioner;

    private Node node1;
    private Node node2;

    @BeforeEach
    void setUp() {
        adminClientFactory = mock(KafkaAdminClientFactory.class);
        sourceAdmin = mock(AdminClient.class);
        targetAdmin = mock(AdminClient.class);

        when(adminClientFactory.createAdminClient(eq("source:9092"), anyBoolean(), any(), any(), any()))
                .thenReturn(sourceAdmin);
        when(adminClientFactory.createAdminClient(eq("target:9092"), anyBoolean(), any(), any(), any()))
                .thenReturn(targetAdmin);

        provisioner = new KafkaTopicProvisioner(adminClientFactory);

        node1 = new Node(1, "broker-1", 9092);
        node2 = new Node(2, "broker-2", 9092);
    }

    private TopicDescription createTopicDescription(String topicName, int partitionCount, int replicationFactor) {
        List<TopicPartitionInfo> partitions = new ArrayList<>();
        List<Node> replicas = replicationFactor > 1 ? List.of(node1, node2) : List.of(node1);
        for (int p = 0; p < partitionCount; p++) {
            partitions.add(new TopicPartitionInfo(p, node1, replicas, replicas));
        }
        return new TopicDescription(topicName, false, partitions);
    }

    @Test
    @DisplayName("Автосоздание топика: создание отсутствующего топика с сохранением партиций и фактора репликации")
    void shouldCreateMissingTopicWithOriginalPartitionsAndReplicationFactor() throws Exception {
        String topic = "orders-stream";

        // Описание на источнике: 6 партиций, RF = 2
        TopicDescription srcDesc = createTopicDescription(topic, 6, 2);
        DescribeTopicsResult srcDescResult = mock(DescribeTopicsResult.class);
        when(srcDescResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, srcDesc)));
        when(sourceAdmin.describeTopics(anyCollection())).thenReturn(srcDescResult);

        // Список топиков на таргете: пустой (топик отсутствует)
        ListTopicsResult tgtListResult = mock(ListTopicsResult.class);
        when(tgtListResult.names()).thenReturn(KafkaFuture.completedFuture(Collections.emptySet()));
        when(targetAdmin.listTopics(any(ListTopicsOptions.class))).thenReturn(tgtListResult);

        // Мок создания топика
        CreateTopicsResult tgtCreateResult = mock(CreateTopicsResult.class);
        when(tgtCreateResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(targetAdmin.createTopics(anyCollection())).thenReturn(tgtCreateResult);

        TopicProvisioningResult result = provisioner.provisionTopics(
                "source:9092", false, null, null, null,
                "target:9092", false, null, null, null,
                List.of(topic), true
        );

        assertTrue(result.success());
        assertEquals(1, result.createdTopics().size());
        assertTrue(result.createdTopics().get(0).contains("orders-stream"));
        assertTrue(result.createdTopics().get(0).contains("партиций: 6"));
        assertTrue(result.createdTopics().get(0).contains("RF: 2"));

        // Проверяем захваченный NewTopic
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(targetAdmin).createTopics(captor.capture());

        Collection<NewTopic> created = captor.getValue();
        assertEquals(1, created.size());
        NewTopic newTopic = created.iterator().next();
        assertEquals("orders-stream", newTopic.name());
        assertEquals(6, newTopic.numPartitions());
        assertEquals(2, newTopic.replicationFactor());
    }

    @Test
    @DisplayName("Существующий целевой топик с меньшим числом партиций не меняется, расхождение сообщается")
    void shouldReportFewerTargetPartitionsWithoutExpanding() throws Exception {
        String topic = "payments-v1";

        // Исходный топик имеет 8 партиций
        TopicDescription srcDesc = createTopicDescription(topic, 8, 2);
        DescribeTopicsResult srcDescResult = mock(DescribeTopicsResult.class);
        when(srcDescResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, srcDesc)));
        when(sourceAdmin.describeTopics(anyCollection())).thenReturn(srcDescResult);

        // Целевой топик уже существует
        ListTopicsResult tgtListResult = mock(ListTopicsResult.class);
        when(tgtListResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of(topic)));
        when(targetAdmin.listTopics(any(ListTopicsOptions.class))).thenReturn(tgtListResult);

        // На таргете только 3 партиции
        TopicDescription tgtDesc = createTopicDescription(topic, 3, 2);
        DescribeTopicsResult tgtDescResult = mock(DescribeTopicsResult.class);
        when(tgtDescResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, tgtDesc)));
        when(targetAdmin.describeTopics(anyCollection())).thenReturn(tgtDescResult);

        TopicProvisioningResult result = provisioner.provisionTopics(
                "source:9092", false, null, null, null,
                "target:9092", false, null, null, null,
                List.of(topic), true
        );

        assertFalse(result.success(), "Расхождение — повод для оператора, а не успех");
        assertEquals(1, result.conflicts().size());
        assertTrue(result.conflicts().get(0).contains("партиций 3 < 8"), result.conflicts().get(0));
        verify(targetAdmin, never()).createPartitions(anyMap());
    }

    @Test
    @DisplayName("RF нового топика ограничен числом брокеров цели, min.insync.replicas — RF")
    void shouldCapReplicationFactorByTargetBrokers() throws Exception {
        String topic = "audit";
        TopicDescription srcDesc = createTopicDescription(topic, 4, 2);
        DescribeTopicsResult srcDescResult = mock(DescribeTopicsResult.class);
        when(srcDescResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, srcDesc)));
        when(sourceAdmin.describeTopics(anyCollection())).thenReturn(srcDescResult);

        ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config srcCfg = new Config(List.of(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")));
        DescribeConfigsResult srcCfgResult = mock(DescribeConfigsResult.class);
        when(srcCfgResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(res, srcCfg)));
        when(sourceAdmin.describeConfigs(anyCollection())).thenReturn(srcCfgResult);

        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(node1)));
        when(targetAdmin.describeCluster()).thenReturn(cluster);
        ListTopicsResult tgtListResult = mock(ListTopicsResult.class);
        when(tgtListResult.names()).thenReturn(KafkaFuture.completedFuture(Collections.emptySet()));
        when(targetAdmin.listTopics(any(ListTopicsOptions.class))).thenReturn(tgtListResult);
        CreateTopicsResult tgtCreateResult = mock(CreateTopicsResult.class);
        when(tgtCreateResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(targetAdmin.createTopics(anyCollection())).thenReturn(tgtCreateResult);

        TopicProvisioningResult result = provisioner.provisionTopics(
                "source:9092", false, null, null, null, "target:9092", false, null, null, null, List.of(topic), true);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(targetAdmin).createTopics(captor.capture());
        NewTopic created = captor.getValue().iterator().next();
        assertEquals(1, created.replicationFactor(), "RF 2 на кластере из одного брокера отвергается брокером");
        assertEquals("1", created.configs().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG),
                "min.insync.replicas больше RF запрещает запись с acks=all");
        assertTrue(result.createdTopics().get(0).contains("в источнике 2"));
    }

    @Test
    @DisplayName("Расхождение политики очистки и предела сообщения существующего топика сообщается")
    void shouldReportConfigConflicts() {
        Config src = new Config(List.of(
                new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact"),
                new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "8388608")));
        Config tgt = new Config(List.of(
                new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "delete"),
                new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048588")));
        List<String> conflicts = KafkaTopicProvisioner.configConflicts(src, tgt);
        assertEquals(2, conflicts.size(), conflicts.toString());
        assertTrue(conflicts.get(0).contains("cleanup.policy delete"));
        assertTrue(conflicts.get(1).contains("max.message.bytes 1048588 < 8388608"));

        Config same = new Config(List.of(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "delete,compact")));
        Config reordered = new Config(List.of(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact, delete")));
        assertTrue(KafkaTopicProvisioner.configConflicts(same, reordered).isEmpty());
    }

    @Test
    @DisplayName("Идентичные топики: совпадение партиций не вызывает модификаций")
    void shouldMatchAlreadySynchronizedTopicWithoutChanges() throws Exception {
        String topic = "telemetry-raw";

        TopicDescription srcDesc = createTopicDescription(topic, 4, 1);
        DescribeTopicsResult srcDescResult = mock(DescribeTopicsResult.class);
        when(srcDescResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, srcDesc)));
        when(sourceAdmin.describeTopics(anyCollection())).thenReturn(srcDescResult);

        ListTopicsResult tgtListResult = mock(ListTopicsResult.class);
        when(tgtListResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of(topic)));
        when(targetAdmin.listTopics(any(ListTopicsOptions.class))).thenReturn(tgtListResult);

        TopicDescription tgtDesc = createTopicDescription(topic, 4, 1);
        DescribeTopicsResult tgtDescResult = mock(DescribeTopicsResult.class);
        when(tgtDescResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, tgtDesc)));
        when(targetAdmin.describeTopics(anyCollection())).thenReturn(tgtDescResult);

        TopicProvisioningResult result = provisioner.provisionTopics(
                "source:9092", false, null, null, null,
                "target:9092", false, null, null, null,
                List.of(topic), true
        );

        assertTrue(result.success());
        assertEquals(1, result.matchedTopics().size());
        assertTrue(result.matchedTopics().get(0).contains("партиций: 4"));
        verify(targetAdmin, never()).createTopics(anyCollection());
        verify(targetAdmin, never()).createPartitions(anyMap());
    }

    @Test
    @DisplayName("Интеграция с KafkaReplicationOrchestratorService: сквозной провижининг при создании задачи")
    void shouldTriggerProvisioningInsideOrchestratorServiceOnJobCreate() {
        KafkaReplicationJobRepository repo = mock(KafkaReplicationJobRepository.class);
        when(repo.save(any(KafkaReplicationJobEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // Настройка мок брокеров
        String topic = "events-kafka";
        TopicDescription srcDesc = createTopicDescription(topic, 12, 3);
        DescribeTopicsResult srcDescResult = mock(DescribeTopicsResult.class);
        when(srcDescResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, srcDesc)));
        when(sourceAdmin.describeTopics(anyCollection())).thenReturn(srcDescResult);

        ListTopicsResult tgtListResult = mock(ListTopicsResult.class);
        when(tgtListResult.names()).thenReturn(KafkaFuture.completedFuture(Collections.emptySet()));
        when(targetAdmin.listTopics(any(ListTopicsOptions.class))).thenReturn(tgtListResult);

        CreateTopicsResult tgtCreateResult = mock(CreateTopicsResult.class);
        when(tgtCreateResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(targetAdmin.createTopics(anyCollection())).thenReturn(tgtCreateResult);

        KafkaReplicationOrchestratorService service = new KafkaReplicationOrchestratorService(repo, provisioner);

        CreateKafkaJobRequest req = new CreateKafkaJobRequest();
        req.setName("auto-topic-job");
        req.setSourceDc("DC1");
        req.setTargetDc("DC2");
        req.setSourceBootstrapServers("source:9092");
        req.setTargetBootstrapServers("target:9092");
        req.setDirection("UNIDIRECTIONAL");
        req.setAutoCreateTopics(true);
        req.setTopics(List.of(new CreateKafkaJobRequest.TopicSetting(topic, "AUTO_DETECT")));

        KafkaReplicationJobDto dto = service.createJob(req);

        assertNotNull(dto);
        assertEquals("auto-topic-job", dto.getName());
        assertTrue(dto.isAutoCreateTopics());
        assertEquals("source:9092", dto.getSourceBootstrapServers());
        assertEquals("target:9092", dto.getTargetBootstrapServers());
        assertNotNull(dto.getTopicProvisioningSummary());
        assertTrue(dto.getTopicProvisioningSummary().contains("events-kafka"));
        assertTrue(dto.getTopicProvisioningSummary().contains("партиций: 12"));

        verify(targetAdmin).createTopics(anyCollection());
    }

    @Test
    @DisplayName("Копирование конфигураций: перенос cleanup.policy=compact, min.insync.replicas и retention при создании топика")
    void shouldCopyAdvancedTopicConfigsWhenCreatingNewTopic() throws Exception {
        String topic = "cdc-customers";

        // Описание на источнике: 3 партиции, RF = 2
        TopicDescription srcDesc = createTopicDescription(topic, 3, 2);
        DescribeTopicsResult srcDescResult = mock(DescribeTopicsResult.class);
        when(srcDescResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, srcDesc)));
        when(sourceAdmin.describeTopics(anyCollection())).thenReturn(srcDescResult);

        // Конфигурации на источнике: cleanup.policy=compact, min.insync.replicas=2, retention.ms=86400000
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        List<ConfigEntry> entries = List.of(
                new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, Collections.emptyList(), null, null),
                new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, Collections.emptyList(), null, null),
                new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "86400000", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, Collections.emptyList(), null, null),
                new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, Collections.emptyList(), null, null)
        );
        Config topicConfig = new Config(entries);
        DescribeConfigsResult descConfigsResult = mock(DescribeConfigsResult.class);
        when(descConfigsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource, topicConfig)));
        when(sourceAdmin.describeConfigs(anyCollection())).thenReturn(descConfigsResult);

        // Таргет: топик пока отсутствует
        ListTopicsResult tgtListResult = mock(ListTopicsResult.class);
        when(tgtListResult.names()).thenReturn(KafkaFuture.completedFuture(Collections.emptySet()));
        when(targetAdmin.listTopics(any(ListTopicsOptions.class))).thenReturn(tgtListResult);

        CreateTopicsResult tgtCreateResult = mock(CreateTopicsResult.class);
        when(tgtCreateResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(targetAdmin.createTopics(anyCollection())).thenReturn(tgtCreateResult);

        TopicProvisioningResult result = provisioner.provisionTopics(
                "source:9092", false, null, null, null,
                "target:9092", false, null, null, null,
                List.of(topic), true
        );

        assertTrue(result.success());
        assertEquals(1, result.createdTopics().size());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(targetAdmin).createTopics(captor.capture());

        NewTopic created = captor.getValue().iterator().next();
        assertEquals("cdc-customers", created.name());
        assertEquals(3, created.numPartitions());
        assertEquals(2, created.replicationFactor());

        // Проверяем скопированные конфигурации
        Map<String, String> configs = created.configs();
        assertNotNull(configs);
        assertEquals("compact", configs.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("2", configs.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));
        assertEquals("86400000", configs.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("zstd", configs.get(TopicConfig.COMPRESSION_TYPE_CONFIG));
    }
}

