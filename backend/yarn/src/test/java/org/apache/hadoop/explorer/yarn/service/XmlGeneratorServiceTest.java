package org.apache.hadoop.explorer.yarn.service;

import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.PartitionResourceConfig;
import org.apache.hadoop.explorer.yarn.model.QueueDraftItem;
import org.apache.hadoop.explorer.yarn.model.QueueState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тесты XmlGeneratorService: генерация и обновление capacity-scheduler.xml")
class XmlGeneratorServiceTest {

    private XmlGeneratorService xmlGeneratorService;

    @BeforeEach
    void setUp() {
        xmlGeneratorService = new XmlGeneratorService();
    }

    @Test
    @DisplayName("Генерация валидного capacity-scheduler.xml с правильной структурой очередей")
    void testGenerateCapacitySchedulerXml() {
        ClusterConfig cluster = ClusterConfig.builder()
                .id("prod-cluster")
                .name("Production Cluster")
                .resourceMode("percentage")
                .queueMappings("u:%user:%user")
                .queueMappingsOverride(false)
                .build();

        QueueDraftItem root = QueueDraftItem.builder()
                .name("root")
                .path("root")
                .parentPath(null)
                .leaf(false)
                .state(QueueState.RUNNING)
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(100.0).maxCapacity(100.0).build()))
                .build();

        QueueDraftItem prod = QueueDraftItem.builder()
                .name("prod")
                .path("root.prod")
                .parentPath("root")
                .leaf(false)
                .state(QueueState.RUNNING)
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(60.0).maxCapacity(100.0).build()))
                .build();

        QueueDraftItem dev = QueueDraftItem.builder()
                .name("dev")
                .path("root.dev")
                .parentPath("root")
                .leaf(true)
                .state(QueueState.RUNNING)
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(40.0).maxCapacity(50.0).build()))
                .build();

        String xml = xmlGeneratorService.generateCapacitySchedulerXml(
                List.of(root, prod, dev),
                cluster,
                "admin_user",
                "Update queue configuration",
                "percentage",
                "u:%user:%user",
                false,
                null
        );

        assertNotNull(xml);
        assertTrue(xml.contains("<configuration>"));
        assertTrue(xml.contains("</configuration>"));

        // Проверяем наличие дочерних очередей root
        assertTrue(xml.contains("<name>yarn.scheduler.capacity.root.queues</name>"));
        assertTrue(xml.contains("<value>prod,dev</value>"));

        // Проверяем емкости
        assertTrue(xml.contains("<name>yarn.scheduler.capacity.root.prod.capacity</name>"));
        assertTrue(xml.contains("<value>60.0</value>"));
        assertTrue(xml.contains("<name>yarn.scheduler.capacity.root.dev.capacity</name>"));
        assertTrue(xml.contains("<value>40.0</value>"));

        // Проверяем маппинг
        assertTrue(xml.contains("<name>yarn.scheduler.capacity.queue-mappings</name>"));
        assertTrue(xml.contains("<value>u:%user:%user</value>"));
    }
}
