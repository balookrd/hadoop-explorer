package org.apache.hadoop.explorer.yarn.service;

import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.ClusterMetrics;
import org.apache.hadoop.explorer.yarn.model.ClusterResources;
import org.apache.hadoop.explorer.yarn.model.QueueNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тесты MockYarnClient: эмуляция YARN ResourceManager")
class MockYarnClientTest {

    private MockYarnClient mockClient;
    private ClusterConfig cluster;

    @BeforeEach
    void setUp() {
        cluster = ClusterConfig.builder()
                .id("prod-yarn")
                .name("Production Cluster")
                .resourceManagerUrls(List.of("http://rm1.prod.company.local:8088", "http://rm2.prod.company.local:8088"))
                .partitions(List.of("DEFAULT", "GPU"))
                .totalResources(new ClusterResources(2097152, 1024))
                .build();
        mockClient = new MockYarnClient(cluster);
    }

    @Test
    @DisplayName("Проверка получения активного RM URL")
    void testGetActiveRmUrl() {
        String activeRm = mockClient.getActiveRmUrl();
        assertNotNull(activeRm);
        assertEquals("http://rm1.prod.company.local:8088", activeRm);
    }

    @Test
    @DisplayName("Проверка получения метрик кластера")
    void testGetClusterMetrics() {
        ClusterMetrics metrics = mockClient.getClusterMetrics("admin");
        assertNotNull(metrics);
        assertEquals(2097152, metrics.getTotalMemoryMb());
        assertEquals(1024, metrics.getTotalVcores());
        assertTrue(metrics.getAllocatedMemoryMb() > 0);
        assertTrue(metrics.getAvailableMemoryMb() > 0);
        assertEquals(120, metrics.getActiveNodes());
        assertEquals(2, metrics.getPartitions().size());
    }

    @Test
    @DisplayName("Проверка структуры дерева очередей root -> prod, dev, default")
    void testGetQueueTree() {
        QueueNode root = mockClient.getQueueTree("admin");
        assertNotNull(root);
        assertEquals("root", root.getName());
        assertFalse(root.isLeaf());
        assertEquals(3, root.getChildren().size());

        QueueNode prod = root.getChildren().stream()
                .filter(q -> "prod".equals(q.getName()))
                .findFirst()
                .orElseThrow();
        assertEquals("root.prod", prod.getPath());
        assertFalse(prod.isLeaf());
        assertEquals(3, prod.getChildren().size()); // spark, flink, hive

        QueueNode spark = prod.getChildren().stream()
                .filter(q -> "spark".equals(q.getName()))
                .findFirst()
                .orElseThrow();
        assertTrue(spark.isLeaf());
        assertEquals(40.0, spark.getPartitions().get("DEFAULT").getCapacity());
        assertTrue(spark.getPartitions().containsKey("GPU"));
    }

    @Test
    @DisplayName("Проверка загрузки базового XML шаблона")
    void testGetCapacitySchedulerXml() {
        String xml = mockClient.getCapacitySchedulerXml("admin");
        assertNotNull(xml);
        assertTrue(xml.contains("<configuration>"));
        assertTrue(xml.contains("yarn.scheduler.capacity.root.queues"));
    }
}
