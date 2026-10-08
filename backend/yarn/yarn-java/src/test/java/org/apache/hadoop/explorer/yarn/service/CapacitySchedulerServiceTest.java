package org.apache.hadoop.explorer.yarn.service;

import org.apache.hadoop.explorer.yarn.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тесты CapacitySchedulerService: валидация баланса и вычисление diff")
class CapacitySchedulerServiceTest {

    private CapacitySchedulerService schedulerService;

    @BeforeEach
    void setUp() {
        schedulerService = new CapacitySchedulerService();
    }

    @Test
    @DisplayName("Валидация баланса очередей: 100% сумма считается валидной")
    void testValidateQueueBalanceSuccess() {
        QueueDraftItem q1 = QueueDraftItem.builder()
                .name("spark")
                .path("root.spark")
                .parentPath("root")
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(60.0).maxCapacity(100.0).build()))
                .build();

        QueueDraftItem q2 = QueueDraftItem.builder()
                .name("flink")
                .path("root.flink")
                .parentPath("root")
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(40.0).maxCapacity(100.0).build()))
                .build();

        List<BranchBalance> balances = schedulerService.validateQueueBalance(List.of(q1, q2), "percentage", "DEFAULT");

        assertNotNull(balances);
        assertEquals(1, balances.size());
        BranchBalance balance = balances.getFirst();
        assertTrue(balance.isBalanced());
        assertEquals("ok", balance.getStatus());
        assertEquals(100.0, balance.getTotalChildrenCapacity(), 0.001);
        assertEquals(0.0, balance.getUnallocatedCapacity(), 0.001);
    }

    @Test
    @DisplayName("Валидация баланса очередей: ошибка underallocated при сумме < 100%")
    void testValidateQueueBalanceUnderallocated() {
        QueueDraftItem q1 = QueueDraftItem.builder()
                .name("spark")
                .path("root.spark")
                .parentPath("root")
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(50.0).maxCapacity(100.0).build()))
                .build();

        QueueDraftItem q2 = QueueDraftItem.builder()
                .name("flink")
                .path("root.flink")
                .parentPath("root")
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(30.0).maxCapacity(100.0).build()))
                .build();

        List<BranchBalance> balances = schedulerService.validateQueueBalance(List.of(q1, q2), "percentage", "DEFAULT");

        assertEquals(1, balances.size());
        BranchBalance balance = balances.getFirst();
        assertFalse(balance.isBalanced());
        assertEquals("underallocated", balance.getStatus());
        assertEquals(80.0, balance.getTotalChildrenCapacity(), 0.001);
        assertEquals(20.0, balance.getUnallocatedCapacity(), 0.001);
    }

    @Test
    @DisplayName("Валидация баланса очередей: ошибка overallocated при сумме > 100%")
    void testValidateQueueBalanceOverallocated() {
        QueueDraftItem q1 = QueueDraftItem.builder()
                .name("spark")
                .path("root.spark")
                .parentPath("root")
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(70.0).maxCapacity(100.0).build()))
                .build();

        QueueDraftItem q2 = QueueDraftItem.builder()
                .name("flink")
                .path("root.flink")
                .parentPath("root")
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(40.0).maxCapacity(100.0).build()))
                .build();

        List<BranchBalance> balances = schedulerService.validateQueueBalance(List.of(q1, q2), "percentage", "DEFAULT");

        assertEquals(1, balances.size());
        BranchBalance balance = balances.getFirst();
        assertFalse(balance.isBalanced());
        assertEquals("overallocated", balance.getStatus());
        assertEquals(110.0, balance.getTotalChildrenCapacity(), 0.001);
    }

    @Test
    @DisplayName("Вычисление diff между live-деревом и черновиком")
    void testComputeDiff() {
        ClusterConfig cluster = ClusterConfig.builder()
                .id("test-cluster")
                .name("Test Cluster")
                .resourceMode("percentage")
                .defaultPartition("DEFAULT")
                .queueMappings("u:%user:%user")
                .build();

        MockYarnClient mockClient = new MockYarnClient(cluster);
        QueueNode liveRoot = mockClient.getQueueTree("admin");

        // Черновик: меняем емкость root.prod.spark с 40 до 50
        QueueDraftItem modifiedSpark = QueueDraftItem.builder()
                .name("spark")
                .path("root.prod.spark")
                .parentPath("root.prod")
                .action("modify")
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(50.0).maxCapacity(80.0).build()))
                .build();

        // Черновик: создаем новую очередь root.prod.ml
        QueueDraftItem newMl = QueueDraftItem.builder()
                .name("ml")
                .path("root.prod.ml")
                .parentPath("root.prod")
                .action("create")
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(10.0).maxCapacity(30.0).build()))
                .build();

        DraftDiffResponse diffResponse = schedulerService.computeDiff(
                cluster,
                List.of(modifiedSpark, newMl),
                liveRoot,
                "DEFAULT",
                "u:%user:%user",
                false
        );

        assertNotNull(diffResponse);
        assertTrue(diffResponse.isHasChanges());
        assertEquals(2, diffResponse.getDiffs().size());

        DiffItem diffSpark = diffResponse.getDiffs().stream()
                .filter(d -> "root.prod.spark".equals(d.getPath()))
                .findFirst()
                .orElseThrow();
        assertEquals("modified", diffSpark.getAction());
        assertEquals(40.0, diffSpark.getLiveCapacity());
        assertEquals(50.0, diffSpark.getDraftCapacity());
        assertEquals(10.0, diffSpark.getDeltaCapacity());

        DiffItem diffMl = diffResponse.getDiffs().stream()
                .filter(d -> "root.prod.ml".equals(d.getPath()))
                .findFirst()
                .orElseThrow();
        assertEquals("created", diffMl.getAction());
        assertEquals(10.0, diffMl.getDraftCapacity());
        assertNull(diffMl.getLiveCapacity());
    }
}
