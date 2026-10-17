package org.apache.hadoop.explorer.replicator.kafka.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Unit Test: KafkaTargetBalancer (Partition-Pinned Routing & Failover)")
class KafkaTargetBalancerTest {

    @Test
    @DisplayName("Равномерное распределение 128 партиций по 4 агентам (по 32 партиции на узел)")
    void shouldEvenlyDistribute128PartitionsAcross4Nodes() {
        List<String> endpoints = List.of(
                "agent-1:50052",
                "agent-2:50052",
                "agent-3:50052",
                "agent-4:50052"
        );
        KafkaTargetBalancer balancer = new KafkaTargetBalancer(endpoints);

        assertEquals(4, balancer.getTargetCount());
        assertEquals(4, balancer.getHealthyIndices().size());

        Map<Integer, List<Integer>> partitionsByTarget = new HashMap<>();
        for (int p = 0; p < 128; p++) {
            int targetIdx = balancer.selectTargetIndex(p);
            partitionsByTarget.computeIfAbsent(targetIdx, k -> new ArrayList<>()).add(p);
        }

        // Проверяем, что задействованы все 4 узла
        assertEquals(4, partitionsByTarget.size());

        // Ровно 32 партиции на каждый узел
        for (int i = 0; i < 4; i++) {
            List<Integer> parts = partitionsByTarget.get(i);
            assertNotNull(parts, "Узел #" + i + " должен получить партиции");
            assertEquals(32, parts.size(), "На узел #" + i + " должно приходиться ровно 32 партиции");
        }
    }

    @Test
    @DisplayName("Детерминированность: повторные обращения для одной партиции всегда дают один и тот же целевой узел")
    void shouldMaintainStrictStickyRoutingPerPartition() {
        KafkaTargetBalancer balancer = new KafkaTargetBalancer(List.of("nodeA:50052", "nodeB:50052", "nodeC:50052"));

        for (int p = 0; p < 128; p++) {
            int initialTarget = balancer.selectTargetIndex(p);
            for (int repeat = 0; repeat < 100; repeat++) {
                assertEquals(initialTarget, balancer.selectTargetIndex(p),
                        "Партиция " + p + " обязана сохранять привязку к узлу " + initialTarget);
            }
        }
    }

    @Test
    @DisplayName("Failover: при сбое узла #1 он исключается из ротации, а партиции распределяются по оставшимся 3 узлам")
    void shouldFailoverPartitionsWhenNodeCrashes() {
        List<String> endpoints = List.of(
                "agent-1:50052",
                "agent-2:50052",
                "agent-3:50052",
                "agent-4:50052"
        );
        KafkaTargetBalancer balancer = new KafkaTargetBalancer(endpoints, 5000L);

        // Симулируем падение узла #1
        balancer.markFailed(1, new RuntimeException("Connection reset by peer"));
        assertFalse(balancer.isHealthy(1));
        assertEquals(List.of(0, 2, 3), balancer.getHealthyIndices());

        // Распределяем 128 партиций
        Set<Integer> assignedTargets = new HashSet<>();
        for (int p = 0; p < 128; p++) {
            int target = balancer.selectTargetIndex(p);
            assertNotEquals(1, target, "Сбойный узел #1 не должен получать партиции!");
            assignedTargets.add(target);
        }

        assertEquals(Set.of(0, 2, 3), assignedTargets);

        // Восстановление узла
        balancer.markHealthy(1);
        assertTrue(balancer.isHealthy(1));
        assertEquals(4, balancer.getHealthyIndices().size());

        // После восстановления узел снова получает свои партиции
        boolean node1Received = false;
        for (int p = 0; p < 128; p++) {
            if (balancer.selectTargetIndex(p) == 1) {
                node1Received = true;
                break;
            }
        }
        assertTrue(node1Received, "Восстановленный узел #1 должен снова получать трафик");
    }

    @Test
    @DisplayName("Экстренный сброс карантина: если все узлы упали, балансировщик не блокируется намертво")
    void shouldRecoverIfAllNodesQuarantined() {
        KafkaTargetBalancer balancer = new KafkaTargetBalancer(List.of("node1:50052", "node2:50052"));

        balancer.markFailed(0, new RuntimeException("err"));
        balancer.markFailed(1, new RuntimeException("err"));

        // Должен произойти emergency reset и вернуться оба узла
        List<Integer> healthy = balancer.getHealthyIndices();
        assertEquals(2, healthy.size());
        assertDoesNotThrow(() -> balancer.selectTargetIndex(5));
    }
}
