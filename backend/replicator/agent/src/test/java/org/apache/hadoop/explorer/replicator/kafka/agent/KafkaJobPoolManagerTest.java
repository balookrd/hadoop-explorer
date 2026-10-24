package org.apache.hadoop.explorer.replicator.kafka.agent;

import org.apache.hadoop.explorer.replicator.generated.control.KafkaJobItemProto;
import org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Жизненный цикл пулов консьюмеров по задачам репликации.
 *
 * <p>Агент держал один пул с одной consumer-группой на все задачи ЦОД: топик, присутствующий
 * в двух задачах, вычитывался один раз, и вторая задача не получала данных вовсе. Настройки
 * задач (размер батча, число потоков, полоса) тоже смешивались — применялась одна на всех.
 * Каждая задача должна обслуживаться своим пулом со своей группой.
 */
@DisplayName("Kafka: пул и consumer-группа на каждую задачу")
class KafkaJobPoolManagerTest {

    /** Запрошенные у фабрики параметры пула: по ним видно изоляцию задач. */
    private record PoolRequest(String groupId, List<String> topics, List<String> endpoints,
                               int consumerThreads, int batchSize, String targetDc) {}

    private final List<PoolRequest> requests = new ArrayList<>();
    private final Map<String, KafkaBatchConsumerPool> pools = new ConcurrentHashMap<>();
    private KafkaJobPoolManager manager;

    @BeforeEach
    void setUp() {
        requests.clear();
        pools.clear();
        manager = new KafkaJobPoolManager("dc1", (spec) -> {
            requests.add(new PoolRequest(spec.groupId(), spec.topics(), spec.targetEndpoints(),
                    spec.consumerThreads(), spec.batchSize(), spec.targetDc()));
            KafkaBatchConsumerPool pool = mock(KafkaBatchConsumerPool.class);
            pools.put(spec.jobId(), pool);
            return pool;
        });
    }

    private static KafkaJobItemProto job(String jobId, String targetDc, List<String> topics,
                                         List<String> endpoints, int threads, int batchSize) {
        return KafkaJobItemProto.newBuilder()
                .setJobId(jobId)
                .setName(jobId)
                .setSourceDc("dc1")
                .setTargetDc(targetDc)
                .setDirection("UNIDIRECTIONAL")
                .addAllTopics(topics)
                .addAllTargetEndpoints(endpoints)
                .setConsumerThreads(threads)
                .setBatchSize(batchSize)
                .setStatus("ACTIVE")
                .build();
    }

    @Test
    @DisplayName("Каждая задача получает свою consumer-группу")
    void testEachJobGetsOwnConsumerGroup() {
        manager.sync(List.of(
                job("job-orders", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 4, 2000),
                job("job-audit", "dc3", List.of("audit"), List.of("kafka-dc3:50052"), 2, 500)));

        assertEquals(2, requests.size(), "Для двух задач должно быть создано два пула");
        List<String> groups = requests.stream().map(PoolRequest::groupId).toList();
        assertEquals(2, groups.stream().distinct().count(),
                "Группы задач обязаны различаться, иначе задачи делят вычитку: " + groups);
        assertTrue(groups.stream().anyMatch(g -> g.contains("job-orders")));
        assertTrue(groups.stream().anyMatch(g -> g.contains("job-audit")));
    }

    @Test
    @DisplayName("Один топик в двух задачах читается каждой задачей отдельно")
    void testSharedTopicIsReadByBothJobs() {
        manager.sync(List.of(
                job("job-to-dc2", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 4, 2000),
                job("job-to-dc3", "dc3", List.of("orders"), List.of("kafka-dc3:50052"), 4, 2000)));

        assertEquals(2, requests.size());
        assertEquals(2, requests.stream().map(PoolRequest::groupId).distinct().count(),
                "Общий топик требует отдельной группы на задачу, иначе вторая задача не получит данных");
        requests.forEach(r -> assertEquals(List.of("orders"), r.topics()));
    }

    @Test
    @DisplayName("Настройки задачи не смешиваются между пулами")
    void testPerJobSettingsAreIsolated() {
        manager.sync(List.of(
                job("job-big", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 8, 5000),
                job("job-small", "dc3", List.of("audit"), List.of("kafka-dc3:50052"), 2, 500)));

        PoolRequest big = requests.stream().filter(r -> r.groupId().contains("job-big")).findFirst().orElseThrow();
        PoolRequest small = requests.stream().filter(r -> r.groupId().contains("job-small")).findFirst().orElseThrow();

        assertEquals(8, big.consumerThreads());
        assertEquals(5000, big.batchSize());
        assertEquals(List.of("kafka-dc2:50052"), big.endpoints());
        assertEquals("dc2", big.targetDc(), "Peer-DC нужен фильтру петель и должен быть свой у каждой задачи");

        assertEquals(2, small.consumerThreads());
        assertEquals(500, small.batchSize());
        assertEquals(List.of("kafka-dc3:50052"), small.endpoints());
        assertEquals("dc3", small.targetDc());
    }

    @Test
    @DisplayName("Повторная синхронизация не пересоздает существующие пулы")
    void testRepeatedSyncKeepsPools() {
        List<KafkaJobItemProto> jobs = List.of(
                job("job-orders", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 4, 2000));

        manager.sync(jobs);
        manager.sync(jobs);

        assertEquals(1, requests.size(), "Пул задачи не должен пересоздаваться на каждом опросе");
        verify(pools.get("job-orders"), times(1)).start();
    }

    @Test
    @DisplayName("Исчезнувшая задача останавливает свой пул")
    void testRemovedJobStopsItsPool() {
        manager.sync(List.of(
                job("job-orders", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 4, 2000),
                job("job-audit", "dc3", List.of("audit"), List.of("kafka-dc3:50052"), 2, 500)));
        KafkaBatchConsumerPool auditPool = pools.get("job-audit");

        manager.sync(List.of(
                job("job-orders", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 4, 2000)));

        verify(auditPool, times(1)).close();
        verify(pools.get("job-orders"), never()).close();
    }

    @Test
    @DisplayName("Пустой список задач останавливает все пулы")
    void testEmptyJobListStopsEverything() {
        manager.sync(List.of(
                job("job-orders", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 4, 2000)));
        KafkaBatchConsumerPool pool = pools.get("job-orders");

        manager.sync(List.of());

        verify(pool, times(1)).close();
        assertTrue(manager.activeJobIds().isEmpty(), "После остановки задач активных пулов не остается");
    }

    @Test
    @DisplayName("Смена топиков задачи обновляет подписку без пересоздания пула")
    void testTopicChangeUpdatesSubscription() {
        manager.sync(List.of(
                job("job-orders", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 4, 2000)));
        KafkaBatchConsumerPool pool = pools.get("job-orders");

        manager.sync(List.of(
                job("job-orders", "dc2", List.of("orders", "payments"), List.of("kafka-dc2:50052"), 4, 2000)));

        assertEquals(1, requests.size(), "Пул не пересоздается из-за изменения списка топиков");
        verify(pool).updateSubscribedTopics(List.of("orders", "payments"));
    }

    @Test
    @DisplayName("Закрытие менеджера останавливает все пулы")
    void testCloseStopsAllPools() {
        manager.sync(List.of(
                job("job-orders", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 4, 2000),
                job("job-audit", "dc3", List.of("audit"), List.of("kafka-dc3:50052"), 2, 500)));

        manager.close();

        pools.values().forEach(pool -> verify(pool, times(1)).close());
        assertTrue(manager.activeJobIds().isEmpty());
    }
}
