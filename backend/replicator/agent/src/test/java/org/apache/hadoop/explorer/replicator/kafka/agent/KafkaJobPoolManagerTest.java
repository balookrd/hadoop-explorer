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
                               int consumerThreads, int batchSize, String targetDc,
                               int offsetSyncIntervalSec, boolean schemaTranscoding,
                               String sourceSrUrl, String targetSrUrl) {}

    private final List<PoolRequest> requests = new ArrayList<>();
    private final Map<String, KafkaBatchConsumerPool> pools = new ConcurrentHashMap<>();
    private KafkaJobPoolManager manager;

    @BeforeEach
    void setUp() {
        requests.clear();
        pools.clear();
        manager = new KafkaJobPoolManager("dc1", (spec) -> {
            requests.add(new PoolRequest(spec.groupId(), spec.topics(), spec.targetEndpoints(),
                    spec.consumerThreads(), spec.batchSize(), spec.targetDc(),
                    spec.offsetSyncIntervalSec(), spec.schemaTranscoding(),
                    spec.sourceSchemaRegistryUrl(), spec.targetSchemaRegistryUrl()));
            KafkaBatchConsumerPool pool = mock(KafkaBatchConsumerPool.class);
            pools.put(spec.jobId(), pool);
            // Перенос смещений в этих сценариях не участвует: проверяется жизненный цикл пулов
            return new KafkaJobPoolManager.JobPipeline(pool, null);
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
    @DisplayName("Интервал переноса смещений и транскодинг берутся из самой задачи")
    void testPerJobOffsetSyncAndTranscodingReachThePool() {
        KafkaJobItemProto withSettings = job("job-settings", "dc2", List.of("orders"),
                List.of("dc2-a:50052"), 2, 500).toBuilder()
                .setOffsetSyncIntervalSec(30)
                .setSchemaTranscoding(true)
                .build();
        KafkaJobItemProto withoutSettings = job("job-plain", "dc2", List.of("audit"),
                List.of("dc2-a:50052"), 2, 500);

        manager.sync(List.of(withSettings, withoutSettings));

        PoolRequest settings = requests.stream()
                .filter(r -> r.groupId().endsWith("job-settings")).findFirst().orElseThrow();
        PoolRequest plain = requests.stream()
                .filter(r -> r.groupId().endsWith("job-plain")).findFirst().orElseThrow();

        assertEquals(30, settings.offsetSyncIntervalSec(),
                "Интервал переноса смещений задается на задачу, иначе поле формы декоративно");
        assertTrue(settings.schemaTranscoding(),
                "Флаг транскодинга задается на задачу");
        assertEquals(0, plain.offsetSyncIntervalSec(),
                "Задача без интервала не включает перенос смещений");
        assertFalse(plain.schemaTranscoding(),
                "Задача без флага не включает транскодинг: настройки задач не смешиваются");
    }

    @Test
    @DisplayName("Реестр схем приезжает в задаче из топологии и доходит до пула")
    void testSchemaRegistryFromJobReachesThePool() {
        KafkaJobItemProto withRegistry = job("job-sr", "dc2", List.of("orders"),
                List.of("dc2-a:50052"), 2, 500).toBuilder()
                .setSchemaTranscoding(true)
                .setSourceSchemaRegistryUrl("http://sr-dc1:8081")
                .setSourceSchemaRegistryType("HORTONWORKS")
                .setTargetSchemaRegistryUrl("http://sr-dc2:8081")
                .setTargetSchemaRegistryType("CONFLUENT")
                .build();

        manager.sync(List.of(withRegistry));

        PoolRequest request = requests.get(0);
        assertEquals("http://sr-dc1:8081", request.sourceSrUrl(),
                "Реестр источника обязан дойти до пула: иначе агент берет его из своего "
                        + "окружения, а выбор кластера в интерфейсе ни на что не влияет");
        assertEquals("http://sr-dc2:8081", request.targetSrUrl());
    }

    @Test
    @DisplayName("Смена реестра в топологии пересоздает конвейер задачи")
    void testChangedRegistryRecreatesPipeline() {
        KafkaJobItemProto before = job("job-sr-switch", "dc2", List.of("orders"),
                List.of("dc2-a:50052"), 2, 500).toBuilder()
                .setSchemaTranscoding(true)
                .setSourceSchemaRegistryUrl("http://sr-dc1:8081")
                .setTargetSchemaRegistryUrl("http://sr-dc2:8081")
                .build();
        manager.sync(List.of(before));
        KafkaBatchConsumerPool firstPool = pools.get("job-sr-switch");

        KafkaJobItemProto after = before.toBuilder()
                .setTargetSchemaRegistryUrl("http://sr-dc2-new:8081")
                .build();
        manager.sync(List.of(after));

        verify(firstPool).close();
        assertEquals("http://sr-dc2-new:8081", requests.get(1).targetSrUrl(),
                "Резолвер схем создается вместе с пулом, поэтому смена реестра обязана "
                        + "пересоздать конвейер, а не остаться без эффекта");
    }

    @Test
    @DisplayName("Смена позадачных настроек пересоздает конвейер задачи")
    void testChangedPerJobSettingsRecreatePipeline() {
        KafkaJobItemProto before = job("job-switch", "dc2", List.of("orders"),
                List.of("dc2-a:50052"), 2, 500);
        manager.sync(List.of(before));
        KafkaBatchConsumerPool firstPool = pools.get("job-switch");

        KafkaJobItemProto after = before.toBuilder()
                .setOffsetSyncIntervalSec(15)
                .setSchemaTranscoding(true)
                .build();
        manager.sync(List.of(after));

        verify(firstPool).close();
        assertEquals(2, requests.size(),
                "Транскодер и перенос смещений создаются вместе с пулом, поэтому смена этих "
                        + "настроек обязана пересоздать конвейер, а не остаться без эффекта");
        PoolRequest recreated = requests.get(1);
        assertEquals(15, recreated.offsetSyncIntervalSec());
        assertTrue(recreated.schemaTranscoding());
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

    @Test
    @DisplayName("Перестановка приемников без смены состава не пересоздает пул")
    void testEndpointReorderDoesNotRestartPool() {
        manager.sync(List.of(job("job-orders", "dc2", List.of("orders"),
                List.of("a:50052", "b:50052", "c:50052"), 4, 2000)));
        manager.sync(List.of(job("job-orders", "dc2", List.of("orders"),
                List.of("c:50052", "a:50052", "b:50052"), 4, 2000)));

        assertEquals(1, requests.size(),
                "Оркестратор собирает приемников из хеш-таблицы, порядок между опросами плавает: "
                        + "перезапуск пула на каждую перестановку дает ребаланс всех партиций группы");
        verify(pools.get("job-orders"), never()).close();
    }

    @Test
    @DisplayName("Метрики агента — сумма по пулам задач и не проседают при пересоздании пула")
    void testMetricsAreSummedAndMonotonic() {
        manager.sync(List.of(
                job("job-a", "dc2", List.of("a"), List.of("kafka-dc2:50052"), 1, 100),
                job("job-b", "dc2", List.of("b"), List.of("kafka-dc2:50052"), 1, 100)));
        when(pools.get("job-a").getTotalMessagesReplicated()).thenReturn(700L);
        when(pools.get("job-b").getTotalMessagesReplicated()).thenReturn(300L);
        when(pools.get("job-a").getCurrentLagMs()).thenReturn(50L);
        when(pools.get("job-b").getCurrentLagMs()).thenReturn(900L);

        assertEquals(1000L, manager.totalMessagesReplicated());
        assertEquals(900L, manager.maxLagMs());

        // Смена приемников пересоздает пул job-a: его 700 записей должны остаться в сумме
        manager.sync(List.of(
                job("job-a", "dc2", List.of("a"), List.of("kafka-dc2-new:50052"), 1, 100),
                job("job-b", "dc2", List.of("b"), List.of("kafka-dc2:50052"), 1, 100)));

        assertEquals(1000L, manager.totalMessagesReplicated(),
                "Оркестратор считает счетчики накопительными: пересоздание пула не должно их обнулять");
    }

    @Test
    @DisplayName("Снятие лимита (0) и смена доли агента доходят до живого пула")
    void testBandwidthShareChangesIncludingRemoval() {
        KafkaJobItemProto limited = job("job-bw", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 2, 1000)
                .toBuilder().setBandwidthLimitMb(100.0).build();
        manager.sync(List.of(limited));
        KafkaBatchConsumerPool pool = pools.get("job-bw");

        // Отправителей стало больше — доля агента уменьшилась
        manager.sync(List.of(limited.toBuilder().setBandwidthLimitMb(25.0).build()));
        verify(pool).setBandwidthLimitMb(25.0);

        // Лимит задачи снят
        manager.sync(List.of(limited.toBuilder().setBandwidthLimitMb(0.0).build()));
        verify(pool).setBandwidthLimitMb(0.0);
        assertEquals(1, requests.size(), "Смена лимита применяется на живом пуле без пересоздания");
    }

    @Test
    @DisplayName("Стартовая позиция задачи передается в спецификацию пула")
    void testStartPositionReachesPoolSpec() {
        List<String> positions = new ArrayList<>();
        KafkaJobPoolManager local = new KafkaJobPoolManager("dc1", spec -> {
            positions.add(spec.startPosition());
            return new KafkaJobPoolManager.JobPipeline(mock(KafkaBatchConsumerPool.class), null);
        });
        local.sync(List.of(job("job-latest", "dc2", List.of("orders"), List.of("kafka-dc2:50052"), 1, 100)
                .toBuilder().setStartPosition("LATEST").build()));
        assertEquals(List.of("LATEST"), positions);
    }

    @Test
    @DisplayName("Статистика по задачам: своя у каждой задачи и не проседает при пересоздании пула")
    void testJobStatsPerJobAndMonotonic() {
        manager.sync(List.of(
                job("job-a", "dc2", List.of("a"), List.of("kafka-dc2:50052"), 1, 100),
                job("job-b", "dc2", List.of("b"), List.of("kafka-dc2:50052"), 1, 100)));
        when(pools.get("job-a").getTotalMessagesReplicated()).thenReturn(700L);
        when(pools.get("job-b").getTotalMessagesReplicated()).thenReturn(5L);

        Map<String, Long> byJob = new java.util.HashMap<>();
        manager.jobStats().forEach(s -> byJob.put(s.jobId(), s.messagesReplicated()));
        assertEquals(Map.of("job-a", 700L, "job-b", 5L), byJob);

        // Пересоздание пула job-a (сменились приемники): новый пул считает с нуля
        manager.sync(List.of(
                job("job-a", "dc2", List.of("a"), List.of("kafka-dc2-new:50052"), 1, 100),
                job("job-b", "dc2", List.of("b"), List.of("kafka-dc2:50052"), 1, 100)));
        when(pools.get("job-a").getTotalMessagesReplicated()).thenReturn(30L);

        byJob.clear();
        manager.jobStats().forEach(s -> byJob.put(s.jobId(), s.messagesReplicated()));
        assertEquals(730L, byJob.get("job-a"), "Счетчик задачи накопительный и переживает пересоздание пула");
    }

    @Test
    @DisplayName("Консьюмеров на агенте не больше, чем ему достанется партиций")
    void testConsumerThreadsCappedByPartitionsPerAgent() {
        // 128 партиций на 12 агентов — по 11 на агента: 32 потока превратились бы в 21 простой
        assertEquals(11, KafkaJobPoolManager.consumerThreadsFor(32, 128, 12));
        // Заданное число меньше доли — действует заданное
        assertEquals(4, KafkaJobPoolManager.consumerThreadsFor(4, 128, 12));
        // Партиций меньше, чем агентов: каждому хватает одного консьюмера
        assertEquals(1, KafkaJobPoolManager.consumerThreadsFor(8, 6, 12));
        // Неизвестно число партиций — ограничения нет
        assertEquals(8, KafkaJobPoolManager.consumerThreadsFor(8, 0, 12));
        // Неизвестно число отправителей — делим на одного
        assertEquals(16, KafkaJobPoolManager.consumerThreadsFor(32, 16, 0));
    }

    @Test
    @DisplayName("Регистр --dc-id не делит агентов одного ЦОД на разные консьюмер-группы")
    void testDcIdCaseDoesNotSplitGroup() {
        KafkaJobItemProto j = job("job-case", "dc2", List.of("orders"), List.of("r:1"), 1, 100);
        String lower = KafkaJobPoolManager.consumerGroupFor(j, "dc1", "job-case");
        String upper = KafkaJobPoolManager.consumerGroupFor(j, "DC1", "job-case");
        assertEquals("kafka-replicator-dc1-job-case", lower);
        assertEquals(lower, upper, "Агенты ЦОД с флагами dc1 и DC1 обязаны читать одной группой");
        assertEquals("dc2", KafkaJobPoolManager.canonicalLocalDc(j, "DC2"),
                "Для обратного направления ЦОД берется в написании задачи");
    }

    @Test
    @DisplayName("Смена числа потоков или размера батча пересоздает пул задачи")
    void testThreadsAndBatchSizeChangeRestartsPool() {
        manager.sync(List.of(job("job-tune", "dc2", List.of("orders"), List.of("r:1"), 2, 1000)));
        KafkaBatchConsumerPool first = pools.get("job-tune");

        manager.sync(List.of(job("job-tune", "dc2", List.of("orders"), List.of("r:1"), 2, 1000)));
        assertEquals(1, requests.size(), "Без изменений пул не пересоздается");

        manager.sync(List.of(job("job-tune", "dc2", List.of("orders"), List.of("r:1"), 6, 1000)));
        assertEquals(2, requests.size(), "Новое число потоков требует нового пула");
        assertEquals(6, requests.get(1).consumerThreads());
        verify(first).close();

        manager.sync(List.of(job("job-tune", "dc2", List.of("orders"), List.of("r:1"), 6, 5000)));
        assertEquals(3, requests.size(), "Новый размер батча требует нового пула");
        assertEquals(5000, requests.get(2).batchSize());
    }

    @Test
    @DisplayName("Ошибка запуска пула уходит в отчет агента и снимается после успешного запуска")
    void testPoolStartErrorIsReported() {
        java.util.concurrent.atomic.AtomicBoolean fail = new java.util.concurrent.atomic.AtomicBoolean(true);
        KafkaJobPoolManager failing = new KafkaJobPoolManager("dc1", spec -> {
            if (fail.get()) {
                throw new IllegalStateException("не удалось создать пул",
                        new javax.security.auth.login.LoginException("Cannot locate KDC"));
            }
            return new KafkaJobPoolManager.JobPipeline(mock(KafkaBatchConsumerPool.class), null);
        });
        KafkaJobItemProto j = job("job-krb", "dc2", List.of("orders"), List.of("r:1"), 1, 100);

        failing.sync(List.of(j));
        assertEquals(1, failing.jobErrors().size());
        assertEquals("job-krb", failing.jobErrors().get(0).jobId());
        assertTrue(failing.jobErrors().get(0).message().contains("Cannot locate KDC"),
                "В отчет попадает корневая причина: " + failing.jobErrors().get(0).message());

        fail.set(false);
        failing.sync(List.of(j));
        assertTrue(failing.jobErrors().isEmpty(), "После успешного запуска ошибка снимается");

        failing.sync(List.of(job("job-noreceivers", "dc2", List.of("orders"), List.of(), 1, 100)));
        assertEquals(List.of("job-noreceivers"), failing.jobErrors().stream().map(e -> e.jobId()).toList(),
                "Задача без приемников видна как ошибка, а снятая задача из отчета пропадает");
        assertTrue(failing.jobErrors().get(0).message().contains("нет живых приемников"));

        // Работающий пул потерял приемники, затем они вернулись с теми же адресами
        KafkaJobItemProto running = job("job-running", "dc2", List.of("orders"), List.of("r:1"), 1, 100);
        failing.sync(List.of(running));
        failing.sync(List.of(job("job-running", "dc2", List.of("orders"), List.of(), 1, 100)));
        assertEquals(1, failing.jobErrors().size());
        failing.sync(List.of(running));
        assertTrue(failing.jobErrors().isEmpty(), "Вернувшиеся приемники снимают ошибку без пересоздания пула");
    }
}
