package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Строгость сопоставления кластеров, ЦОД и зеркальных путей.
 *
 * <p>Сравнение подстрокой и по суффиксу пути давало ложные совпадения: ограждение `dc1`
 * считало «тем же самым» датацентр `dc10` и не ограждало его, а произвольная задача с похожим
 * хвостом пути выглядела как уже существующая обратная репликация — из-за чего реверс при
 * failover не создавался вовсе.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Строгое сопоставление кластеров и ЦОД")
class ClusterMatchingStrictnessTest {

    @Autowired
    private DisasterRecoveryService drService;

    private static JobEntity job(String sourceCluster, String targetCluster, String sourcePath, String targetPath) {
        JobEntity job = new JobEntity();
        job.setSourceClusterId(sourceCluster);
        job.setTargetClusterId(targetCluster);
        job.setSourcePath(sourcePath);
        job.setTargetPath(targetPath);
        return job;
    }

    @Test
    @DisplayName("ЦОД dc1 не совпадает с похожим по имени dc10")
    void testSimilarDcIdsDoNotMatch() {
        assertFalse(drService.matchesDc("dc10", "dc1"),
                "dc10 не является dc1: при ограждении dc1 он остался бы без ограждения");
        assertFalse(drService.matchesDc("dc1", "dc10"), "dc1 не является dc10");
    }

    @Test
    @DisplayName("Кластер соотносится с ЦОД по явной привязке из топологии")
    void testClusterBelongsToDcByTopology() {
        assertTrue(drService.matchesDc("prod-a", "dc1"), "prod-a объявлен в топологии как кластер dc1");
        assertTrue(drService.matchesDc("prod-b", "dc10"), "prod-b объявлен в топологии как кластер dc10");
        assertFalse(drService.matchesDc("prod-a", "dc10"), "prod-a не относится к dc10");
        assertFalse(drService.matchesDc("prod-b", "dc1"), "prod-b не относится к dc1");
    }

    @Test
    @DisplayName("Идентификатор ЦОД совпадает сам с собой")
    void testSameDcIdMatches() {
        assertTrue(drService.matchesDc("dc1", "dc1"));
        assertTrue(drService.matchesDc("DC1", "dc1"), "регистр идентификатора не важен");
    }

    @Test
    @DisplayName("Кластер неизвестной топологии не относится ни к одному ЦОД")
    void testUnknownClusterDoesNotMatchDc() {
        assertFalse(drService.matchesDc("prod-a-backup", "dc1"),
                "Похожее имя без явной привязки в топологии не делает кластер частью ЦОД");
    }

    @Test
    @DisplayName("Зеркальной считается только задача с инверсией обоих путей")
    void testOnlyFullInversionCountsAsMirror() {
        JobEntity direct = job("prod-a", "prod-b", "/warehouse/sales", "/backup/warehouse/sales");
        JobEntity properReverse = job("prod-b", "prod-a", "/backup/warehouse/sales", "/warehouse/sales");
        JobEntity partialMatch = job("prod-b", "prod-a", "/backup/warehouse/sales", "/other/place");
        JobEntity suffixMatch = job("prod-b", "prod-a", "/sales", "/warehouse/sales");

        assertTrue(drService.matchesReversePath(properReverse, direct),
                "Полная инверсия путей — это и есть обратная репликация");
        assertFalse(drService.matchesReversePath(partialMatch, direct),
                "Совпадение одного пути не делает задачу зеркальной: иначе реверс не создается");
        assertFalse(drService.matchesReversePath(suffixMatch, direct),
                "Совпадение по суффиксу пути не является зеркальностью");
    }

    @Test
    @DisplayName("Совпадающие пути на обоих кластерах тоже считаются зеркалом")
    void testIdenticalPathsOnBothClustersCountAsMirror() {
        JobEntity direct = job("prod-a", "prod-b", "/warehouse/sales", "/warehouse/sales");
        JobEntity reverse = job("prod-b", "prod-a", "/warehouse/sales", "/warehouse/sales");

        assertTrue(drService.matchesReversePath(reverse, direct));
    }
}
