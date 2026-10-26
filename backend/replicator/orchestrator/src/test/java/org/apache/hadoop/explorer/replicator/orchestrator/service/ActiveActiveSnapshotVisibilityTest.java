package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyStopRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.DrSnapshotRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Видимость состояния Kill-Switch между узлами active-active.
 *
 * <p>Снимки ограждения жили в {@code ConcurrentHashMap} узла и читались из БД только в
 * {@code @PostConstruct}. За балансировщиком это означало, что DR-статус «мигал» в
 * зависимости от того, какой узел ответил на запрос, а откат ограждения на другом узле был
 * невозможен: он просто не знал о снимке.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Active-active: состояние Kill-Switch видно всем узлам")
class ActiveActiveSnapshotVisibilityTest {

    @Autowired
    private DisasterRecoveryService drService;

    @Autowired
    private DrSnapshotRepository snapshotRepository;

    @Autowired
    private org.apache.hadoop.explorer.replicator.orchestrator.repository.BandwidthLimitRepository limitRepository;

    @BeforeEach
    void setUp() {
        snapshotRepository.deleteAll();
        limitRepository.deleteAll();
    }

    @Test
    @DisplayName("Снимок ограждения, созданный другим узлом, виден в DR-статусе")
    void testSnapshotCreatedElsewhereIsVisibleInStatus() {
        // Узел A выставил ограждение: в БД появилась запись
        drService.emergencyStop(new DrEmergencyStopRequest("dc1", "авария ЦОД", true), "operator");
        assertFalse(snapshotRepository.findAll().isEmpty(), "Снимок должен быть в БД");

        // Узел B не держит снимок в памяти — очищаем локальное состояние,
        // оставляя запись в базе
        drService.forgetLocalSnapshotsForTesting();

        var status = drService.getDrStatus();
        boolean anyFenced = status.datacenters().stream().anyMatch(dc -> dc.isFenced());
        assertTrue(anyFenced,
                "Узел, не создававший ограждение, обязан увидеть его по данным БД: "
                        + "иначе DR-статус зависит от того, какой узел ответил");
    }

    @Test
    @DisplayName("Откат ограждения работает на узле, который его не создавал")
    void testRollbackWorksOnAnotherNode() {
        drService.emergencyStop(new DrEmergencyStopRequest("dc1", "авария ЦОД", true), "operator");
        drService.forgetLocalSnapshotsForTesting();

        var response = drService.rollbackEmergencyStop(
                new org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyRollbackRequest(
                        "dc1", true, true, true),
                "operator");

        assertTrue(response.success(),
                "Откат обязан работать на любом узле: снимок берется из БД, а не из памяти");
        assertTrue(snapshotRepository.findAll().isEmpty(),
                "После отката снимок должен быть удален из БД");
    }

    @Test
    @DisplayName("Откат несуществующего ограждения не задевает чужой кластер")
    void testRollbackOfUnknownClusterKeepsOtherFencing() {
        drService.emergencyStop(new DrEmergencyStopRequest("dc1", "авария ЦОД", true), "operator");

        var response = drService.rollbackEmergencyStop(
                new org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyRollbackRequest(
                        "dc2", true, true, true),
                "operator");

        assertFalse(response.success(),
                "Для кластера без снимка откат обязан вернуть ошибку, а не снять чужое ограждение");
        assertFalse(snapshotRepository.findAll().isEmpty(),
                "Ограждение dc1 должно остаться в силе");
    }

    @Test
    @DisplayName("Kill-Switch без сетевого ограждения виден узлу, который его не включал")
    void testKillSwitchWithoutNetworkFencingIsVisibleOnAnotherNode() {
        // Задачи остановлены, но канал не перекрыт: в БД есть снимок и ни одного нулевого лимита
        drService.emergencyStop(new DrEmergencyStopRequest("dc1", "ошибка выкатки", false), "operator");
        assertFalse(snapshotRepository.findAll().isEmpty(), "Снимок должен быть в БД");
        assertTrue(limitRepository.findAll().stream().noneMatch(l -> l.getLimitBytesPerSec() == 0L),
                "Сетевого ограждения в этом сценарии нет");

        // Узел B: своего снимка он не создавал
        drService.forgetLocalSnapshotsForTesting();

        var summary = drService.getDrStatus().summary();
        assertTrue(summary.killSwitchActive(),
                "Узел, не включавший Kill-Switch, обязан показывать его активным: "
                        + "иначе оператор видит разное в зависимости от того, какой узел ответил");
        assertEquals("dc1", summary.fencedClusterId(), "Кластер под ограждением берется из БД");
        assertEquals("ошибка выкатки", summary.lastEmergencyReason(), "Причина ограждения берется из БД");
    }

    @Test
    @DisplayName("Снятое на другом узле ограждение перестает считаться активным")
    void testFencingRemovedElsewhereStopsBeingActive() {
        drService.emergencyStop(new DrEmergencyStopRequest("dc1", "ошибка выкатки", false), "operator");

        // Узел A снял ограждение напрямую в БД — локальная память узла B об этом не знает
        snapshotRepository.deleteAll();

        var summary = drService.getDrStatus().summary();
        assertFalse(summary.killSwitchActive(),
                "Снимок удален из БД: узел обязан перестать считать Kill-Switch активным");
    }
}
