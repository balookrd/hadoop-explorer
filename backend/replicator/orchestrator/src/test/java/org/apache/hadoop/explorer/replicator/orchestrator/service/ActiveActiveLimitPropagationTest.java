package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyStopRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.BandwidthLimitRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Распространение лимитов и ограждения между узлами active-active.
 *
 * <p>Механизм персистентности лимитов существует ({@code persistAndApplyLimit} плюс
 * {@code syncLimitsFromDatabase} раз в пять секунд), но Kill-Switch и ручная смена полосы
 * писали прямо в {@code ConcurrentHashMap} своего узла. За балансировщиком это означало, что
 * ограждение применялось только на узле, принявшем запрос, а остальные продолжали выдавать
 * токены агентам и показывать {@code killSwitchActive=false}.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Active-active: лимиты и ограждение видны всем узлам")
class ActiveActiveLimitPropagationTest {

    @Autowired
    private DisasterRecoveryService drService;

    @Autowired
    private BandwidthLimitRepository limitRepository;

    @Autowired
    private ReplicatorProperties properties;

    @BeforeEach
    void setUp() {
        limitRepository.deleteAll();
    }

    /** Второй узел кластера: своя память, та же база. */
    private TopologyRegistry secondNodeRegistry() {
        TopologyRegistry registry = new TopologyRegistry(properties);
        TokenBucketThrottler throttler = new TokenBucketThrottler(properties, registry);
        OrchestratorClusterCoordinator coordinator =
                new OrchestratorClusterCoordinator(null, limitRepository, throttler, registry, null);
        coordinator.syncLimitsFromDatabase();
        return registry;
    }

    @Test
    @DisplayName("Сетевое ограждение Kill-Switch доходит до второго узла")
    void testFencingReachesSecondNode() {
        DrEmergencyStopRequest request = new DrEmergencyStopRequest("dc1", "авария ЦОД", true);

        drService.emergencyStop(request, "operator");

        TopologyRegistry secondNode = secondNodeRegistry();
        var fencedLimit = secondNode.getDcLimit("dc1", "dc2");
        assertTrue(fencedLimit.isPresent(),
                "Второй узел не узнал об ограждении: он продолжит выдавать токены агентам");
        assertEquals(0L, fencedLimit.get(),
                "На втором узле канал dc1 -> dc2 обязан быть перекрыт");
    }

    @Test
    @DisplayName("Ограждение сохраняется в БД, а не только в памяти узла")
    void testFencingIsPersisted() {
        drService.emergencyStop(new DrEmergencyStopRequest("dc1", "авария ЦОД", true), "operator");

        assertFalse(limitRepository.findAll().isEmpty(),
                "Ограждение должно попадать в БД: иначе перезапуск узла снимает его молча");
    }

    @Test
    @DisplayName("Откат Kill-Switch возвращает прежние лимиты на обоих узлах")
    void testRollbackRestoresLimitsEverywhere() {
        drService.emergencyStop(new DrEmergencyStopRequest("dc1", "авария ЦОД", true), "operator");
        assertEquals(0L, secondNodeRegistry().getDcLimit("dc1", "dc2").orElseThrow());

        drService.rollbackEmergencyStop(
                new org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyRollbackRequest("dc1", true, true, true),
                "operator");

        var restored = secondNodeRegistry().getDcLimit("dc1", "dc2");
        assertTrue(restored.isEmpty() || restored.get() > 0L,
                "После отката ограждения канал должен быть открыт и на втором узле, получено: " + restored);
    }
}
