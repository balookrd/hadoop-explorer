package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ограждение отчетов о прогрессе HMS-задачи.
 *
 * <p>Ключевая гарантия (SPLIT-BRAIN): отчет принимается только от агента, который держит
 * аренду задачи, а позиция в потоке событий двигается только вперед. Иначе «подвисший»
 * агент после failover досылает отчет, снова становится владельцем и перезаписывает позицию
 * устаревшим значением — два агента начинают читать CDC одной базы, дублируя DDL.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("HMS: ограждение отчетов о прогрессе")
public class HmsProgressFencingTest {

    @Autowired
    private HmsCoordinatorService coordinatorService;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @BeforeEach
    void setup() {
        hmsJobRepository.deleteAll();
    }

    private HmsReplicationJobEntity jobOwnedBy(String ownerAgentId, long lastProcessedEventId) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId("hms-job-fencing-1");
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName("analytics");
        job.setTargetDbName("analytics");
        job.setStatus("ACTIVE");
        job.setAssignedAgentId(ownerAgentId);
        job.setLeaseExpiresAt(Instant.now().plusSeconds(60));
        job.setLastProcessedEventId(lastProcessedEventId);
        job.setCreatedBy("tester");
        return hmsJobRepository.save(job);
    }

    private static HmsProgressReportRequest report(String agentId, Long lastProcessedEventId) {
        return new HmsProgressReportRequest("ACTIVE", 10, 10, 100, 100,
                lastProcessedEventId, null, 0L, "Синхронизировано", List.of(), agentId);
    }

    @Test
    @DisplayName("Отчет от агента, не владеющего арендой, отклоняется")
    void testReportFromNonOwnerIsRejected() {
        HmsReplicationJobEntity job = jobOwnedBy("agent-new-owner", 500L);

        boolean accepted = coordinatorService.updateJobProgress(job.getId(), report("agent-zombie", 490L));

        assertFalse(accepted, "Отчет от агента без аренды обязан быть отклонен");
        HmsReplicationJobEntity reloaded = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertEquals("agent-new-owner", reloaded.getAssignedAgentId(),
                "Зомби-агент не должен возвращать себе владение задачей");
        assertEquals(500L, reloaded.getLastProcessedEventId(),
                "Позиция в потоке событий не должна откатываться назад");
    }

    @Test
    @DisplayName("Владелец аренды не может откатить позицию назад")
    void testOwnerCannotMovePositionBackwards() {
        HmsReplicationJobEntity job = jobOwnedBy("agent-owner", 500L);

        coordinatorService.updateJobProgress(job.getId(), report("agent-owner", 300L));

        HmsReplicationJobEntity reloaded = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertEquals(500L, reloaded.getLastProcessedEventId(),
                "lastProcessedEventId обязан быть монотонным: откат назад переигрывает уже примененные события");
    }

    @Test
    @DisplayName("Отчет владельца с продвижением позиции принимается")
    void testOwnerReportAdvancesPosition() {
        HmsReplicationJobEntity job = jobOwnedBy("agent-owner", 500L);

        boolean accepted = coordinatorService.updateJobProgress(job.getId(), report("agent-owner", 777L));

        assertTrue(accepted, "Отчет законного владельца должен приниматься");
        HmsReplicationJobEntity reloaded = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertEquals(777L, reloaded.getLastProcessedEventId());
    }

    @Test
    @DisplayName("Отчет без agentId принимается: обратная совместимость со старыми агентами")
    void testReportWithoutAgentIdIsAccepted() {
        HmsReplicationJobEntity job = jobOwnedBy("agent-owner", 500L);

        boolean accepted = coordinatorService.updateJobProgress(job.getId(), report(null, 600L));

        assertTrue(accepted, "Агент, не присылающий agentId, не должен блокироваться");
        assertEquals(600L, hmsJobRepository.findById(job.getId()).orElseThrow().getLastProcessedEventId());
    }

    @Test
    @DisplayName("Задача без владельца (после failover) отклоняет отчет агента")
    void testUnownedJobRejectsAgentReport() {
        // Failover и rebootstrap снимают владельца; отчет прежнего агента не должен вернуть
        // ему задачу — новый владелец захватывает ее только через опрос
        HmsReplicationJobEntity job = jobOwnedBy(null, 500L);

        boolean accepted = coordinatorService.updateJobProgress(job.getId(), report("agent-zombie", 42L));

        assertFalse(accepted, "Отчет для задачи без владельца должен отклоняться");
        HmsReplicationJobEntity reloaded = hmsJobRepository.findById(job.getId()).orElseThrow();
        assertNull(reloaded.getAssignedAgentId(), "Зомби-агент не должен становиться владельцем");
        assertEquals(500L, reloaded.getLastProcessedEventId());
    }
}
