package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Строгость привязки HMS-задачи к кластеру агента.
 *
 * <p>Сравнение подстрокой позволяло агенту соседнего кластера (например, `prod-dc1-backup`)
 * забрать задачу кластера `prod-dc1`: агент читал CDC чужого метастора и применял DDL
 * в чужом датацентре.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("HMS: строгая привязка задачи к кластеру")
public class HmsClusterMatchingTest {

    @Autowired
    private HmsCoordinatorService coordinatorService;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @Autowired
    private AgentRegistry agentRegistry;

    @BeforeEach
    void setup() {
        hmsJobRepository.deleteAll();
        agentRegistry.clear();
        // Живой HMS-агент целевого кластера: без него задача уходит в WAITING_FOR_AGENTS
        agentRegistry.register(new AgentRegisterRequest(
                "agent-dc2-1", "prod-dc2", "all", "node-dc2:50051", "node-dc2", 100.0),
                "test-orchestrator-agent-secret-0123456789");
    }

    private void createPendingJob(String sourceClusterId) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId("hms-job-" + sourceClusterId);
        job.setSourceClusterId(sourceClusterId);
        job.setTargetClusterId("prod-dc2");
        job.setSourceDbName("analytics");
        job.setTargetDbName("analytics");
        job.setStatus("QUEUED");
        job.setCreatedBy("tester");
        hmsJobRepository.save(job);
    }

    @Test
    @DisplayName("Агент кластера с похожим именем не получает чужую задачу")
    void testAgentOfSimilarlyNamedClusterGetsNothing() {
        createPendingJob("prod-dc1");

        List<HmsPendingJobDto> claimed = coordinatorService.getPendingJobsForCluster("prod-dc1-backup", "agent-backup-1");

        assertTrue(claimed.isEmpty(),
                "Агент кластера 'prod-dc1-backup' забрал задачу кластера 'prod-dc1': CDC читался бы из чужого метастора");
    }

    @Test
    @DisplayName("Агент своего кластера получает задачу")
    void testAgentOfOwnClusterClaimsJob() {
        createPendingJob("prod-dc1");

        List<HmsPendingJobDto> claimed = coordinatorService.getPendingJobsForCluster("prod-dc1", "agent-dc1-1");

        assertEquals(1, claimed.size(), "Агент своего кластера обязан получить задачу");
    }

    @Test
    @DisplayName("Различия в дефисах и регистре идентификатора допустимы")
    void testNormalizedIdentifiersStillMatch() {
        createPendingJob("prod-dc1");

        List<HmsPendingJobDto> claimed = coordinatorService.getPendingJobsForCluster("PROD_DC1", "agent-dc1-1");

        assertEquals(1, claimed.size(), "Нормализация регистра и разделителей должна сохраниться");
    }
}
