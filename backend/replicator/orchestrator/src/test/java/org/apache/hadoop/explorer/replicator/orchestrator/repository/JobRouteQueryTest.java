package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Выборка заданий по маршруту.
 *
 * <p>Сценарии аварийного переключения искали маршрут перебором всей таблицы заданий. Запрос
 * обязан повторять правило сравнения кластеров из кода: без регистра, дефисов и подчеркиваний,
 * но строго по совпадению — {@code dc1} и {@code dc10} разные кластеры, и спутать их значит
 * развернуть репликацию не туда.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Задания: выборка по маршруту")
class JobRouteQueryTest {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @BeforeEach
    void setUp() {
        jobRepository.deleteAll();
        hmsJobRepository.deleteAll();

        jobRepository.saveAll(List.of(
                job("route-direct", "dc-1", "dc2"),
                job("route-reverse", "dc2", "dc-1"),
                job("route-underscore", "DC_1", "DC2"),
                job("route-neighbour", "dc10", "dc2"),
                job("route-other-target", "dc-1", "dc3"),
                job("route-foreign", "dc3", "dc4")
        ));
        hmsJobRepository.saveAll(List.of(
                hmsJob("hms-route-direct", "dc-1", "dc2", "agent-a"),
                hmsJob("hms-route-foreign", "dc3", "dc4", "agent-b"),
                hmsJob("hms-route-same-agent", "dc2", "dc1", "agent-a")
        ));
    }

    private JobEntity job(String id, String source, String target) {
        JobEntity job = new JobEntity();
        job.setId(id);
        job.setSourceClusterId(source);
        job.setTargetClusterId(target);
        job.setSourcePath("/warehouse/" + id);
        job.setTargetPath("/backup/warehouse/" + id);
        job.setStatus("RUNNING");
        job.setCreatedBy("tester");
        job.setExecutionPrincipal("tester@REALM.LOCAL");
        return job;
    }

    private HmsReplicationJobEntity hmsJob(String id, String source, String target, String agentId) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId(id);
        job.setSourceClusterId(source);
        job.setTargetClusterId(target);
        job.setSourceDbName("analytics");
        job.setTargetDbName("analytics_replica");
        job.setStatus("ACTIVE");
        job.setCreatedBy("tester");
        job.setAssignedAgentId(agentId);
        return job;
    }

    @Test
    @DisplayName("Дефисы, подчеркивания и регистр при сравнении кластеров не учитываются")
    void routeShouldIgnoreDashesUnderscoresAndCase() {
        List<String> found = jobRepository.findByNormalizedRoute("dc1", "dc2").stream()
                .map(JobEntity::getId).sorted().toList();

        assertEquals(List.of("route-direct", "route-underscore"), found);
    }

    @Test
    @DisplayName("Похожий идентификатор кластера в маршрут не попадает")
    void routeShouldNotMatchSimilarClusterId() {
        List<String> found = jobRepository.findByNormalizedRoute("dc1", "dc2").stream()
                .map(JobEntity::getId).toList();

        assertFalse(found.contains("route-neighbour"), "dc10 — другой кластер, а не dc1");
    }

    @Test
    @DisplayName("Тот же источник с другим приемником в маршрут не попадает")
    void routeShouldRespectTargetCluster() {
        List<String> found = jobRepository.findByNormalizedRoute("dc1", "dc2").stream()
                .map(JobEntity::getId).toList();

        assertFalse(found.contains("route-other-target"),
                "Маршрут dc1 ➔ dc2 не включает задание dc-1 ➔ dc3");
    }

    @Test
    @DisplayName("Направление маршрута учитывается")
    void routeShouldRespectDirection() {
        assertEquals(List.of("route-reverse"),
                jobRepository.findByNormalizedRoute("dc2", "dc1").stream().map(JobEntity::getId).toList(),
                "Обратный маршрут должен отбираться отдельно от прямого");
    }

    @Test
    @DisplayName("Маршрут задач HMS отбирается тем же правилом")
    void hmsRouteShouldUseTheSameRule() {
        assertEquals(List.of("hms-route-direct"),
                hmsJobRepository.findByNormalizedRoute("dc1", "dc2").stream()
                        .map(HmsReplicationJobEntity::getId).toList());
    }

    @Test
    @DisplayName("Задачи HMS выбираются по назначенному агенту")
    void hmsJobsShouldBeSelectableByAgent() {
        assertEquals(List.of("hms-route-direct", "hms-route-same-agent"),
                hmsJobRepository.findByAssignedAgentId("agent-a").stream()
                        .map(HmsReplicationJobEntity::getId).sorted().toList());
        assertTrue(hmsJobRepository.findByAssignedAgentId("agent-missing").isEmpty());
    }
}
