package io.github.balookrd.pristan.replicator.orchestrator.schema;

import io.github.balookrd.pristan.replicator.model.AgentRegisterRequest;
import io.github.balookrd.pristan.replicator.model.HmsProgressReportRequest;
import io.github.balookrd.pristan.replicator.orchestrator.config.ReplicatorProperties;
import io.github.balookrd.pristan.replicator.orchestrator.entity.HmsReplicationJobEntity;
import io.github.balookrd.pristan.replicator.orchestrator.hms.service.HmsCoordinatorService;
import io.github.balookrd.pristan.replicator.orchestrator.registry.AgentRegistry;
import io.github.balookrd.pristan.replicator.orchestrator.repository.HmsReplicationJobRepository;
import io.github.balookrd.pristan.replicator.orchestrator.repository.TaskRepository;
import io.github.balookrd.pristan.replicator.orchestrator.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Запросы оркестратора на PostgreSQL — боевой базе.
 *
 * <p>H2 в тестах и в демо прощает то, на чем PostgreSQL падает: например, null-параметр
 * в {@code :p IS NULL OR LOWER(x) = LOWER(:p)} PostgreSQL получает как bytea, и {@code LOWER(bytea)}
 * не существует — запрос авторов для сводки UI падал на каждом опросе. Тест прогоняет основные
 * пути HMS-репликации на пустой базе PostgreSQL:
 * <pre>
 * docker run -d --rm --name pg-mig -e POSTGRES_PASSWORD=pg -p 55432:5432 postgres:16-alpine
 * REPLICATOR_PG_TEST_URL=jdbc:postgresql://localhost:55432/postgres?user=postgres&amp;password=pg mvn test ...
 * </pre>
 */
@SpringBootTest
@ActiveProfiles("test")
@EnabledIfEnvironmentVariable(named = "REPLICATOR_PG_TEST_URL", matches = ".+")
@DisplayName("Схема: запросы HMS-репликации на PostgreSQL")
class RepositoriesOnPostgresTest {

    @DynamicPropertySource
    static void postgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> System.getenv("REPLICATOR_PG_TEST_URL"));
        registry.add("spring.datasource.username", () -> "");
        registry.add("spring.datasource.password", () -> "");
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.database-platform", () -> "org.hibernate.dialect.PostgreSQLDialect");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.clean-disabled", () -> "false");
        registry.add("spring.flyway.clean-on-validation-error", () -> "true");
    }

    @Autowired
    private HmsCoordinatorService coordinatorService;
    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;
    @Autowired
    private TaskRepository taskRepository;
    @Autowired
    private JobService jobService;
    @Autowired
    private AgentRegistry agentRegistry;
    @Autowired
    private ReplicatorProperties properties;
    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    @DisplayName("Авторы, раздача, отчеты, агрегаты подзадач и захват задач работают на PostgreSQL")
    void hmsPathsRunOnPostgres() {
        agentRegistry.register(new AgentRegisterRequest("pg-src", "pgdc1", "all", "10.1.0.1:50051", null, 100.0),
                properties.getAgentSecret());
        agentRegistry.register(new AgentRegisterRequest("pg-dst", "pgdc2", "all", "10.1.0.2:50051", null, 100.0),
                properties.getAgentSecret());
        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "pgdc1", "pgdc2", "pg_db", "pg_db", "*", "tester");

        assertDoesNotThrow(() -> hmsJobRepository.listAuthors(null), "авторы без фильтра (null-параметр)");
        assertEquals(List.of("tester"), hmsJobRepository.listAuthors("TESTER"));

        assertEquals(1, coordinatorService.getPendingJobsForCluster("pgdc1", "pg-src").size(),
                "условный UPDATE аренды на PostgreSQL");
        assertTrue(coordinatorService.updateJobProgress(job.getId(), new HmsProgressReportRequest(
                "BOOTSTRAPPING", 1, 0, 10, 2, null, 5L, null, null, List.of(), "pg-src", "{\"done\":{}}")));

        assertDoesNotThrow(() -> jobService.aggregateSubjobsByParentIds(List.of(job.getId())));
        assertDoesNotThrow(() -> jobService.listSubjobsByParentId(job.getId(), PageRequest.of(0, 10)));
        assertDoesNotThrow(() -> transactionTemplate.executeWithoutResult(s ->
                taskRepository.findPendingTasksForClusterSkipLocked("PENDING", null, PageRequest.of(0, 10))),
                "захват задач без кластера (null-параметр)");
        assertTrue(coordinatorService.deleteReplicationJob(job.getId()));
    }
}
