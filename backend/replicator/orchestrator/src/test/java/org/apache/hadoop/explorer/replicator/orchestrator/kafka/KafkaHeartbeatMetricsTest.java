package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.CapturingStatementInspector;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Обработка heartbeat Kafka-агента на горячем пути.
 *
 * <p>Каждый heartbeat вычитывал все Kafka-задачи целиком ({@code findAll()}), фильтровал их
 * в Java и сохранял каждую подходящую. При 50 агентах с интервалом 5 секунд и 20 задачах это
 * около 200 UPDATE в секунду по одним и тем же строкам — row lock contention и write
 * amplification на ровном месте.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Kafka heartbeat: точечный запрос вместо полной выборки")
class KafkaHeartbeatMetricsTest {

    @Autowired
    private KafkaReplicationOrchestratorService service;

    @Autowired
    private KafkaReplicationJobRepository repository;

    @BeforeEach
    void setup() {
        repository.deleteAll();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        // Таблица задач Kafka общая для тестов в одной JVM, а метрики считают по ней
        // глобальные суммы: оставленные задачи ломают проверки в других классах.
        repository.deleteAll();
    }

    private void createJob(String id, String sourceDc, String targetDc, String status, String direction) {
        KafkaReplicationJobEntity job = new KafkaReplicationJobEntity();
        job.setId(id);
        job.setName(id);
        job.setSourceDc(sourceDc);
        job.setTargetDc(targetDc);
        job.setStatus(status);
        job.setDirection(direction);
        job.setTopicsJson("[\"orders\"]");
        job.setUpdatedAt(Instant.now());
        repository.save(job);
    }

    @Test
    @DisplayName("Heartbeat не вычитывает всю таблицу задач")
    void testHeartbeatDoesNotScanAllJobs() {
        createJob("kafka-active-dc1", "dc1", "dc2", "ACTIVE", "UNIDIRECTIONAL");
        createJob("kafka-stopped-dc1", "dc1", "dc2", "STOPPED", "UNIDIRECTIONAL");
        createJob("kafka-active-dc3", "dc3", "dc2", "ACTIVE", "UNIDIRECTIONAL");

        CapturingStatementInspector.start();
        service.updateMetricsFromHeartbeat("dc1", "agent-dc1-1", 1000L, 2048L, 15L);
        List<String> sql = CapturingStatementInspector.stop();

        long fullScans = sql.stream()
                .filter(q -> q.toLowerCase().contains("from kafka_replication_jobs"))
                .filter(q -> !q.toLowerCase().contains("where"))
                .count();
        assertEquals(0, fullScans,
                "Heartbeat не должен вычитывать таблицу задач целиком. SQL: " + sql);

        long updates = CapturingStatementInspector.countMatching(sql, "update kafka_replication_jobs");
        assertTrue(updates <= 1,
                "Обновляться должна только задача своего ЦОД, выполнено UPDATE: " + updates);
    }

    @Test
    @DisplayName("Метрики попадают в активную задачу своего ЦОД")
    void testMetricsLandInOwnDcJob() {
        createJob("kafka-active-dc1", "dc1", "dc2", "ACTIVE", "UNIDIRECTIONAL");
        createJob("kafka-active-dc3", "dc3", "dc2", "ACTIVE", "UNIDIRECTIONAL");

        service.updateMetricsFromHeartbeat("dc1", "agent-dc1-1", 1000L, 2048L, 15L);

        KafkaReplicationJobEntity own = repository.findById("kafka-active-dc1").orElseThrow();
        assertEquals(1000L, own.getMessagesReplicated());
        assertEquals(2048L, own.getBytesReplicated());
        assertEquals(15L, own.getCurrentLagMs());

        KafkaReplicationJobEntity foreign = repository.findById("kafka-active-dc3").orElseThrow();
        assertTrue(foreign.getMessagesReplicated() == null || foreign.getMessagesReplicated() == 0L,
                "Метрики агента dc1 не должны попадать в задачу другого ЦОД");
    }

    @Test
    @DisplayName("Обратное направление BIDIRECTIONAL-задачи тоже получает метрики")
    void testBidirectionalReverseLegIsUpdated() {
        createJob("kafka-bidi", "dc2", "dc1", "ACTIVE", "BIDIRECTIONAL");

        service.updateMetricsFromHeartbeat("dc1", "agent-dc1-1", 500L, 1024L, 7L);

        KafkaReplicationJobEntity job = repository.findById("kafka-bidi").orElseThrow();
        assertEquals(500L, job.getMessagesReplicated(),
                "Для BIDIRECTIONAL агент целевого ЦОД тоже отчитывается по этой задаче");
    }
}
