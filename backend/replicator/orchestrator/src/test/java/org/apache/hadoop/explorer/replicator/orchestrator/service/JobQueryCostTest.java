package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.CapturingStatementInspector;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Стоимость выборок в путях, которые вызываются постоянно.
 *
 * <p>Опрос активных задач Kafka для ЦОД и дозаполнение истории запусков при старте читали
 * таблицу заданий целиком и фильтровали в памяти. Первый вызывается на каждом обращении
 * агента, второй — при старте узла, где на большой таблице это еще и N+1 по истории.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Задания: стоимость выборок в частых путях")
class JobQueryCostTest {

    @Autowired
    private KafkaReplicationOrchestratorService kafkaService;

    @Autowired
    private KafkaReplicationJobRepository kafkaJobRepository;

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @BeforeEach
    void setUp() {
        kafkaJobRepository.deleteAll();
    }

    private KafkaReplicationJobEntity kafkaJob(String id, String status, String direction,
                                               String sourceDc, String targetDc) {
        KafkaReplicationJobEntity job = new KafkaReplicationJobEntity();
        job.setId(id);
        job.setName(id);
        job.setStatus(status);
        job.setDirection(direction);
        job.setSourceDc(sourceDc);
        job.setTargetDc(targetDc);
        job.setTopicsJson("[]");
        return job;
    }

    @Test
    @DisplayName("Активные задачи Kafka для ЦОД выбираются запросом, а не фильтром в памяти")
    void activeKafkaJobsForDcShouldBeQueried() {
        kafkaJobRepository.save(kafkaJob("kafka-out", "ACTIVE", "UNIDIRECTIONAL", "dc1", "dc2"));
        kafkaJobRepository.save(kafkaJob("kafka-back", "ACTIVE", "BIDIRECTIONAL", "dc2", "dc1"));
        kafkaJobRepository.save(kafkaJob("kafka-paused", "PAUSED", "UNIDIRECTIONAL", "dc1", "dc2"));
        kafkaJobRepository.save(kafkaJob("kafka-foreign", "ACTIVE", "UNIDIRECTIONAL", "dc3", "dc4"));

        CapturingStatementInspector.start();
        List<KafkaReplicationJobDto> active = kafkaService.getActiveJobsForDc("dc1");
        List<String> sql = CapturingStatementInspector.stop();

        assertEquals(2, active.size(),
                "Своя активная задача и обратное направление двусторонней: " + active.stream()
                        .map(KafkaReplicationJobDto::getId).toList());
        assertTrue(active.stream().anyMatch(j -> "kafka-out".equals(j.getId())));
        assertTrue(active.stream().anyMatch(j -> "kafka-back".equals(j.getId())));
        assertTrue(!sql.isEmpty(), "Перехватчик SQL должен видеть запросы вызова");
        assertEquals(0, CapturingStatementInspector.countFullScans(sql, "kafka_replication_jobs"),
                "Опрос агента не должен вычитывать таблицу задач Kafka целиком. SQL: " + sql);
    }

    @Test
    @DisplayName("Дозаполнение истории запусков не читает таблицу заданий целиком")
    void runBackfillShouldNotScanAllJobs() {
        jobRepository.save(job("job-backfill-1"));
        jobRepository.save(job("job-backfill-2"));

        CapturingStatementInspector.start();
        jobService.initializeExistingJobs();
        List<String> sql = CapturingStatementInspector.stop();

        assertEquals(1, jobRunRepository.countByJobId("job-backfill-1"),
                "Задание без истории должно получить первый запуск");
        assertEquals(1, jobRunRepository.countByJobId("job-backfill-2"),
                "Задание без истории должно получить первый запуск");
        assertEquals(0, CapturingStatementInspector.countFullScans(sql, "replication_jobs"),
                "Старт узла не должен вычитывать таблицу заданий целиком. SQL: " + sql);
    }

    private JobEntity job(String id) {
        JobEntity job = new JobEntity();
        job.setId(id);
        job.setSourceClusterId("prod-a");
        job.setTargetClusterId("prod-b");
        job.setSourcePath("/warehouse/" + id);
        job.setTargetPath("/backup/warehouse/" + id);
        job.setStatus("QUEUED");
        job.setCreatedBy("tester");
        return job;
    }
}
