package org.apache.hadoop.explorer.replicator.orchestrator.metrics;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.CapturingStatementInspector;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Стоимость одного скрейпа метрик.
 *
 * <p>Рендер метрик вычитывал таблицу пофайловых подзадач целиком, а эндпоинт был доступен
 * без аутентификации. Prometheus со стандартным интервалом 15 секунд (или кто угодно, кто
 * дотянулся до порта) превращал это в постоянную полную выборку таблицы на миллионы строк.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Метрики: стоимость скрейпа")
class MetricsScrapeCostTest {

    @Autowired
    private ReplicatorMetricsService metricsService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @Autowired
    private KafkaReplicationJobRepository kafkaJobRepository;

    @BeforeEach
    void setUp() {
        taskRepository.deleteAll();
        jobRepository.deleteAll();

        JobEntity job = new JobEntity();
        job.setId("job-metrics");
        job.setSourceClusterId("prod-a");
        job.setTargetClusterId("prod-b");
        job.setSourcePath("/warehouse/sales");
        job.setTargetPath("/backup/warehouse/sales");
        job.setStatus("RUNNING");
        job.setCreatedBy("tester");
        jobRepository.save(job);

        List<TaskEntity> tasks = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            TaskEntity task = new TaskEntity();
            task.setId(UUID.randomUUID().toString());
            task.setJobId("job-metrics");
            task.setSourcePath("/warehouse/sales/part-" + i);
            task.setTargetPath("/backup/warehouse/sales/part-" + i);
            task.setStatus("COMPLETED");
            task.setFileSize(1024L * (i + 1));
            task.setUpdatedAt(Instant.now());
            tasks.add(task);
        }
        taskRepository.saveAll(tasks);
    }

    @Test
    @DisplayName("Скрейп не вычитывает таблицу подзадач целиком")
    void testScrapeDoesNotSelectAllTasks() {
        CapturingStatementInspector.start();
        String body = metricsService.renderReplicatorPrometheusMetrics();
        List<String> sql = CapturingStatementInspector.stop();

        assertTrue(body.contains("replicator_file_size_bytes"), "Метрика размеров файлов должна остаться");

        long fullTaskScans = sql.stream()
                .filter(q -> q.toLowerCase().contains("from replication_tasks"))
                .filter(q -> !q.toLowerCase().contains("where"))
                .filter(q -> !q.toLowerCase().contains("count(") && !q.toLowerCase().contains("sum("))
                .count();
        assertEquals(0, fullTaskScans,
                "Скрейп вычитывает подзадачи целиком: на таблице в миллионы строк это OOM "
                        + "или остановка под GC-паузами. SQL: " + sql);
    }

    /**
     * Задания всех трех видов с известными суммами: агрегаты должны давать те же числа,
     * что прежний подсчет в памяти.
     */
    private void prepareJobsOfEveryKind() {
        hmsJobRepository.deleteAll();
        kafkaJobRepository.deleteAll();

        jobRepository.save(hdfsJob("job-done", "COMPLETED", 9000L, 9000L, true));
        jobRepository.save(hdfsJob("job-running", "RUNNING", 4000L, 1500L, false));

        hmsJobRepository.save(hmsJob("hms-1", 20, 8, 500, 200));
        hmsJobRepository.save(hmsJob("hms-2", 10, 4, 200, 50));

        kafkaJobRepository.save(kafkaJob("kafka-1", "ACTIVE", 1000L, 2048L, 150L));
        kafkaJobRepository.save(kafkaJob("kafka-2", "PAUSED", 500L, 1024L, 900L));
    }

    private JobEntity hdfsJob(String id, String status, long totalBytes, long copiedBytes, boolean scheduled) {
        JobEntity job = new JobEntity();
        job.setId(id);
        job.setSourceClusterId("prod-a");
        job.setTargetClusterId("prod-b");
        job.setSourcePath("/warehouse/" + id);
        job.setTargetPath("/backup/warehouse/" + id);
        job.setStatus(status);
        job.setCreatedBy("tester");
        job.setTotalBytes(totalBytes);
        job.setCopiedBytes(copiedBytes);
        job.setScheduled(scheduled);
        return job;
    }

    private HmsReplicationJobEntity hmsJob(String id, int totalTables, int replicatedTables,
                                           int totalPartitions, int replicatedPartitions) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId(id);
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName("analytics");
        job.setTargetDbName("analytics_replica");
        job.setStatus("ACTIVE");
        job.setCreatedBy("tester");
        job.setTotalTables(totalTables);
        job.setReplicatedTables(replicatedTables);
        job.setTotalPartitions(totalPartitions);
        job.setReplicatedPartitions(replicatedPartitions);
        return job;
    }

    private KafkaReplicationJobEntity kafkaJob(String id, String status, long messages, long bytes, long lagMs) {
        KafkaReplicationJobEntity job = new KafkaReplicationJobEntity();
        job.setId(id);
        job.setName(id);
        job.setSourceDc("dc1");
        job.setTargetDc("dc2");
        job.setTopicsJson("[]");
        job.setStatus(status);
        job.setMessagesReplicated(messages);
        job.setBytesReplicated(bytes);
        job.setCurrentLagMs(lagMs);
        return job;
    }


    @Test
    @DisplayName("Скрейп не вычитывает таблицы заданий целиком")
    void testScrapeDoesNotSelectAllJobs() {
        prepareJobsOfEveryKind();

        CapturingStatementInspector.start();
        String body = metricsService.renderReplicatorPrometheusMetrics();
        List<String> sql = CapturingStatementInspector.stop();

        assertTrue(body.contains("replicator_jobs_total"), "Метрики заданий должны остаться");

        assertEquals(0, CapturingStatementInspector.countFullScans(sql, "replication_jobs"),
                "Скрейп вычитывает задания HDFS целиком, и так на каждом интервале Prometheus. SQL: " + sql);
        assertEquals(0, CapturingStatementInspector.countFullScans(sql, "hms_replication_jobs"),
                "Скрейп вычитывает задания HMS целиком. SQL: " + sql);
        assertEquals(0, CapturingStatementInspector.countFullScans(sql, "kafka_replication_jobs"),
                "Скрейп вычитывает задания Kafka целиком. SQL: " + sql);
    }

    @Test
    @DisplayName("Агрегаты заданий считаются верно")
    void testJobAggregatesValues() {
        prepareJobsOfEveryKind();

        String body = metricsService.renderReplicatorPrometheusMetrics();

        assertTrue(body.contains("replicator_bytes_total{status=\"COMPLETED\"} 9000"),
                gauge(body, "replicator_bytes_total"));
        assertTrue(body.contains("replicator_bytes_total{status=\"IN_PROGRESS\"} 1500"),
                gauge(body, "replicator_bytes_total"));
        assertTrue(body.contains("replicator_jobs_total{status=\"SCHEDULED\",mode=\"scheduled\"} 1"),
                gauge(body, "replicator_jobs_total"));
        assertTrue(body.contains("replicator_jobs_total{status=\"MANUAL\",mode=\"manual\"} 2"),
                gauge(body, "replicator_jobs_total"));
        assertTrue(body.contains("replicator_hms_tables_total{type=\"total\"} 30"),
                gauge(body, "replicator_hms_tables_total"));
        assertTrue(body.contains("replicator_hms_tables_total{type=\"replicated\"} 12"),
                gauge(body, "replicator_hms_tables_total"));
        assertTrue(body.contains("replicator_hms_partitions_total{type=\"total\"} 700"),
                gauge(body, "replicator_hms_partitions_total"));
        assertTrue(body.contains("replicator_hms_partitions_total{type=\"replicated\"} 250"),
                gauge(body, "replicator_hms_partitions_total"));
    }

    private String gauge(String body, String prefix) {
        return "Фактические метрики:\n" + body.lines().filter(l -> l.startsWith(prefix)).toList();
    }

    @Test
    @DisplayName("Распределение размеров файлов считается верно")
    void testFileSizeHistogramValues() {
        String body = metricsService.renderReplicatorPrometheusMetrics();

        assertTrue(body.contains("replicator_file_size_bytes_count 50"),
                "Количество файлов в гистограмме должно соответствовать данным, получено:\n"
                        + body.lines().filter(l -> l.startsWith("replicator_file_size_bytes")).toList());
    }
}
