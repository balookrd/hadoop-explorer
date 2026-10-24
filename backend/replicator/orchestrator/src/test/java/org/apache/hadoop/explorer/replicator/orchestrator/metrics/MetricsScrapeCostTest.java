package org.apache.hadoop.explorer.replicator.orchestrator.metrics;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.CapturingStatementInspector;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
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

    @Test
    @DisplayName("Распределение размеров файлов считается верно")
    void testFileSizeHistogramValues() {
        String body = metricsService.renderReplicatorPrometheusMetrics();

        assertTrue(body.contains("replicator_file_size_bytes_count 50"),
                "Количество файлов в гистограмме должно соответствовать данным, получено:\n"
                        + body.lines().filter(l -> l.startsWith("replicator_file_size_bytes")).toList());
    }
}
