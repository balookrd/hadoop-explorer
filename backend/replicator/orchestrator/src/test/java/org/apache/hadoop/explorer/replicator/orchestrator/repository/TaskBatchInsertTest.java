package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
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
 * Вставка пакета подзадач репликации без лишних чтений.
 *
 * <p>У {@code TaskEntity} присвоенный строковый {@code @Id} и нет {@code Persistable},
 * поэтому Spring Data сохраняет его через {@code em.merge()}. Замер на Hibernate 6.5
 * показывает, что при вставке новых задач чтений не происходит
 * ({@code entityLoadCount == 0}), тогда как {@code saveAll} по detached-копиям дает
 * ровно по одной загрузке на строку. Тест закрепляет это поведение: если merge-путь
 * начнет читать каждую строку перед вставкой, пакет анализа из десятков тысяч файлов
 * превратится в такое же число одиночных запросов.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Пакетная вставка подзадач")
class TaskBatchInsertTest {

    private static final int BATCH = 20;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private JobRepository jobRepository;

    private String jobId;

    @BeforeEach
    void setup() {
        taskRepository.deleteAll();
        jobRepository.deleteAll();
        JobEntity job = new JobEntity();
        job.setId("job-batch-insert");
        job.setSourceClusterId("prod-a");
        job.setTargetClusterId("prod-b");
        job.setSourcePath("/warehouse/sales");
        job.setTargetPath("/backup/warehouse/sales");
        job.setStatus("QUEUED");
        job.setCreatedBy("tester");
        jobId = jobRepository.save(job).getId();
    }

    private List<TaskEntity> newTasks(int count) {
        List<TaskEntity> tasks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            TaskEntity task = new TaskEntity();
            task.setId(UUID.randomUUID().toString());
            task.setJobId(jobId);
            task.setSourcePath("/warehouse/sales/part-" + i + ".parquet");
            task.setTargetPath("/backup/warehouse/sales/part-" + i + ".parquet");
            task.setStatus("QUEUED");
            task.setUpdatedAt(Instant.now());
            tasks.add(task);
        }
        return tasks;
    }

    @Test
    @DisplayName("Новые подзадачи вставляются без SELECT на каждую строку")
    void testBatchInsertDoesNotSelectPerRow() {
        CapturingStatementInspector.start();
        taskRepository.saveAll(newTasks(BATCH));
        List<String> sql = CapturingStatementInspector.stop();

        long inserts = CapturingStatementInspector.countMatching(sql, "insert into replication_tasks");
        long selects = CapturingStatementInspector.countMatching(sql, "select");

        // Одно подготовленное выражение на весь пакет — это и есть JDBC-батчинг
        assertEquals(1, inserts, "Пакет должен вставляться одним подготовленным выражением. SQL: " + sql);
        assertEquals(0, selects,
                "Перед вставкой новых задач выполнено чтений: " + selects
                        + " — SELECT на строку превращает пакет анализа из десятков тысяч файлов "
                        + "в такое же число одиночных запросов. SQL: " + sql);
        assertEquals(BATCH, taskRepository.count(), "Все задачи должны оказаться в БД");
    }

    @Test
    @DisplayName("Обновление загруженной задачи читает ее один раз и пишет UPDATE")
    void testManagedUpdateIssuesUpdateNotInsert() {
        String id = taskRepository.saveAll(newTasks(1)).get(0).getId();

        CapturingStatementInspector.start();
        TaskEntity managed = taskRepository.findById(id).orElseThrow();
        managed.setStatus("RUNNING");
        taskRepository.saveAndFlush(managed);
        List<String> sql = CapturingStatementInspector.stop();

        assertEquals(1, CapturingStatementInspector.countMatching(sql, "update replication_tasks"),
                "Существующая задача обязана обновляться, а не вставляться заново. SQL: " + sql);
        assertEquals("RUNNING", taskRepository.findById(id).orElseThrow().getStatus());
    }


    @Test
    @DisplayName("Обновление существующей подзадачи сохраняется корректно")
    void testExistingTaskUpdateStillWorks() {
        List<TaskEntity> saved = taskRepository.saveAll(newTasks(2));
        String id = saved.get(0).getId();

        TaskEntity reloaded = taskRepository.findById(id).orElseThrow();
        reloaded.setStatus("COMPLETED");
        taskRepository.save(reloaded);

        assertEquals("COMPLETED", taskRepository.findById(id).orElseThrow().getStatus());
        assertEquals(2, taskRepository.count());
    }
}
