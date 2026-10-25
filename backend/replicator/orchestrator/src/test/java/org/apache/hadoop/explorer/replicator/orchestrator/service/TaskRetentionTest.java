package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Удержание истории пофайловых задач.
 *
 * <p>Таблица {@code replication_tasks} растет без границ: на каждое задание в нее попадает по
 * записи на файл, то есть миллионы строк на крупной синхронизации. Настройка
 * {@code task-retention-days} существовала и по умолчанию обещала семь дней, запрос удаления по
 * возрасту тоже был написан — но его не вызывал никто, ни планировщик, ни REST. Очистка была
 * сконфигурирована и не выполнялась, и именно эту задачу должно было решать секционирование из
 * неподключенного DDL-скрипта.
 *
 * <p>Проверяется и обратная сторона: очистка не имеет права задеть активные задачи, иначе
 * агенты потеряют то, что сейчас передают, а задание никогда не завершится.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Удержание истории задач: очистка по возрасту")
class TaskRetentionTest {

    private static final String JOB_ID = "job-retention-test";

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ReplicatorProperties properties;

    private int originalRetentionDays;

    @BeforeEach
    void setUp() {
        originalRetentionDays = properties.getTaskRetentionDays();
        taskRepository.deleteAll();
        jobRepository.deleteAll();

        JobEntity job = new JobEntity();
        job.setId(JOB_ID);
        job.setSourcePath("/data/source");
        job.setTargetPath("/data/target");
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setStatus("COMPLETED");
        job.setCreatedBy("operator");
        jobRepository.save(job);
    }

    @AfterEach
    void tearDown() {
        properties.setTaskRetentionDays(originalRetentionDays);
        taskRepository.deleteAll();
        jobRepository.deleteAll();
    }

    private void createTask(String id, String status, long ageDays) {
        TaskEntity task = new TaskEntity();
        task.setId(id);
        task.setJobId(JOB_ID);
        task.setSourcePath("/data/source/" + id);
        task.setTargetPath("/data/target/" + id);
        task.setStatus(status);
        task.setFileSize(1024);
        task.setUpdatedAt(Instant.now().minus(ageDays, ChronoUnit.DAYS));
        taskRepository.save(task);
    }

    private Set<String> remainingIds() {
        return taskRepository.findAll().stream()
                .map(TaskEntity::getId)
                .collect(Collectors.toSet());
    }

    @Test
    @DisplayName("Завершенные задачи старше срока удержания удаляются")
    void testExpiredTerminalTasksAreRemoved() {
        properties.setTaskRetentionDays(7);
        createTask("old-completed", "COMPLETED", 30);
        createTask("old-skipped", "SKIPPED", 10);
        createTask("old-failed", "FAILED", 8);

        int removed = taskService.executePurgeExpiredTasks();

        assertEquals(3, removed, "Очистка обязана удалить все завершенные задачи старше срока");
        assertTrue(remainingIds().isEmpty(),
                "В таблице остались записи: " + remainingIds()
                        + ". Без очистки история пофайловых задач растет без границ");
    }

    @Test
    @DisplayName("Активные задачи не удаляются, каким бы ни был их возраст")
    void testActiveTasksAreNeverRemoved() {
        properties.setTaskRetentionDays(7);
        createTask("stuck-running", "RUNNING", 90);
        createTask("waiting-queued", "QUEUED", 90);
        createTask("old-completed", "COMPLETED", 90);

        int removed = taskService.executePurgeExpiredTasks();

        assertEquals(1, removed, "Удалить следовало только завершенную задачу");
        assertEquals(Set.of("stuck-running", "waiting-queued"), remainingIds(),
                "Очистка задела активные задачи: агент потеряет то, что передает сейчас, "
                        + "а задание никогда не завершится");
    }

    @Test
    @DisplayName("Свежие завершенные задачи сохраняются")
    void testRecentTasksAreKept() {
        properties.setTaskRetentionDays(7);
        createTask("fresh-completed", "COMPLETED", 1);
        createTask("borderline-completed", "COMPLETED", 6);

        int removed = taskService.executePurgeExpiredTasks();

        assertEquals(0, removed, "Свежие задачи удалять нельзя: по ним смотрят отчеты о прогоне");
        assertEquals(2, remainingIds().size(), "Остались: " + remainingIds());
    }

    @Test
    @DisplayName("Нулевой срок удержания выключает очистку")
    void testZeroRetentionDisablesPurge() {
        properties.setTaskRetentionDays(0);
        createTask("old-completed", "COMPLETED", 365);

        int removed = taskService.executePurgeExpiredTasks();

        assertEquals(0, removed,
                "Нулевой срок означает «хранить все»: очистка не должна удалять ничего");
        assertEquals(1, remainingIds().size(), "Задача должна остаться");
    }

    @Test
    @DisplayName("За один проход удаляется не больше лимита: длинная транзакция не блокирует очередь")
    void testPurgeIsBounded() {
        properties.setTaskRetentionDays(7);
        int batchSize = 5;
        int maxBatches = 2;
        properties.setTaskRetentionBatchSize(batchSize);
        properties.setTaskRetentionMaxBatchesPerRun(maxBatches);

        List<String> created = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            String id = String.format("old-%02d", i);
            createTask(id, "COMPLETED", 30);
            created.add(id);
        }

        int removed = taskService.executePurgeExpiredTasks();

        assertEquals(batchSize * maxBatches, removed,
                "Проход обязан быть ограничен: удаление миллионов строк одной транзакцией "
                        + "держит блокировки и раздувает WAL");
        assertEquals(created.size() - removed, remainingIds().size(),
                "Остаток задач должен соответствовать удаленному количеству");

        // Следующий проход продолжает с того места, где остановился предыдущий
        int removedOnSecondRun = taskService.executePurgeExpiredTasks();
        assertEquals(batchSize * maxBatches, removedOnSecondRun,
                "Очистка обязана догонять за следующие проходы, а не терять остаток");
    }
}
