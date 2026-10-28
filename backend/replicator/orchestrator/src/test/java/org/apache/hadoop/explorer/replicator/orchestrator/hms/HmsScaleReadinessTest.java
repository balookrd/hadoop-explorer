package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest;
import org.apache.hadoop.explorer.replicator.model.ClaimTasksRequest;
import org.apache.hadoop.explorer.replicator.model.TaskCreateItem;
import org.apache.hadoop.explorer.replicator.model.TaskItemDto;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.TaskService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Оркестратор под боевой масштаб HMS-репликации: пулы на миллионы файлов порциями, пауза,
 * останавливающая копирование, длинные пути и сообщения.
 */
@SpringBootTest
@Transactional
class HmsScaleReadinessTest {

    @Autowired
    private TaskService taskService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    private JobEntity subjob(String parentId, String status) {
        JobEntity job = new JobEntity();
        job.setId("job-" + UUID.randomUUID());
        job.setJobType("HMS_SUBJOB");
        job.setParentJobId(parentId);
        job.setSourcePath("/warehouse/db.db/t/dt=" + UUID.randomUUID());
        job.setTargetPath("/warehouse/db.db/t/dt=x");
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setStatus(status);
        return jobRepository.saveAndFlush(job);
    }

    private HmsReplicationJobEntity hmsJob(String status) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId("hms-job-" + UUID.randomUUID());
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName("db");
        job.setTargetDbName("db");
        job.setCreatedBy("admin_user");
        job.setStatus(status);
        return hmsJobRepository.saveAndFlush(job);
    }

    private static List<TaskCreateItem> items(String prefix, int count, boolean skipped) {
        List<TaskCreateItem> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(new TaskCreateItem(prefix + "-" + i, "/src/" + prefix + "/f" + i, "/dst/" + prefix + "/f" + i, 100L, skipped));
        }
        return list;
    }

    private static BatchCreateTasksRequest chunk(String jobId, List<TaskCreateItem> items, boolean append, boolean more) {
        BatchCreateTasksRequest req = new BatchCreateTasksRequest(jobId, items);
        req.setAppend(append);
        req.setMoreChunks(more);
        return req;
    }

    @Test
    @DisplayName("Пул порциями: задание запускается по последней порции, итоги — по всем порциям")
    void chunkedPoolStartsJobOnLastChunkWithTotalsOfAllChunks() {
        JobEntity job = subjob(null, "ANALYZING");
        List<TaskCreateItem> second = items("b", 3, false);

        assertTrue(taskService.batchCreateTasks(job.getId(), chunk(job.getId(), items("a", 3, false), false, true)));
        assertEquals("ANALYZING", jobRepository.findById(job.getId()).orElseThrow().getStatus(),
                "промежуточная порция не запускает задание");
        assertTrue(taskService.batchCreateTasks(job.getId(), chunk(job.getId(), second, true, true)));
        assertTrue(taskService.batchCreateTasks(job.getId(), chunk(job.getId(), items("c", 2, true), true, false)));

        JobEntity started = jobRepository.findById(job.getId()).orElseThrow();
        assertEquals("RUNNING", started.getStatus());
        assertEquals(8, started.getTotalObjects());
        assertEquals(800L, started.getTotalBytes());
        assertEquals(2, started.getSkippedObjects());
        assertEquals(200L, started.getCopiedBytes());
    }

    @Test
    @DisplayName("Пул отмененного задания отклоняется: копия не идет после отмены")
    void poolOfCancelledJobIsRejected() {
        JobEntity job = subjob(null, "CANCELLED");
        assertFalse(taskService.batchCreateTasks(job.getId(), new BatchCreateTasksRequest(job.getId(), items("x", 2, false))));
        assertEquals("CANCELLED", jobRepository.findById(job.getId()).orElseThrow().getStatus());
        assertTrue(taskRepository.findByJobId(job.getId()).isEmpty());
    }

    @Test
    @DisplayName("Подзадачи HMS-задачи на паузе не анализируются и не раздаются, после возобновления раздаются")
    void subjobsOfPausedHmsJobAreHeldBack() {
        HmsReplicationJobEntity parent = hmsJob("PAUSED");
        JobEntity queued = subjob(parent.getId(), "QUEUED");
        assertEquals(0, jobRepository.claimJobForAnalysis(queued.getId(), "analysis", Instant.now()));

        JobEntity running = subjob(parent.getId(), "ANALYZING");
        assertTrue(taskService.batchCreateTasks(running.getId(), new BatchCreateTasksRequest(running.getId(), items("p", 2, false))));
        List<TaskItemDto> whilePaused = taskService.claimTasks(new ClaimTasksRequest("worker-paused-1", "dc1", 10));
        assertTrue(whilePaused.stream().noneMatch(t -> running.getId().equals(t.getJobId())),
                "задачи приостановленной HMS-задачи не выдаются");

        parent.setStatus("ACTIVE");
        hmsJobRepository.saveAndFlush(parent);
        assertEquals(1, jobRepository.claimJobForAnalysis(queued.getId(), "analysis", Instant.now()));
        // Без кластера: пустой опрос выше включил для dc1 короткий кулдаун очереди
        List<TaskItemDto> afterResume = taskService.claimTasks(new ClaimTasksRequest("worker-paused-2", null, 10));
        assertTrue(afterResume.stream().anyMatch(t -> running.getId().equals(t.getJobId())));
    }

    @Test
    @DisplayName("Длинные пути файлов сохраняются, длинные сообщения усекаются под колонку")
    void longPathsAreStoredAndLongMessagesTruncated() {
        String longPath = "/warehouse/db.db/t/" + "k=v/".repeat(600) + "part-00000.snappy.parquet";
        JobEntity job = subjob(null, "ANALYZING");
        job.setSourcePath(longPath);
        job.setMessage("ошибка ".repeat(2000));
        jobRepository.saveAndFlush(job);
        assertTrue(taskService.batchCreateTasks(job.getId(), new BatchCreateTasksRequest(job.getId(),
                List.of(new TaskCreateItem("long-1", longPath, longPath, 10L, false)))));
        assertEquals(longPath, taskRepository.findByJobId(job.getId()).get(0).getSourcePath());

        HmsReplicationJobEntity hms = hmsJob("ACTIVE");
        hms.setMessage("x".repeat(20_000));
        hmsJobRepository.saveAndFlush(hms);
        assertEquals(4096, hmsJobRepository.findById(hms.getId()).orElseThrow().getMessage().length());
    }

    @Test
    @DisplayName("Финализация: выбираются только RUNNING-задания со всеми учтенными объектами и без активных задач")
    void readyToFinalizeQuerySelectsOnlyDrainedJobs() {
        JobEntity drained = subjob(null, "RUNNING");
        drained.setTotalObjects(2);
        drained.setTransferredObjects(2);
        jobRepository.saveAndFlush(drained);

        JobEntity busy = subjob(null, "ANALYZING");
        assertTrue(taskService.batchCreateTasks(busy.getId(), new BatchCreateTasksRequest(busy.getId(), items("q", 1, false))));

        List<String> ready = jobRepository.findRunningJobsReadyToFinalize(PageRequest.of(0, 500));
        assertTrue(ready.contains(drained.getId()));
        assertFalse(ready.contains(busy.getId()));
    }
}
