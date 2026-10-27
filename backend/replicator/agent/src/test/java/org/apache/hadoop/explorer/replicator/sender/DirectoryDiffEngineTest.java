package org.apache.hadoop.explorer.replicator.sender;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.model.UpdateJobRequest;
import org.apache.hadoop.explorer.replicator.model.TaskCreateItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("DirectoryDiffEngine Unit Tests")
class DirectoryDiffEngineTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("createBundleTaskItem: формирует задачу BUNDLE_TAR с валидным JSON-манифестом")
    void testCreateBundleTaskItem() throws Exception {
        DirectoryDiffEngine engine = new DirectoryDiffEngine(
                null, null, objectMapper, 1024 * 1024, 16 * 1024 * 1024, 1000
        );

        List<HadoopFsManager.FileItem> items = new ArrayList<>();
        items.add(new HadoopFsManager.FileItem("file1.txt", "/data/input/file1.txt", 150L, 1000L));
        items.add(new HadoopFsManager.FileItem("subdir/file2.txt", "/data/input/subdir/file2.txt", 350L, 2000L));

        TaskCreateItem task = engine.createBundleTaskItem("/data/input", "/data/output", items, 500L);

        assertNotNull(task);
        assertNotNull(task.getId());
        assertEquals("/data/input", task.getSourcePath());
        assertEquals("/data/output", task.getTargetPath());
        assertEquals(500L, task.getFileSize());
        assertEquals("BUNDLE_TAR", task.getTaskType());
        assertEquals(2, task.getFileCount());

        List<ReplicationSender.BundleFileEntry> manifest = objectMapper.readValue(
                task.getBundleManifest(),
                new TypeReference<List<ReplicationSender.BundleFileEntry>>() {}
        );
        assertEquals(2, manifest.size());
        assertEquals("/data/input/file1.txt", manifest.get(0).getSourcePath());
        assertEquals("file1.txt", manifest.get(0).getRelativePath());
        assertEquals(150L, manifest.get(0).getSize());

        assertEquals("/data/input/subdir/file2.txt", manifest.get(1).getSourcePath());
        assertEquals("subdir/file2.txt", manifest.get(1).getRelativePath());
        assertEquals(350L, manifest.get(1).getSize());
    }

    @Test
    @DisplayName("createBundleTaskItem: пустой список файлов корректно упаковывается")
    void testCreateBundleTaskItemEmptyList() {
        DirectoryDiffEngine engine = new DirectoryDiffEngine(
                null, null, objectMapper, 1024, 1024, 10
        );

        TaskCreateItem task = engine.createBundleTaskItem("/src", "/dst", List.of(), 0L);
        assertNotNull(task);
        assertEquals(1, task.getFileCount());
        assertEquals("[]", task.getBundleManifest().trim());
    }

    private static JobDto missingSourceJob(String jobType) {
        JobDto job = new JobDto("job-1", "dc1", "dc2", "/warehouse/db.db/t/dt=1", "/warehouse/db.db/t/dt=1",
                "RUNNING", "user@REALM", false);
        job.setJobType(jobType);
        return job;
    }

    @Test
    @DisplayName("HMS-подзадача: каталога партиции на источнике нет — копировать нечего, подзадача успешна")
    void hmsSubjobWithMissingSourceSucceedsEmpty() {
        HadoopFsManager fs = mock(HadoopFsManager.class);
        when(fs.exists(anyString(), any(), anyBoolean())).thenReturn(false);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        DirectoryDiffEngine engine = new DirectoryDiffEngine(fs, orchestrator, objectMapper, 1024, 1024, 10);

        assertTrue(engine.analyzeAndCreateTaskPool(missingSourceJob("HMS_SUBJOB"), null),
                "отказ подзадачи ронял барьер данных и bootstrap всей таблицы");
        verify(orchestrator).batchCreateTasks(argThat((BatchCreateTasksRequest r) -> r.getTasks().isEmpty()));
        verify(orchestrator, never()).updateJobProgress(anyString(), any(UpdateJobRequest.class));
    }

    @Test
    @DisplayName("Обычная задача: отсутствующий источник — по-прежнему отказ")
    void standaloneJobWithMissingSourceFails() {
        HadoopFsManager fs = mock(HadoopFsManager.class);
        when(fs.exists(anyString(), any(), anyBoolean())).thenReturn(false);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        DirectoryDiffEngine engine = new DirectoryDiffEngine(fs, orchestrator, objectMapper, 1024, 1024, 10);

        assertFalse(engine.analyzeAndCreateTaskPool(missingSourceJob("HDFS"), null));
        verify(orchestrator).updateJobProgress(eq("job-1"), argThat((UpdateJobRequest r) -> "FAILED".equals(r.getStatus())));
    }
}
