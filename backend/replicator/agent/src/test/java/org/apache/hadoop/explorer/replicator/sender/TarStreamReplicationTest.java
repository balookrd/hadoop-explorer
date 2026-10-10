package org.apache.hadoop.explorer.replicator.sender;

import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TarStreamReplicationTest {

    private String serverName;
    private Server inProcessServer;
    private File tempStagingDir;
    private File tempSourceDir;
    private File tempTargetDir;
    private HadoopFsManager fsManager;
    private MockOrchestratorClient orchestratorMock;

    static class MockOrchestratorClient extends OrchestratorClient {
        final List<BatchCreateTasksRequest> batchRequests = new CopyOnWriteArrayList<>();
        final List<CompleteTaskRequest> completedTasks = new CopyOnWriteArrayList<>();
        final List<FailTaskRequest> failedTasks = new CopyOnWriteArrayList<>();

        public MockOrchestratorClient() {
            super("http://localhost:8005", "secret", true);
        }

        @Override
        public double requestNetworkTokens(TokenRequest req) {
            return 0.0;
        }

        @Override
        public boolean batchCreateTasks(BatchCreateTasksRequest req) {
            batchRequests.add(req);
            return true;
        }

        @Override
        public boolean completeTask(CompleteTaskRequest req) {
            completedTasks.add(req);
            return true;
        }

        @Override
        public boolean failTask(FailTaskRequest req) {
            failedTasks.add(req);
            return true;
        }

        @Override
        public void updateJobProgress(String jobId, UpdateJobRequest req) {
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        tempStagingDir = Files.createTempDirectory("tar-staging-").toFile();
        tempSourceDir = Files.createTempDirectory("tar-source-").toFile();
        tempTargetDir = Files.createTempDirectory("tar-target-").toFile();

        serverName = InProcessServerBuilder.generateName();
        fsManager = new HadoopFsManager();
        orchestratorMock = new MockOrchestratorClient();

        DataTransferServiceImpl receiverService = new DataTransferServiceImpl(
                tempStagingDir.getAbsolutePath(),
                fsManager,
                null,
                null,
                4
        );

        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(receiverService)
                .build()
                .start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
            inProcessServer.awaitTermination(5, TimeUnit.SECONDS);
        }
        deleteRecursively(tempStagingDir);
        deleteRecursively(tempSourceDir);
        deleteRecursively(tempTargetDir);
    }

    @Test
    public void testSmallFilesBundlingAndZeroStagingTarStream() throws Exception {
        // 1. Создаем 25 мелких файлов (по 2-10 КБ) в различных поддиректориях
        int totalFiles = 25;
        Map<String, byte[]> expectedContents = new HashMap<>();

        for (int i = 0; i < totalFiles; i++) {
            String relPath = (i % 2 == 0)
                    ? "logs/service_" + (i % 3) + "/trace_" + i + ".log"
                    : "metrics/part_" + i + ".json";

            File f = new File(tempSourceDir, relPath);
            f.getParentFile().mkdirs();

            byte[] data = ("payload-data-line-index-" + i + "-" + UUID.randomUUID() + "\n").repeat(50).getBytes(StandardCharsets.UTF_8);
            Files.write(f.toPath(), data);
            expectedContents.put(relPath, data);
        }

        // 2. Создаем sender с порогом мелких файлов 1 МБ
        ReplicationSender sender = new ReplicationSender(
                "worker-tar-1",
                orchestratorMock,
                fsManager,
                null,
                32 * 1024,
                false, null, null, null, false,
                1024 * 1024L, 16 * 1024 * 1024L, 500
        );

        JobDto job = new JobDto(
                "job-tar-small-1",
                "dc1",
                "dc2",
                tempSourceDir.getAbsolutePath(),
                tempTargetDir.getAbsolutePath(),
                "QUEUED",
                "hdfs@REALM.LOCAL",
                false
        );

        String targetAddress = "inprocess:" + serverName;

        // 3. Анализ каталога
        boolean analyzed = sender.analyzeAndCreateTaskPool(job, targetAddress);
        assertTrue(analyzed, "Анализ каталога и формирование пула должен завершиться успешно");

        assertEquals(1, orchestratorMock.batchRequests.size());
        BatchCreateTasksRequest batchReq = orchestratorMock.batchRequests.get(0);
        assertEquals(1, batchReq.getTasks().size(), "Все 25 мелких файлов должны быть сгруппированы в ровно 1 задачу BUNDLE_TAR");

        TaskCreateItem bundleTaskCreate = batchReq.getTasks().get(0);
        assertEquals("BUNDLE_TAR", bundleTaskCreate.getTaskType());
        assertEquals(totalFiles, bundleTaskCreate.getFileCount());
        assertNotNull(bundleTaskCreate.getBundleManifest());

        // 4. Исполнение задачи бандла воркером
        TaskItemDto taskToExecute = new TaskItemDto(
                bundleTaskCreate.getId(),
                job.getId(),
                "run-1",
                bundleTaskCreate.getSourcePath(),
                bundleTaskCreate.getTargetPath(),
                bundleTaskCreate.getFileSize(),
                "QUEUED",
                "worker-tar-1",
                null,
                targetAddress,
                "hdfs@REALM.LOCAL",
                false,
                0,
                3,
                null,
                bundleTaskCreate.getTaskType(),
                bundleTaskCreate.getFileCount(),
                bundleTaskCreate.getBundleManifest()
        );

        boolean transferred = sender.transferTask(taskToExecute);
        assertTrue(transferred, "Потоковая передача виртуального TAR-стрима должна выполниться успешно");

        // 5. Проверка фиксации в MockOrchestratorClient
        assertEquals(1, orchestratorMock.completedTasks.size());
        CompleteTaskRequest completed = orchestratorMock.completedTasks.get(0);
        assertEquals(bundleTaskCreate.getId(), completed.getTaskId());
        assertEquals(totalFiles, completed.getFilesCount(), "Число переданных объектов в completeTask должно равняться totalFiles");
        assertTrue(completed.getBytesTransferred() > 0);

        // 6. Проверка файлов на целевой стороне (Zero-Staging)
        for (Map.Entry<String, byte[]> entry : expectedContents.entrySet()) {
            File targetFile = new File(tempTargetDir, entry.getKey());
            assertTrue(targetFile.exists(), "Целевой файл должен существовать: " + entry.getKey());
            byte[] targetBytes = Files.readAllBytes(targetFile.toPath());
            assertArrayEquals(entry.getValue(), targetBytes, "Содержимое целевого файла должно полностью совпадать с исходным: " + entry.getKey());
        }
    }

    @Test
    public void testMixedLargeAndSmallFiles() throws Exception {
        // Создаем 1 крупный файл (1.2 МБ) и 5 мелких файлов (по 5 КБ)
        File largeFile = new File(tempSourceDir, "large_table.parquet");
        byte[] largeData = new byte[1200 * 1024];
        Arrays.fill(largeData, (byte) 'L');
        Files.write(largeFile.toPath(), largeData);

        for (int i = 0; i < 5; i++) {
            File smallFile = new File(tempSourceDir, "small_" + i + ".txt");
            byte[] smallData = ("small-payload-" + i).repeat(100).getBytes(StandardCharsets.UTF_8);
            Files.write(smallFile.toPath(), smallData);
        }

        ReplicationSender sender = new ReplicationSender(
                "worker-mixed",
                orchestratorMock,
                fsManager,
                null,
                64 * 1024,
                false, null, null, null, false,
                1024 * 1024L, 16 * 1024 * 1024L, 500
        );

        JobDto job = new JobDto(
                "job-mixed-1",
                "dc1",
                "dc2",
                tempSourceDir.getAbsolutePath(),
                tempTargetDir.getAbsolutePath(),
                "QUEUED",
                "hdfs@REALM.LOCAL",
                false
        );

        String targetAddress = "inprocess:" + serverName;
        boolean ok = sender.analyzeAndCreateTaskPool(job, targetAddress);
        assertTrue(ok);

        assertEquals(1, orchestratorMock.batchRequests.size());
        List<TaskCreateItem> tasks = orchestratorMock.batchRequests.get(0).getTasks();
        // Должно быть ровно 2 задачи: 1 FILE (крупный) и 1 BUNDLE_TAR (5 мелких)
        assertEquals(2, tasks.size(), "Должно быть 2 задачи: 1 индивидуальная для крупного файла и 1 бандл для мелких");

        long bundleTasksCount = tasks.stream().filter(t -> "BUNDLE_TAR".equals(t.getTaskType())).count();
        long fileTasksCount = tasks.stream().filter(t -> "FILE".equals(t.getTaskType())).count();
        assertEquals(1, bundleTasksCount);
        assertEquals(1, fileTasksCount);

        // Исполняем обе задачи
        for (TaskCreateItem t : tasks) {
            TaskItemDto taskItem = new TaskItemDto(
                    t.getId(),
                    job.getId(),
                    "run-1",
                    t.getSourcePath(),
                    t.getTargetPath(),
                    t.getFileSize(),
                    "QUEUED",
                    "worker-mixed",
                    null,
                    targetAddress,
                    "hdfs@REALM.LOCAL",
                    false,
                    0, 3, null,
                    t.getTaskType(),
                    t.getFileCount(),
                    t.getBundleManifest()
            );
            assertTrue(sender.transferTask(taskItem));
        }

        // Проверяем, что крупный файл и мелкие файлы существуют на приемнике
        File targetLarge = new File(tempTargetDir, "large_table.parquet");
        assertTrue(targetLarge.exists());
        assertEquals(largeData.length, targetLarge.length());

        for (int i = 0; i < 5; i++) {
            File targetSmall = new File(tempTargetDir, "small_" + i + ".txt");
            assertTrue(targetSmall.exists());
        }
    }

    private void deleteRecursively(File file) {
        if (file == null || !file.exists()) return;
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }
}
