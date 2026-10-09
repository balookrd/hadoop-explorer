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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class DistributedMultiAgentReplicationTest {

    private String serverName;
    private Server inProcessServer;
    private File stagingDir;
    private File targetDir;
    private File srcDir;

    private HadoopFsManager fsManager;
    private LocalBandwidthLimiter bandwidthLimiter;
    private MockDistributedOrchestratorClient orchestratorMock;

    @BeforeEach
    void setUp() throws IOException {
        serverName = "test-distributed-" + UUID.randomUUID();
        stagingDir = Files.createTempDirectory("repl-staging-dist-").toFile();
        targetDir = Files.createTempDirectory("repl-target-dist-").toFile();
        srcDir = Files.createTempDirectory("repl-src-dist-").toFile();

        fsManager = new HadoopFsManager("file:///", null, null);
        bandwidthLimiter = new LocalBandwidthLimiter(0.0);
        orchestratorMock = new MockDistributedOrchestratorClient();

        DataTransferServiceImpl service = new DataTransferServiceImpl(
                stagingDir.getAbsolutePath(),
                fsManager,
                bandwidthLimiter,
                null
        );

        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(service)
                .build()
                .start();
    }

    @AfterEach
    void tearDown() {
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
        }
        deleteRecursively(stagingDir);
        deleteRecursively(targetDir);
        deleteRecursively(srcDir);
    }

    @Test
    @DisplayName("Масштабирование репликации: Анализ формирует пул пофайловых задач, параллельно разбираемый несколькими воркерами")
    void testDistributedTaskPoolMultiAgentExecution() throws Exception {
        // 1. Создаем 6 файлов разного размера в исходном каталоге
        int filesCount = 6;
        for (int i = 0; i < filesCount; i++) {
            File f = new File(srcDir, "part-0000" + i + ".parquet");
            try (FileOutputStream fos = new FileOutputStream(f)) {
                byte[] data = ("Distributed parquet data chunk #" + i + " - " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
                fos.write(data);
            }
        }

        JobDto job = new JobDto();
        job.setId("job-dist-100");
        job.setSourcePath(srcDir.getAbsolutePath());
        job.setTargetPath(targetDir.getAbsolutePath());
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setStatus("QUEUED");

        String targetAddress = "inprocess:" + serverName;

        // 2. Агент-анализатор (Analyzer): сканирует HDFS-1, запрашивает манифест у целевого узла и формирует пул
        ReplicationSender analyzerSender = new ReplicationSender(
                "analyzer-agent-dc1",
                orchestratorMock,
                fsManager,
                bandwidthLimiter,
                64 * 1024,
                false, null, null, null, false
        );

        boolean analyzed = analyzerSender.analyzeAndCreateTaskPool(job, targetAddress);
        assertTrue(analyzed, "Этап анализа каталога и создания пула задач должен быть успешен");

        // Проверяем, что в оркестраторе зарегистрировано ровно 6 пофайловых задач
        assertEquals(filesCount, orchestratorMock.taskPool.size(), "Пул задач должен содержать ровно 6 элементов");

        // 3. Создаем 2 параллельных воркера (Agent-Worker-1 и Agent-Worker-2)
        ReplicationSender worker1 = new ReplicationSender(
                "worker-dc1-node1",
                orchestratorMock,
                fsManager,
                bandwidthLimiter,
                64 * 1024,
                false, null, null, null, false
        );

        ReplicationSender worker2 = new ReplicationSender(
                "worker-dc1-node2",
                orchestratorMock,
                fsManager,
                bandwidthLimiter,
                64 * 1024,
                false, null, null, null, false
        );

        ExecutorService executor = Executors.newFixedThreadPool(2);

        Callable<Integer> workerTask = (Callable<Integer>) () -> {
            String workerId = Thread.currentThread().getName();
            ReplicationSender senderInstance = workerId.contains("1") ? worker1 : worker2;
            int processed = 0;

            while (true) {
                List<TaskItemDto> batch = orchestratorMock.claimTasks(new ClaimTasksRequest(workerId, "dc1", 2));
                if (batch.isEmpty()) {
                    break;
                }
                for (TaskItemDto t : batch) {
                    t.setTargetAddress(targetAddress);
                    boolean ok = senderInstance.transferTask(t);
                    if (ok) {
                        processed++;
                    }
                }
            }
            return processed;
        };

        Future<Integer> f1 = executor.submit(() -> {
            Thread.currentThread().setName("worker-1");
            return workerTask.call();
        });

        Future<Integer> f2 = executor.submit(() -> {
            Thread.currentThread().setName("worker-2");
            return workerTask.call();
        });

        int doneWorker1 = f1.get(10, TimeUnit.SECONDS);
        int doneWorker2 = f2.get(10, TimeUnit.SECONDS);
        executor.shutdown();

        // 4. Проверяем результаты:
        assertEquals(filesCount, doneWorker1 + doneWorker2, "Суммарно всеми воркерами должны быть обработаны все 6 файлов");
        assertTrue(doneWorker1 > 0, "Worker 1 должен был выполнить хотя бы 1 задачу");
        assertTrue(doneWorker2 > 0, "Worker 2 должен был выполнить хотя бы 1 задачу");

        // Проверяем, что все файлы появились в целевой директории
        File[] targetFiles = targetDir.listFiles((dir, name) -> name.endsWith(".parquet"));
        assertNotNull(targetFiles);
        assertEquals(filesCount, targetFiles.length, "Все 6 файлов должны быть успешно закоммичены в targetDir");

        // Проверяем, что все задачи в оркестраторе перешли в статус COMPLETED
        for (TaskItemDto task : orchestratorMock.taskPool.values()) {
            assertEquals("COMPLETED", task.getStatus(), "Все задачи в пуле должны быть COMPLETED");
            assertNotNull(task.getAssignedAgentId(), "Каждой задаче должен быть назначен агент");
        }
    }

    @Test
    @DisplayName("Анализатор и пул задач должны полностью игнорировать временные файлы и папки (_temporary, .spark-staging, *.tmp)")
    void testAnalyzeAndReplicateIgnoresTemporaryFilesAndFolders() throws Exception {
        // 1. Создаем легитимные файлы
        File valid1 = new File(srcDir, "valid1.parquet");
        Files.writeString(valid1.toPath(), "valid content 1");
        File valid2 = new File(srcDir, "valid2.parquet");
        Files.writeString(valid2.toPath(), "valid content 2");
        File successFile = new File(srcDir, "_SUCCESS");
        Files.writeString(successFile.toPath(), "");
        File metadataFile = new File(srcDir, "_metadata");
        Files.writeString(metadataFile.toPath(), "parquet schema metadata");

        // 2. Создаем временные каталоги и файлы
        File tempSubdir = new File(srcDir, "_temporary/0/task_001");
        tempSubdir.mkdirs();
        File tempTaskFile = new File(tempSubdir, "part-999.parquet");
        Files.writeString(tempTaskFile.toPath(), "temp task data");

        File sparkStaging = new File(srcDir, ".spark-staging-job1");
        sparkStaging.mkdirs();
        File stagingJar = new File(sparkStaging, "app.jar");
        Files.writeString(stagingJar.toPath(), "jar bytes");

        File tmpFile = new File(srcDir, "data.tmp");
        Files.writeString(tmpFile.toPath(), "incomplete data");

        File inprogressFile = new File(srcDir, "stream.inprogress");
        Files.writeString(inprogressFile.toPath(), "writing stream");

        File dsStore = new File(srcDir, ".DS_Store");
        Files.writeString(dsStore.toPath(), "binary trash");

        File copyingFile = new File(srcDir, "part-00000.parquet._copying_");
        Files.writeString(copyingFile.toPath(), "in-flight copying");

        JobDto job = new JobDto();
        job.setId("job-temp-filter-101");
        job.setSourcePath(srcDir.getAbsolutePath());
        job.setTargetPath(targetDir.getAbsolutePath());
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setStatus("QUEUED");

        String targetAddress = "inprocess:" + serverName;

        ReplicationSender analyzerSender = new ReplicationSender(
                "analyzer-agent-dc1",
                orchestratorMock,
                fsManager,
                bandwidthLimiter,
                64 * 1024,
                false, null, null, null, false
        );

        boolean analyzed = analyzerSender.analyzeAndCreateTaskPool(job, targetAddress);
        assertTrue(analyzed, "Анализ должен успешно завершиться");

        // Проверяем: создано ровно 4 задачи в пуле (valid1, valid2, _SUCCESS, _metadata)
        assertEquals(4, orchestratorMock.taskPool.size(),
                "В пуле должны быть только легитимные файлы, временные должны быть отфильтрованы");

        // Проверяем, что ни один временный файл не попал в пул задач
        for (TaskItemDto task : orchestratorMock.taskPool.values()) {
            assertFalse(task.getSourcePath().contains("_temporary"), "Не должно быть _temporary: " + task.getSourcePath());
            assertFalse(task.getSourcePath().contains(".spark-staging"), "Не должно быть .spark-staging: " + task.getSourcePath());
            assertFalse(task.getSourcePath().endsWith(".tmp"), "Не должно быть .tmp: " + task.getSourcePath());
            assertFalse(task.getSourcePath().endsWith(".inprogress"), "Не должно быть .inprogress: " + task.getSourcePath());
            assertFalse(task.getSourcePath().contains("_copying_"), "Не должно быть _copying_: " + task.getSourcePath());
            assertFalse(task.getSourcePath().contains(".DS_Store"), "Не должно быть .DS_Store: " + task.getSourcePath());
        }

        // Выполняем репликацию через воркера
        ReplicationSender worker = new ReplicationSender(
                "worker-dc1-node1",
                orchestratorMock,
                fsManager,
                bandwidthLimiter,
                64 * 1024,
                false, null, null, null, false
        );

        List<TaskItemDto> claimed = orchestratorMock.claimTasks(new ClaimTasksRequest("worker-dc1-node1", "dc1", 10));
        assertEquals(4, claimed.size());
        for (TaskItemDto t : claimed) {
            t.setTargetAddress(targetAddress);
            boolean ok = worker.transferTask(t);
            assertTrue(ok, "Передача задачи " + t.getId() + " должна быть успешной");
        }

        // Проверяем целевую папку
        assertTrue(new File(targetDir, "valid1.parquet").exists());
        assertTrue(new File(targetDir, "valid2.parquet").exists());
        assertTrue(new File(targetDir, "_SUCCESS").exists());
        assertTrue(new File(targetDir, "_metadata").exists());

        // Проверяем отсутствие временных файлов на целевом узле
        assertFalse(new File(targetDir, "_temporary").exists());
        assertFalse(new File(targetDir, ".spark-staging-job1").exists());
        assertFalse(new File(targetDir, "data.tmp").exists());
        assertFalse(new File(targetDir, "stream.inprogress").exists());
        assertFalse(new File(targetDir, ".DS_Store").exists());
        assertFalse(new File(targetDir, "part-00000.parquet._copying_").exists());
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) return;
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) deleteRecursively(f);
            }
        }
        file.delete();
    }

    /**
     * Потокобезопасный in-memory стаб Оркестратора для тестирования распределенного пула задач.
     */
    static class MockDistributedOrchestratorClient extends OrchestratorClient {

        final Map<String, TaskItemDto> taskPool = new ConcurrentHashMap<>();
        final Queue<String> queuedTaskIds = new ConcurrentLinkedQueue<>();

        MockDistributedOrchestratorClient() {
            super("http://localhost:8005", "secret");
        }

        @Override
        public void updateJobProgress(String jobId, UpdateJobRequest request) {}

        @Override
        public double requestNetworkTokens(TokenRequest request) {
            return 0.0;
        }

        @Override
        public boolean batchCreateTasks(BatchCreateTasksRequest request) {
            for (TaskCreateItem item : request.getTasks()) {
                TaskItemDto dto = new TaskItemDto(
                        item.getId(),
                        request.getJobId(),
                        "run-1",
                        item.getSourcePath(),
                        item.getTargetPath(),
                        item.getFileSize(),
                        item.isSkipped() ? "SKIPPED" : "QUEUED",
                        null,
                        null,
                        null,
                        null,
                        null
                );
                taskPool.put(item.getId(), dto);
                if (!item.isSkipped()) {
                    queuedTaskIds.add(item.getId());
                }
            }
            return true;
        }

        @Override
        public synchronized List<TaskItemDto> claimTasks(ClaimTasksRequest request) {
            List<TaskItemDto> claimed = new ArrayList<>();
            int count = 0;
            while (count < request.getLimit() && !queuedTaskIds.isEmpty()) {
                String id = queuedTaskIds.poll();
                TaskItemDto task = taskPool.get(id);
                if (task != null && "QUEUED".equalsIgnoreCase(task.getStatus())) {
                    task.setStatus("RUNNING");
                    task.setAssignedAgentId(request.getAgentId());
                    claimed.add(task);
                    count++;
                }
            }
            return claimed;
        }

        @Override
        public boolean completeTask(CompleteTaskRequest request) {
            TaskItemDto task = taskPool.get(request.getTaskId());
            if (task != null) {
                task.setStatus("COMPLETED");
                task.setChecksum(request.getChecksum());
                task.setAssignedAgentId(request.getAgentId());
                return true;
            }
            return false;
        }

        @Override
        public boolean failTask(FailTaskRequest request) {
            TaskItemDto task = taskPool.get(request.getTaskId());
            if (task != null) {
                task.setStatus("FAILED");
                return true;
            }
            return false;
        }
    }
}
