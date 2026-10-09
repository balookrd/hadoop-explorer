package org.apache.hadoop.explorer.replicator.sender;

import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.model.UpdateJobRequest;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class DirectoryIncrementalSyncTest {

    private String serverName;
    private Server inProcessServer;
    private File tempStagingDir;
    private File tempSourceDir;
    private File tempTargetDir;
    private HadoopFsManager fsManager;
    private TestOrchestratorClient orchestratorClient;
    private ReplicationSender sender;

    static class TestOrchestratorClient extends OrchestratorClient {
        final List<UpdateJobRequest> recordedUpdates = new java.util.concurrent.CopyOnWriteArrayList<>();

        public TestOrchestratorClient() {
            super("http://localhost:8005", "test-secret", true);
        }

        @Override
        public double requestNetworkTokens(org.apache.hadoop.explorer.replicator.model.TokenRequest req) {
            return 0.0;
        }

        @Override
        public void updateJobProgress(String jobId, UpdateJobRequest req) {
            recordedUpdates.add(req);
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        serverName = "test-sync-server-" + System.nanoTime();
        tempStagingDir = Files.createTempDirectory("sync-staging-").toFile();
        tempSourceDir = Files.createTempDirectory("sync-src-").toFile();
        tempTargetDir = Files.createTempDirectory("sync-dst-").toFile();

        fsManager = new HadoopFsManager(null, null, null);
        LocalBandwidthLimiter limiter = new LocalBandwidthLimiter(0.0);

        DataTransferServiceImpl service = new DataTransferServiceImpl(
                tempStagingDir.getAbsolutePath(),
                fsManager,
                limiter
        );

        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(service)
                .build()
                .start();

        orchestratorClient = new TestOrchestratorClient();

        sender = new ReplicationSender(
                "test-worker-1",
                orchestratorClient,
                fsManager,
                limiter,
                64 * 1024
        );
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

    private void deleteRecursively(File f) {
        if (f != null && f.exists()) {
            File[] files = f.listFiles();
            if (files != null) {
                for (File sub : files) deleteRecursively(sub);
            }
            f.delete();
        }
    }

    @Test
    public void testRecursiveDirectorySyncAndIncrementalSkip() throws Exception {
        // 1. Создаем структуру файлов в источнике
        File f1 = new File(tempSourceDir, "file1.txt");
        Files.writeString(f1.toPath(), "Content of file 1");

        File subDir = new File(tempSourceDir, "nested");
        subDir.mkdirs();
        File f2 = new File(subDir, "file2.parquet");
        Files.writeString(f2.toPath(), "Parquet simulated binary content");

        JobDto job = new JobDto();
        job.setId("job-dir-sync-1");
        job.setSourcePath(tempSourceDir.getAbsolutePath());
        job.setTargetPath(tempTargetDir.getAbsolutePath());
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");

        // 2. Первый запуск: полная передача всех файлов каталога
        boolean firstSuccess = sender.transferFile(job, "inprocess:" + serverName);
        assertTrue(firstSuccess, "Первая передача каталога должна завершиться успешно");

        // Проверяем, что файлы появились в целевом каталоге
        File targetF1 = new File(tempTargetDir, "file1.txt");
        File targetF2 = new File(tempTargetDir, "nested/file2.parquet");
        assertTrue(targetF1.exists(), "targetF1 должен существовать на приемнике");
        assertTrue(targetF2.exists(), "targetF2 должен существовать на приемнике");
        assertEquals("Content of file 1", Files.readString(targetF1.toPath()));
        assertEquals("Parquet simulated binary content", Files.readString(targetF2.toPath()));

        assertFalse(orchestratorClient.recordedUpdates.isEmpty());
        UpdateJobRequest lastReq = orchestratorClient.recordedUpdates.get(orchestratorClient.recordedUpdates.size() - 1);
        assertEquals("COMPLETED", lastReq.getStatus());
        assertTrue(lastReq.getMessage().contains("передано 2, пропущено 0"),
                "В первом запуске должны быть переданы 2 файла и пропущено 0: " + lastReq.getMessage());

        // 3. Второй запуск: повторная синхронизация того же каталога без изменений
        orchestratorClient.recordedUpdates.clear();

        boolean secondSuccess = sender.transferFile(job, "inprocess:" + serverName);
        assertTrue(secondSuccess, "Второй запуск должен завершиться успешно");

        assertFalse(orchestratorClient.recordedUpdates.isEmpty());
        UpdateJobRequest lastReq2 = orchestratorClient.recordedUpdates.get(orchestratorClient.recordedUpdates.size() - 1);
        assertEquals("COMPLETED", lastReq2.getStatus());
        assertTrue(lastReq2.getMessage().contains("передано 0, пропущено 2"),
                "Во втором запуске файлы должны быть инкрементально пропущены (передано 0, пропущено 2): " + lastReq2.getMessage());

        // 4. Добавляем третий файл
        File f3 = new File(subDir, "file3.json");
        Files.writeString(f3.toPath(), "{\"status\": \"new_event\"}");

        orchestratorClient.recordedUpdates.clear();

        boolean thirdSuccess = sender.transferFile(job, "inprocess:" + serverName);
        assertTrue(thirdSuccess, "Третий запуск должен завершиться успешно");

        File targetF3 = new File(tempTargetDir, "nested/file3.json");
        assertTrue(targetF3.exists(), "Новый файл f3 должен появиться на приемнике");

        assertFalse(orchestratorClient.recordedUpdates.isEmpty());
        UpdateJobRequest lastReq3 = orchestratorClient.recordedUpdates.get(orchestratorClient.recordedUpdates.size() - 1);
        assertEquals("COMPLETED", lastReq3.getStatus());
        assertTrue(lastReq3.getMessage().contains("передано 1, пропущено 2"),
                "В третьем запуске должен передаться только новый файл (передано 1, пропущено 2): " + lastReq3.getMessage());
    }
}
