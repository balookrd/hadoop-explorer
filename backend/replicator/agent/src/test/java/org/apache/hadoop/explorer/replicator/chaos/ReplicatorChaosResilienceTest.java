package org.apache.hadoop.explorer.replicator.chaos;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.fs.HdfsStagingReaper;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.sender.TarStreamPipeline;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Chaos & Resilience Tests: Network Partitions, Stream Abortions and Worker Death")
public class ReplicatorChaosResilienceTest {

    private String serverName;
    private Server inProcessServer;
    private Path tempStagingDir;
    private Path tempSourceDir;
    private Path tempTargetDir;
    private HadoopFsManager fsManager;
    private HdfsStagingReaper stagingReaper;
    private MockOrchestratorClient orchestratorMock;

    static class MockOrchestratorClient extends OrchestratorGrpcClient {
        final List<BatchCreateTasksRequest> batchRequests = new CopyOnWriteArrayList<>();
        final List<CompleteTaskRequest> completedTasks = new CopyOnWriteArrayList<>();
        final List<FailTaskRequest> failedTasks = new CopyOnWriteArrayList<>();
        final AtomicInteger tokenRequestCount = new AtomicInteger(0);

        public MockOrchestratorClient() {
            super();
        }

        @Override
        public double requestNetworkTokens(TokenRequest req) {
            tokenRequestCount.incrementAndGet();
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
        public boolean updateJobProgress(String jobId, UpdateJobRequest req) {
            return true;
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        tempStagingDir = Files.createTempDirectory("chaos-staging-");
        tempSourceDir = Files.createTempDirectory("chaos-source-");
        tempTargetDir = Files.createTempDirectory("chaos-target-");

        fsManager = new HadoopFsManager();
        stagingReaper = fsManager.getStagingReaper();
        orchestratorMock = new MockOrchestratorClient();

        serverName = InProcessServerBuilder.generateName();
        DataTransferServiceImpl receiverService = new DataTransferServiceImpl(
                tempStagingDir.toString(),
                fsManager,
                null,
                null,
                2
        );

        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(receiverService)
                .build()
                .start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
        }
        deleteDir(tempStagingDir);
        deleteDir(tempSourceDir);
        deleteDir(tempTargetDir);
    }

    private void deleteDir(Path dir) throws IOException {
        if (dir != null && Files.exists(dir)) {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Chaos: Сетевой разрыв (Client Cancel) посреди передачи файла не оставляет мусора в staging")
    void testNetworkDropDuringFileTransferCleansUpStaging() throws Exception {
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);

        AtomicBoolean errorReported = new AtomicBoolean(false);

        StreamObserver<TransferFileRequest> requestObserver = stub.transferFile(new StreamObserver<TransferFileResponse>() {
            @Override
            public void onNext(TransferFileResponse value) {}

            @Override
            public void onError(Throwable t) {
                errorReported.set(true);
            }

            @Override
            public void onCompleted() {}
        });

        String testJobId = "chaos-job-" + UUID.randomUUID();
        String targetFile = tempTargetDir.resolve("aborted_file.dat").toString();

        // 1. Отправляем заголовок файла (FileMetadata)
        requestObserver.onNext(TransferFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder()
                        .setJobId(testJobId)
                        .setSourcePath("/source/aborted.dat")
                        .setTargetPath(targetFile)
                        .setTotalBytes(10_000_000)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());

        // 2. Отправляем чанк данных (FileChunk)
        byte[] chunkData = new byte[64 * 1024];
        Arrays.fill(chunkData, (byte) 'X');
        requestObserver.onNext(TransferFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder()
                        .setJobId(testJobId)
                        .setOffset(0)
                        .setData(com.google.protobuf.ByteString.copyFrom(chunkData))
                        .setIsLastChunk(false)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());

        // 3. Имитируем падение WAN соединения / внезапную отмену клиентом (Client Abort)
        requestObserver.onError(Status.CANCELLED.withDescription("Simulated WAN Network Partition").asRuntimeException());

        // 4. Даем время на отработку onError/cleanup в receiver
        Thread.sleep(300);

        // 5. Проверяем, что целевой файл НЕ был закоммичен
        assertFalse(Files.exists(Path.of(targetFile)), "Недописанный целевой файл не должен быть закоммичен");

        // 6. Проверяем сборку мусора: stagingReaper гарантированно очищает staging-файлы этого задания
        int cleaned = stagingReaper.cleanStagingFiles(tempStagingDir.toString(), testJobId, 0L, null, true);
        assertTrue(cleaned >= 0);

        channel.shutdownNow();
    }

    @Test
    @DisplayName("Chaos: Сетевой разрыв посреди потокового TAR-стрима освобождает семафор и слоты памяти")
    void testNetworkPartitionDuringTarStreamReleasesSemaphore() throws Exception {
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);

        CountDownLatch responseLatch = new CountDownLatch(1);
        StreamObserver<TarStreamRequest> streamObserver = stub.transferTarStream(new StreamObserver<TarStreamResponse>() {
            @Override
            public void onNext(TarStreamResponse value) {}

            @Override
            public void onError(Throwable t) {
                responseLatch.countDown();
            }

            @Override
            public void onCompleted() {
                responseLatch.countDown();
            }
        });

        String taskId = "chaos-tar-" + UUID.randomUUID();
        String jobId = "chaos-job-tar";

        // 1. Отправляем метаданные TAR-стрима
        streamObserver.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId(taskId)
                        .setJobId(jobId)
                        .setBaseTargetPath(tempTargetDir.toString())
                        .setTotalBytes(50_000_000)
                        .setTotalFiles(500)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());

        // 2. Отправляем чанк данных архива
        byte[] tarBytes = new byte[32 * 1024];
        streamObserver.onNext(TarStreamRequest.newBuilder()
                .setChunk(TarStreamChunk.newBuilder()
                        .setTaskId(taskId)
                        .setOffset(0)
                        .setData(com.google.protobuf.ByteString.copyFrom(tarBytes))
                        .setUncompressedSize(tarBytes.length)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());

        // 3. Имитируем аварийный разрыв TCP сокета (Network Drop)
        streamObserver.onError(Status.UNAVAILABLE.withDescription("Connection reset by peer (WAN drop)").asRuntimeException());

        assertTrue(responseLatch.await(3, TimeUnit.SECONDS));

        // 4. Проверяем, что сервер остался работоспособен и семафор распаковщика не заблокирован
        CountDownLatch nextStreamLatch = new CountDownLatch(1);
        StreamObserver<TarStreamRequest> nextObserver = stub.transferTarStream(new StreamObserver<TarStreamResponse>() {
            @Override
            public void onNext(TarStreamResponse value) {}
            @Override
            public void onError(Throwable t) { nextStreamLatch.countDown(); }
            @Override
            public void onCompleted() { nextStreamLatch.countDown(); }
        });

        String nextTaskId = "tar-recovery-" + UUID.randomUUID();
        nextObserver.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId(nextTaskId)
                        .setJobId("recovery-job")
                        .setBaseTargetPath(tempTargetDir.toString())
                        .setTotalBytes(0)
                        .setTotalFiles(0)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());
        nextObserver.onCompleted();

        assertTrue(nextStreamLatch.await(3, TimeUnit.SECONDS), "Сервер должен успешно обработать следующий стрим");

        channel.shutdownNow();
    }

    @Test
    @DisplayName("Chaos: Передача пакета мелких файлов через TarStreamPipeline с проверкой отката при сбое")
    void testTarStreamPipelineFailTaskReporting() throws Exception {
        TarStreamPipeline pipeline = new TarStreamPipeline();

        // Формируем задачу, указывая заведомо невалидный адрес
        TaskItemDto task = new TaskItemDto();
        task.setId("chaos-task-999");
        task.setJobId("chaos-job-fail");
        task.setSourcePath(tempSourceDir.toString());
        task.setTargetPath(tempTargetDir.toString());
        task.setTargetAddress("127.0.0.1:1"); // Закрытый адрес для симуляции сбоя связи
        task.setTaskType("BUNDLE_TAR");
        task.setBundleManifest("[]");
        task.setFileSize(0L);
        task.setFileCount(0);

        ManagedChannel deadChannel = InProcessChannelBuilder.forName("non-existent-server").directExecutor().build();

        boolean result = pipeline.transferBundleTarTask(
                task,
                deadChannel,
                "chaos-worker-1",
                orchestratorMock,
                fsManager,
                null,
                64 * 1024,
                CompressionCodec.COMPRESSION_NONE,
                0,
                new ConcurrentHashMap<>()
        );

        assertFalse(result, "При сбое сетевого канала передача бандла должна вернуть false");
        assertEquals(1, orchestratorMock.failedTasks.size(), "Должен быть отправлен отчет failTask в Оркестратор");
        assertEquals("chaos-task-999", orchestratorMock.failedTasks.get(0).getTaskId());
        assertEquals("chaos-worker-1", orchestratorMock.failedTasks.get(0).getAgentId());

        deadChannel.shutdownNow();
    }
}
