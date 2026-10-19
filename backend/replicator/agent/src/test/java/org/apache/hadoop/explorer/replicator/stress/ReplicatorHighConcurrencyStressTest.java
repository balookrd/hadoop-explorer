package org.apache.hadoop.explorer.replicator.stress;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.compression.WireCompressor;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.metrics.AgentMetrics;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.sender.TarStreamPipeline;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Stress & High Concurrency Tests: 500+ Small Files Streaming TAR, OOM Backpressure and Wire Compressor")
public class ReplicatorHighConcurrencyStressTest {

    private String serverName;
    private Server inProcessServer;
    private Path tempStagingDir;
    private Path tempSourceDir;
    private Path tempTargetDir;
    private HadoopFsManager fsManager;
    private DataTransferServiceImpl receiverService;
    private MockStressOrchestratorClient orchestratorMock;

    static class MockStressOrchestratorClient extends OrchestratorGrpcClient {
        final List<CompleteTaskRequest> completedTasks = new CopyOnWriteArrayList<>();
        final List<FailTaskRequest> failedTasks = new CopyOnWriteArrayList<>();

        @Override
        public double requestNetworkTokens(TokenRequest req) {
            return 0.0;
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
        tempStagingDir = Files.createTempDirectory("stress-staging-");
        tempSourceDir = Files.createTempDirectory("stress-source-");
        tempTargetDir = Files.createTempDirectory("stress-target-");

        fsManager = new HadoopFsManager();
        orchestratorMock = new MockStressOrchestratorClient();

        serverName = InProcessServerBuilder.generateName();
        // Ограничиваем пул параллельных потоков TAR до 2 с быстрым таймаутом ожидания слота (200 мс) для проверки backpressure
        receiverService = new DataTransferServiceImpl(
                tempStagingDir.toString(),
                fsManager,
                null,
                null,
                4,
                2,
                512 * 1024,
                200L
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
    @DisplayName("Stress: Потоковая передача пакета из 500 мелких файлов через TarStreamPipeline с пофайловой сверкой")
    void testMassiveSmallFilesStreamingTarTransfer() throws Exception {
        int fileCount = 500;
        List<org.apache.hadoop.explorer.replicator.sender.ReplicationSender.BundleFileEntry> manifestList = new ArrayList<>(fileCount);
        long totalBytes = 0;

        // Генерируем 500 файлов в различных подпапках
        for (int i = 0; i < fileCount; i++) {
            String subDirName = "sub_" + (i % 10);
            Path subDir = tempSourceDir.resolve(subDirName);
            Files.createDirectories(subDir);

            String fileName = "small_file_" + i + ".txt";
            Path filePath = subDir.resolve(fileName);
            String content = "Data-Line-Payload-" + i + "-" + UUID.randomUUID() + "\n";
            byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
            Files.write(filePath, contentBytes);

            totalBytes += contentBytes.length;
            String relPath = subDirName + "/" + fileName;

            manifestList.add(new org.apache.hadoop.explorer.replicator.sender.ReplicationSender.BundleFileEntry(
                    filePath.toString(),
                    relPath,
                    contentBytes.length
            ));
        }

        String manifestJson = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(manifestList);

        TaskItemDto task = new TaskItemDto();
        task.setId("stress-task-500");
        task.setJobId("stress-job-bundle");
        task.setSourcePath(tempSourceDir.toString());
        task.setTargetPath(tempTargetDir.toString());
        task.setTaskType("BUNDLE_TAR");
        task.setBundleManifest(manifestJson);
        task.setFileSize(totalBytes);
        task.setFileCount(fileCount);

        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        TarStreamPipeline pipeline = new TarStreamPipeline();

        boolean result = pipeline.transferBundleTarTask(
                task,
                channel,
                "stress-worker-1",
                orchestratorMock,
                fsManager,
                null,
                64 * 1024,
                CompressionCodec.COMPRESSION_LZ4,
                1,
                new ConcurrentHashMap<>()
        );

        assertTrue(result, "Передача 500 файлов через виртуальный TAR-поток должна завершиться успешно");
        assertEquals(1, orchestratorMock.completedTasks.size(), "Должен быть отправлен отчет completeTask в Оркестратор");
        assertEquals("stress-task-500", orchestratorMock.completedTasks.get(0).getTaskId());
        assertEquals(fileCount, orchestratorMock.completedTasks.get(0).getFilesCount());
        assertEquals(totalBytes, orchestratorMock.completedTasks.get(0).getBytesTransferred());

        // Проверяем физическое присутствие и идентичность всех 500 файлов на целевой стороне
        for (int i = 0; i < fileCount; i++) {
            String relPath = "sub_" + (i % 10) + "/small_file_" + i + ".txt";
            Path targetFile = tempTargetDir.resolve(relPath);
            assertTrue(Files.exists(targetFile), "Целевой файл " + relPath + " должен существовать в целевой директории");
            byte[] expected = Files.readAllBytes(tempSourceDir.resolve(relPath));
            byte[] actual = Files.readAllBytes(targetFile);
            assertArrayEquals(expected, actual, "Содержимое файла " + relPath + " должно полностью совпадать");
        }

        // Проверяем отсутствие оставшихся staging-файлов
        try (var s = Files.walk(tempStagingDir)) {
            assertEquals(1, s.count(), "В staging-директории не должно оставаться временных файлов");
        }

        channel.shutdownNow();
    }

    @Test
    @DisplayName("Stress: Защита памяти (OOM Backpressure) — отказ при превышении лимита параллельных TAR-потоков")
    void testConcurrentTarStreamSaturationAndRejection() throws Exception {
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);

        CountDownLatch stream1Started = new CountDownLatch(1);
        CountDownLatch stream2Started = new CountDownLatch(1);
        CountDownLatch stream3ResponseLatch = new CountDownLatch(1);
        AtomicBoolean stream3Rejected = new AtomicBoolean(false);

        // Поток 1 (занимает слот 1 из 2)
        StreamObserver<TarStreamRequest> stream1 = stub.transferTarStream(new StreamObserver<TarStreamResponse>() {
            @Override public void onNext(TarStreamResponse value) {}
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });
        stream1.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId("slot-task-1")
                        .setJobId("job-slot-1")
                        .setBaseTargetPath(tempTargetDir.toString())
                        .setTotalFiles(10)
                        .setTotalBytes(1024)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());
        stream1Started.countDown();

        // Поток 2 (занимает слот 2 из 2)
        StreamObserver<TarStreamRequest> stream2 = stub.transferTarStream(new StreamObserver<TarStreamResponse>() {
            @Override public void onNext(TarStreamResponse value) {}
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });
        stream2.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId("slot-task-2")
                        .setJobId("job-slot-2")
                        .setBaseTargetPath(tempTargetDir.toString())
                        .setTotalFiles(10)
                        .setTotalBytes(1024)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());
        stream2Started.countDown();

        // Ожидаем занятия обоих слотов
        assertEquals(0, receiverService.getAvailableTarStreamSlots(), "Все слоты распаковщика должны быть заняты");

        // Поток 3 (должен получить отказ из-за нехватки памяти/слотов)
        StreamObserver<TarStreamRequest> stream3 = stub.transferTarStream(new StreamObserver<TarStreamResponse>() {
            @Override
            public void onNext(TarStreamResponse value) {
                if (!value.getSuccess() && value.getMessage().contains("перегружен")) {
                    stream3Rejected.set(true);
                }
                stream3ResponseLatch.countDown();
            }
            @Override public void onError(Throwable t) { stream3ResponseLatch.countDown(); }
            @Override public void onCompleted() { stream3ResponseLatch.countDown(); }
        });

        stream3.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId("slot-task-3-overflow")
                        .setJobId("job-slot-3")
                        .setBaseTargetPath(tempTargetDir.toString())
                        .setTotalFiles(1)
                        .setTotalBytes(100)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());

        assertTrue(stream3ResponseLatch.await(5, TimeUnit.SECONDS), "Перегруженный стрим должен быстро получить отказ");
        assertTrue(stream3Rejected.get(), "Третий поток должен быть отклонен сервером из-за исчерпания слотов памяти");

        // Завершаем Поток 1
        stream1.onCompleted();
        Thread.sleep(100);

        // Проверяем, что слот освободился
        assertTrue(receiverService.getAvailableTarStreamSlots() >= 1, "После завершения потока слот должен освободиться");

        // Завершаем Поток 2
        stream2.onCompleted();
        channel.shutdownNow();
    }

    @Test
    @DisplayName("Stress: WireCompressor под высокой нагрузкой (10 000 чанков ZSTD / LZ4) и устойчивость к порче данных")
    void testWireCompressionThroughputStress() {
        int iterations = 10_000;
        byte[] original = "Hadoop-Explorer-WireCompression-Stress-Payload-Test-String-1234567890".repeat(5).getBytes(StandardCharsets.UTF_8);

        // 1. Стресс сжатия и распаковки LZ4
        for (int i = 0; i < iterations; i++) {
            WireCompressor.CompressedPayload payload = WireCompressor.compress(
                    original, 0, original.length, CompressionCodec.COMPRESSION_LZ4, 1
            );
            assertTrue(payload.bytes().length > 0 && payload.bytes().length <= original.length + 64);
            byte[] decompressed = WireCompressor.decompress(
                    payload.bytes(), payload.appliedCodec(), payload.uncompressedSize()
            );
            assertArrayEquals(original, decompressed);
        }

        // 2. Стресс сжатия и распаковки ZSTD
        for (int i = 0; i < 1_000; i++) {
            WireCompressor.CompressedPayload payload = WireCompressor.compress(
                    original, 0, original.length, CompressionCodec.COMPRESSION_ZSTD, 3
            );
            assertTrue(payload.bytes().length > 0);
            byte[] decompressed = WireCompressor.decompress(
                    payload.bytes(), payload.appliedCodec(), payload.uncompressedSize()
            );
            assertArrayEquals(original, decompressed);
        }

        // 3. Проверка устойчивости к поврежденным чанкам данных (Corrupted payload)
        byte[] corrupted = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        assertThrows(RuntimeException.class, () ->
                WireCompressor.decompress(corrupted, CompressionCodec.COMPRESSION_LZ4, 1024)
        );
        assertThrows(RuntimeException.class, () ->
                WireCompressor.decompress(corrupted, CompressionCodec.COMPRESSION_ZSTD, 1024)
        );
    }
}
