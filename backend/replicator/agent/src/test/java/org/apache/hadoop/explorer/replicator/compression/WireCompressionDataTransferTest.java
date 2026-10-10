package org.apache.hadoop.explorer.replicator.compression;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.model.TaskItemDto;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.sender.ReplicationSender;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WireCompressionDataTransferTest {

    private String serverName;
    private Server inProcessServer;
    private ManagedChannel inProcessChannel;
    private File tempStagingDir;
    private File tempTargetDir;
    private File tempSourceDir;
    private HadoopFsManager fsManager;
    private MockOrchestratorClient orchestratorClient;

    static class MockOrchestratorClient extends OrchestratorClient {
        public MockOrchestratorClient() {
            super("http://localhost:8005", "secret", true);
        }

        @Override
        public double requestNetworkTokens(org.apache.hadoop.explorer.replicator.model.TokenRequest req) {
            return 0.0;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        serverName = InProcessServerBuilder.generateName();
        tempStagingDir = Files.createTempDirectory("repl-wire-staging-").toFile();
        tempTargetDir = Files.createTempDirectory("repl-wire-target-").toFile();
        tempSourceDir = Files.createTempDirectory("repl-wire-source-").toFile();

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

        inProcessChannel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        orchestratorClient = new MockOrchestratorClient();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (inProcessChannel != null) {
            inProcessChannel.shutdownNow();
        }
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
        }
        deleteRecursively(tempStagingDir);
        deleteRecursively(tempTargetDir);
        deleteRecursively(tempSourceDir);
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
    @DisplayName("Wire Compression: Zstandard сжатие чанков при потоковой передаче в gRPC")
    void testZstdWireCompressionStreaming() throws Exception {
        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(inProcessChannel);

        File targetFile = new File(tempTargetDir, "system_audit.log");
        String logContent = "2026-10-10 10:15:32 [AUDIT] user=data_engineer_42 action=SELECT table=analytics.sales_q3 cluster=DC1 status=SUCCESS latency_ms=14\n".repeat(200);
        byte[] uncompressedBytes = logContent.getBytes(StandardCharsets.UTF_8);

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hash = md.digest(uncompressedBytes);
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) sb.append(String.format("%02x", b));
        String expectedChecksum = sb.toString();

        CompletableFuture<TransferFileResponse> responseFuture = new CompletableFuture<>();
        StreamObserver<TransferFileRequest> requestObserver = stub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {
                responseFuture.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                responseFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {}
        });

        // 1. Метаданные файла
        FileMetadata metadata = FileMetadata.newBuilder()
                .setJobId("job-zstd-1")
                .setSourcePath("/logs/system_audit.log")
                .setTargetPath(targetFile.getAbsolutePath())
                .setTotalBytes(uncompressedBytes.length)
                .setCompressionCodec(CompressionCodec.COMPRESSION_ZSTD)
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

        // 2. Сжатие чанка через WireCompressor
        WireCompressor.CompressedPayload payload = WireCompressor.compress(
                uncompressedBytes, 0, uncompressedBytes.length, CompressionCodec.COMPRESSION_ZSTD, 3
        );

        assertEquals(CompressionCodec.COMPRESSION_ZSTD, payload.appliedCodec());
        assertTrue(payload.bytes().length < uncompressedBytes.length / 3,
                "Zstd должен сжать повторяющийся текстовый лог более чем на 66% (исходный: "
                        + uncompressedBytes.length + ", сжатый: " + payload.bytes().length + ")");

        FileChunk chunk = FileChunk.newBuilder()
                .setJobId("job-zstd-1")
                .setOffset(0)
                .setData(ByteString.copyFrom(payload.bytes()))
                .setIsLastChunk(true)
                .setCompressionCodec(payload.appliedCodec())
                .setUncompressedSize(payload.uncompressedSize())
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setChunk(chunk).build());
        requestObserver.onCompleted();

        TransferFileResponse response = responseFuture.get(5, TimeUnit.SECONDS);
        assertTrue(response.getSuccess(), "Передача со сжатием должна завершиться успешно");
        assertEquals(uncompressedBytes.length, response.getBytesWritten(), "Приемник должен записать оригинальный объем несжатого файла");
        assertEquals(expectedChecksum, response.getChecksum(), "SHA-256 контрольная сумма должна совпадать с оригинальной");

        // Проверяем содержимое файла на диске приемника
        byte[] writtenBytes = Files.readAllBytes(targetFile.toPath());
        assertArrayEquals(uncompressedBytes, writtenBytes, "Содержимое файла должно байт-в-байт совпадать с оригиналом");
    }

    @Test
    @DisplayName("Wire Compression: LZ4 сжатие чанков при потоковой передаче в gRPC")
    void testLz4WireCompressionStreaming() throws Exception {
        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(inProcessChannel);

        File targetFile = new File(tempTargetDir, "metrics.csv");
        String csvContent = "metric_id,host,timestamp,cpu_usage,mem_usage,disk_io\n" +
                "cpu_idle,dc1-worker-101.local,1760000000,98.2,14.5,120\n".repeat(300);
        byte[] uncompressedBytes = csvContent.getBytes(StandardCharsets.UTF_8);

        CompletableFuture<TransferFileResponse> responseFuture = new CompletableFuture<>();
        StreamObserver<TransferFileRequest> requestObserver = stub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {
                responseFuture.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                responseFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {}
        });

        FileMetadata metadata = FileMetadata.newBuilder()
                .setJobId("job-lz4-1")
                .setSourcePath("/metrics/metrics.csv")
                .setTargetPath(targetFile.getAbsolutePath())
                .setTotalBytes(uncompressedBytes.length)
                .setCompressionCodec(CompressionCodec.COMPRESSION_LZ4)
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

        WireCompressor.CompressedPayload payload = WireCompressor.compress(
                uncompressedBytes, 0, uncompressedBytes.length, CompressionCodec.COMPRESSION_LZ4, 0
        );

        assertEquals(CompressionCodec.COMPRESSION_LZ4, payload.appliedCodec());
        assertTrue(payload.bytes().length < uncompressedBytes.length / 2, "LZ4 должен сократить размер CSV более чем в 2 раза");

        FileChunk chunk = FileChunk.newBuilder()
                .setJobId("job-lz4-1")
                .setOffset(0)
                .setData(ByteString.copyFrom(payload.bytes()))
                .setIsLastChunk(true)
                .setCompressionCodec(payload.appliedCodec())
                .setUncompressedSize(payload.uncompressedSize())
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setChunk(chunk).build());
        requestObserver.onCompleted();

        TransferFileResponse response = responseFuture.get(5, TimeUnit.SECONDS);
        assertTrue(response.getSuccess());
        assertEquals(uncompressedBytes.length, response.getBytesWritten());

        byte[] writtenBytes = Files.readAllBytes(targetFile.toPath());
        assertArrayEquals(uncompressedBytes, writtenBytes);
    }

    @Test
    @DisplayName("Автоматическое отключение сжатия для уже сжатых форматов (Parquet, ORC, Gz)")
    void testPrecompressedFileSkipsCompression() throws Exception {
        // Создаем отправитель со включенным Zstandard сжатием
        ReplicationSender sender = new ReplicationSender(
                "sender-1",
                orchestratorClient,
                fsManager,
                null,
                64 * 1024,
                false, null, null, null, false,
                0L, 16 * 1024 * 1024L, 500,
                CompressionCodec.COMPRESSION_ZSTD,
                3
        );

        // Исходный файл с расширением .parquet
        File parquetSource = new File(tempSourceDir, "analytics_orders.parquet");
        byte[] dummyData = "PARQUET_HEADER_DATA_1234567890".repeat(50).getBytes(StandardCharsets.UTF_8);
        try (FileOutputStream fos = new FileOutputStream(parquetSource)) {
            fos.write(dummyData);
        }

        File parquetTarget = new File(tempTargetDir, "analytics_orders.parquet");

        // Проверяем резолвер
        CompressionCodec resolved = WireCompressor.resolveCodecForPath(
                parquetSource.getAbsolutePath(),
                sender.getDefaultCompressionCodec()
        );
        assertEquals(CompressionCodec.COMPRESSION_NONE, resolved,
                "Для файлов *.parquet кодек должен автоматически сбрасываться в NONE");

        // Проверяем также для ORC, Gz, Snappy
        assertEquals(CompressionCodec.COMPRESSION_NONE,
                WireCompressor.resolveCodecForPath("/warehouse/table.orc", CompressionCodec.COMPRESSION_ZSTD));
        assertEquals(CompressionCodec.COMPRESSION_NONE,
                WireCompressor.resolveCodecForPath("/logs/archive.gz", CompressionCodec.COMPRESSION_ZSTD));
        assertEquals(CompressionCodec.COMPRESSION_NONE,
                WireCompressor.resolveCodecForPath("/data/events.snappy", CompressionCodec.COMPRESSION_ZSTD));
    }

    @Test
    @DisplayName("TAR-стрим: прозрачное сжатие пакета мелких файлов JSON/логов на лету")
    void testTarStreamWithWireCompression() throws Exception {
        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(inProcessChannel);

        File targetBaseDir = new File(tempTargetDir, "small_files_bundle");
        targetBaseDir.mkdirs();

        int fileCount = 5;
        List<String> fileNames = new ArrayList<>();
        List<byte[]> fileContents = new ArrayList<>();
        long totalBytes = 0;

        for (int i = 0; i < fileCount; i++) {
            String name = "file_" + i + ".json";
            fileNames.add(name);
            String content = "{\"event_id\":" + i + ",\"type\":\"TRANSACTION\",\"data\":\"Some transaction payload message\"}\n".repeat(100);
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            fileContents.add(bytes);
            totalBytes += bytes.length;
        }

        CompletableFuture<TarStreamResponse> responseFuture = new CompletableFuture<>();
        StreamObserver<TarStreamRequest> requestObserver = stub.transferTarStream(new StreamObserver<>() {
            @Override
            public void onNext(TarStreamResponse value) {
                responseFuture.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                responseFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {}
        });

        // 1. Метаданные бандла с кодеком Zstandard
        TarStreamMetadata metadata = TarStreamMetadata.newBuilder()
                .setJobId("job-tar-zstd")
                .setTaskId("task-tar-zstd-1")
                .setBaseTargetPath(targetBaseDir.getAbsolutePath())
                .setTotalFiles(fileCount)
                .setTotalBytes(totalBytes)
                .setCompressionCodec(CompressionCodec.COMPRESSION_ZSTD)
                .build();
        requestObserver.onNext(TarStreamRequest.newBuilder().setMetadata(metadata).build());

        // 2. Упаковка в TAR с потоковым сжатием чанков через TarChunkingOutputStream
        AtomicInteger chunksSent = new AtomicInteger(0);
        ReplicationSender.TarChunkingOutputStream chunkingOut = new ReplicationSender.TarChunkingOutputStream(
                "task-tar-zstd-1",
                new StreamObserver<>() {
                    @Override
                    public void onNext(TarStreamRequest value) {
                        chunksSent.incrementAndGet();
                        requestObserver.onNext(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        requestObserver.onError(t);
                    }

                    @Override
                    public void onCompleted() {}
                },
                32 * 1024,
                null,
                orchestratorClient,
                "worker-tar-1",
                CompressionCodec.COMPRESSION_ZSTD,
                3
        );

        try (org.apache.commons.compress.archivers.tar.TarArchiveOutputStream tarOut =
                     new org.apache.commons.compress.archivers.tar.TarArchiveOutputStream(chunkingOut)) {
            for (int i = 0; i < fileCount; i++) {
                org.apache.commons.compress.archivers.tar.TarArchiveEntry entry =
                        new org.apache.commons.compress.archivers.tar.TarArchiveEntry(fileNames.get(i));
                byte[] bytes = fileContents.get(i);
                entry.setSize(bytes.length);
                tarOut.putArchiveEntry(entry);
                tarOut.write(bytes);
                tarOut.closeArchiveEntry();
            }
            tarOut.finish();
            tarOut.flush();
        }
        chunkingOut.flush();
        requestObserver.onCompleted();

        TarStreamResponse resp = responseFuture.get(5, TimeUnit.SECONDS);
        assertTrue(resp.getSuccess(), "TAR стрим со сжатием должен успешно завершиться");
        assertEquals(fileCount, resp.getFilesCommitted(), "Все 5 файлов должны быть успешно закоммичены");
        assertEquals(totalBytes, resp.getBytesWritten(), "Суммарный объем записанных файлов должен совпадать");

        // Проверяем, что все файлы распаковались на диск целевой директории
        for (int i = 0; i < fileCount; i++) {
            File saved = new File(targetBaseDir, fileNames.get(i));
            assertTrue(saved.exists(), "Файл должен существовать: " + saved.getAbsolutePath());
            assertArrayEquals(fileContents.get(i), Files.readAllBytes(saved.toPath()));
        }
    }

    @Test
    @DisplayName("Обратная совместимость: прием чанка COMPRESSION_NONE от старого клиента")
    void testBackwardCompatibilityWithUncompressedChunk() throws Exception {
        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(inProcessChannel);

        File targetFile = new File(tempTargetDir, "legacy.txt");
        byte[] legacyBytes = "Data from legacy uncompressed replicator agent version 0.9.0".getBytes(StandardCharsets.UTF_8);

        CompletableFuture<TransferFileResponse> responseFuture = new CompletableFuture<>();
        StreamObserver<TransferFileRequest> requestObserver = stub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {
                responseFuture.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                responseFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {}
        });

        // Метаданные без явного задания кодека (COMPRESSION_NONE по умолчанию)
        FileMetadata metadata = FileMetadata.newBuilder()
                .setJobId("job-legacy-1")
                .setSourcePath("/tmp/legacy.txt")
                .setTargetPath(targetFile.getAbsolutePath())
                .setTotalBytes(legacyBytes.length)
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

        // Чанк без кодека (COMPRESSION_NONE по умолчанию)
        FileChunk chunk = FileChunk.newBuilder()
                .setJobId("job-legacy-1")
                .setOffset(0)
                .setData(ByteString.copyFrom(legacyBytes))
                .setIsLastChunk(true)
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setChunk(chunk).build());
        requestObserver.onCompleted();

        TransferFileResponse response = responseFuture.get(5, TimeUnit.SECONDS);
        assertTrue(response.getSuccess());
        assertEquals(legacyBytes.length, response.getBytesWritten());
        assertArrayEquals(legacyBytes, Files.readAllBytes(targetFile.toPath()));
    }
}
