package io.github.balookrd.pristan.parom.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import io.github.balookrd.pristan.parom.client.OrchestratorGrpcClient;
import io.github.balookrd.pristan.parom.fs.HadoopFsManager;
import io.github.balookrd.pristan.parom.generated.DataTransferServiceGrpc;
import io.github.balookrd.pristan.parom.generated.TarStreamChunk;
import io.github.balookrd.pristan.parom.generated.TarStreamMetadata;
import io.github.balookrd.pristan.parom.generated.TarStreamRequest;
import io.github.balookrd.pristan.parom.generated.TarStreamResponse;
import io.github.balookrd.pristan.parom.model.TaskItemDto;
import io.github.balookrd.pristan.parom.sender.ReplicationSender;
import io.github.balookrd.pristan.parom.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integrity and attributes of small files sent as a TAR bundle.
 *
 * <p>The receiver renamed every unpacked file to its target path as soon as it was read from the
 * stream and compared nothing with the sender, so a corrupted bundle was committed. Bundled files
 * also got the default mode and the time of the copy.
 */
@DisplayName("TAR-бандл: сверка с отправителем и перенос атрибутов")
class TarBundleIntegrityTest {

    /** Не кратно секунде: заголовок ustar хранит mtime в секундах и миллисекунды отбросил бы. */
    private static final long SOURCE_MTIME = 1_700_000_000_123L;

    @TempDir
    Path tmp;

    private DataTransferServiceImpl service;
    private Server server;

    @BeforeEach
    void setUp() {
        service = new DataTransferServiceImpl(tmp.resolve("staging").toString(),
                new HadoopFsManager(null, null, null), new LocalBandwidthLimiter(0.0));
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.shutdownNow();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static byte[] tarOf(String name, String content) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(out)) {
            byte[] data = content.getBytes(StandardCharsets.UTF_8);
            TarArchiveEntry entry = new TarArchiveEntry(name);
            entry.setSize(data.length);
            tar.putArchiveEntry(entry);
            tar.write(data);
            tar.closeArchiveEntry();
            tar.finish();
        }
        return out.toByteArray();
    }

    private TarStreamResponse sendBundle(Path targetDir, byte[] tar, TarStreamChunk trailer) throws Exception {
        CompletableFuture<TarStreamResponse> response = new CompletableFuture<>();
        StreamObserver<TarStreamRequest> request = service.transferTarStream(new StreamObserver<>() {
            @Override public void onNext(TarStreamResponse value) { response.complete(value); }
            @Override public void onError(Throwable t) { response.completeExceptionally(t); }
            @Override public void onCompleted() { }
        });
        request.onNext(TarStreamRequest.newBuilder().setMetadata(TarStreamMetadata.newBuilder()
                .setTaskId("task-1").setJobId("job-1").setBaseTargetPath(targetDir.toString())
                .setTotalFiles(1).setTotalBytes(tar.length)).build());
        request.onNext(TarStreamRequest.newBuilder().setChunk(TarStreamChunk.newBuilder()
                .setTaskId("task-1").setData(ByteString.copyFrom(tar)).setUncompressedSize(tar.length)).build());
        request.onNext(TarStreamRequest.newBuilder().setChunk(trailer).build());
        request.onCompleted();
        return response.get(15, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("Сумма потока не совпала с суммой отправителя — ни один файл бандла не закоммичен")
    void mismatchedStreamChecksumCommitsNothing() throws Exception {
        Path targetDir = tmp.resolve("target");
        byte[] tar = tarOf("part-0.json", "{\"id\":1}");

        TarStreamResponse response = sendBundle(targetDir, tar, TarStreamChunk.newBuilder()
                .setTaskId("task-1").setIsLastChunk(true).setFilesSent(1)
                .setStreamChecksum(sha256("другой поток".getBytes(StandardCharsets.UTF_8))).build());

        assertFalse(response.getSuccess());
        assertFalse(Files.exists(targetDir.resolve("part-0.json")));
    }

    @Test
    @DisplayName("Файлов распаковано меньше, чем отправлено, — бандл не закоммичен")
    void missingEntriesCommitNothing() throws Exception {
        Path targetDir = tmp.resolve("target");
        byte[] tar = tarOf("part-0.json", "{\"id\":1}");

        TarStreamResponse response = sendBundle(targetDir, tar, TarStreamChunk.newBuilder()
                .setTaskId("task-1").setIsLastChunk(true).setFilesSent(2).setStreamChecksum(sha256(tar)).build());

        assertFalse(response.getSuccess());
        assertFalse(Files.exists(targetDir.resolve("part-0.json")));
    }

    @Test
    @DisplayName("Суммы и число файлов совпали — бандл закоммичен")
    void matchingTrailerCommitsBundle() throws Exception {
        Path targetDir = tmp.resolve("target");
        byte[] tar = tarOf("part-0.json", "{\"id\":1}");

        TarStreamResponse response = sendBundle(targetDir, tar, TarStreamChunk.newBuilder()
                .setTaskId("task-1").setIsLastChunk(true).setFilesSent(1).setStreamChecksum(sha256(tar)).build());

        assertTrue(response.getSuccess(), response.getMessage());
        assertEquals("{\"id\":1}", Files.readString(targetDir.resolve("part-0.json")));
    }

    private TaskItemDto bundleTask(Path source, String targetDir, String targetAddress) throws Exception {
        TaskItemDto task = new TaskItemDto();
        task.setId("task-1");
        task.setJobId("job-1");
        task.setTaskType("BUNDLE_TAR");
        task.setSourcePath(source.getParent().toString());
        task.setTargetPath(targetDir);
        task.setFileSize(Files.size(source));
        task.setFileCount(1);
        task.setBundleManifest(new ObjectMapper().writeValueAsString(List.of(
                new ReplicationSender.BundleFileEntry(source.toString(), source.getFileName().toString(), Files.size(source)))));
        task.setTargetAddress(targetAddress);
        return task;
    }

    private static OrchestratorGrpcClient orchestrator() {
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        when(orchestrator.requestTokenGrant(any())).thenReturn(OrchestratorGrpcClient.TokenGrant.immediate());
        return orchestrator;
    }

    private Path sourceFile() throws Exception {
        Path source = tmp.resolve("source/part-0.json");
        Files.createDirectories(source.getParent());
        Files.writeString(source, "{\"id\":1}");
        Files.setPosixFilePermissions(source, PosixFilePermissions.fromString("rw-r-----"));
        Files.setLastModifiedTime(source, FileTime.fromMillis(SOURCE_MTIME));
        return source;
    }

    @Test
    @DisplayName("Отправитель завершает бандл чанком с SHA-256 потока и числом отправленных файлов")
    void senderSendsStreamTrailer() throws Exception {
        List<TarStreamRequest> received = new CopyOnWriteArrayList<>();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new DataTransferServiceGrpc.DataTransferServiceImplBase() {
                    @Override
                    public StreamObserver<TarStreamRequest> transferTarStream(StreamObserver<TarStreamResponse> out) {
                        return new StreamObserver<>() {
                            @Override public void onNext(TarStreamRequest value) { received.add(value); }
                            @Override public void onError(Throwable t) { }
                            @Override public void onCompleted() {
                                out.onNext(TarStreamResponse.newBuilder().setSuccess(true).setFilesCommitted(1).build());
                                out.onCompleted();
                            }
                        };
                    }
                })
                .build().start();
        Path source = sourceFile();

        try (ReplicationSender sender = new ReplicationSender("worker-1", orchestrator(),
                new HadoopFsManager(null, null, null), null, 16 * 1024)) {
            assertTrue(sender.transferTask(bundleTask(source, "/backup/dir", "inprocess:" + name)));
        }

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        for (TarStreamRequest request : received) {
            if (request.hasChunk()) {
                TarStreamChunk chunk = request.getChunk();
                if (!chunk.getData().isEmpty()) {
                    // Сумма считается по несжатому потоку: чанки в канале сжаты
                    stream.write(io.github.balookrd.pristan.parom.compression.WireCompressor.decompress(
                            chunk.getData().toByteArray(), chunk.getCompressionCodec(), chunk.getUncompressedSize()));
                }
            }
        }
        TarStreamChunk last = received.get(received.size() - 1).getChunk();
        assertEquals(1, last.getFilesSent());
        assertEquals(sha256(stream.toByteArray()), last.getStreamChecksum());
    }

    @Test
    @DisplayName("Файл из бандла получает права и mtime источника с точностью до миллисекунды")
    void bundledFileKeepsModeAndMtime() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
        Path source = sourceFile();
        Path targetDir = tmp.resolve("target");

        try (ReplicationSender sender = new ReplicationSender("worker-1", orchestrator(),
                new HadoopFsManager(null, null, null), null, 16 * 1024, false, null, null, null, false,
                1024 * 1024L, 16 * 1024 * 1024L, 500)) {
            assertTrue(sender.transferTask(bundleTask(source, targetDir.toString(), "inprocess:" + name)));
        }

        Path target = targetDir.resolve("part-0.json");
        assertEquals("rw-r-----", PosixFilePermissions.toString(Files.getPosixFilePermissions(target)));
        assertEquals(SOURCE_MTIME, Files.getLastModifiedTime(target).toMillis());
    }
}
