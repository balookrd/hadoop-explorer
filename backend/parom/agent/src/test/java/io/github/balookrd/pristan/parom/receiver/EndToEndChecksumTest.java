package io.github.balookrd.pristan.parom.receiver;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.github.balookrd.pristan.parom.client.OrchestratorGrpcClient;
import io.github.balookrd.pristan.parom.fs.HadoopFsManager;
import io.github.balookrd.pristan.parom.generated.DataTransferServiceGrpc;
import io.github.balookrd.pristan.parom.generated.FileChunk;
import io.github.balookrd.pristan.parom.generated.FileMetadata;
import io.github.balookrd.pristan.parom.generated.TransferFileRequest;
import io.github.balookrd.pristan.parom.generated.TransferFileResponse;
import io.github.balookrd.pristan.parom.model.TaskItemDto;
import io.github.balookrd.pristan.parom.sender.ReplicationSender;
import io.github.balookrd.pristan.parom.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * The file is committed on the target only when the receiver's digest equals the sender's.
 *
 * <p>The receiver compared its SHA-256 with {@code FileMetadata.checksum}, which the sender never
 * filled (the metadata goes first, before the file is read), so nothing was compared and a
 * corrupted transfer of the right length was committed.
 */
@DisplayName("Сквозная контрольная сумма передачи файла")
class EndToEndChecksumTest {

    @TempDir
    Path tmp;

    private Server server;
    private ManagedChannel channel;
    private final byte[] content = "partition data\n".repeat(5000).getBytes(StandardCharsets.UTF_8);

    @AfterEach
    void tearDown() {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private TransferFileResponse sendToRealReceiver(Path target, String trailerChecksum) throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new DataTransferServiceImpl(tmp.resolve("staging").toString(),
                        new HadoopFsManager(null, null, null), new LocalBandwidthLimiter(0.0)))
                .build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();

        CompletableFuture<TransferFileResponse> response = new CompletableFuture<>();
        StreamObserver<TransferFileRequest> request = DataTransferServiceGrpc.newStub(channel)
                .transferFile(new StreamObserver<>() {
                    @Override public void onNext(TransferFileResponse value) { response.complete(value); }
                    @Override public void onError(Throwable t) { response.completeExceptionally(t); }
                    @Override public void onCompleted() { }
                });
        request.onNext(TransferFileRequest.newBuilder().setMetadata(FileMetadata.newBuilder()
                .setJobId("job-1").setSourcePath("/src/part-0").setTargetPath(target.toString())
                .setTotalBytes(content.length)).build());
        request.onNext(TransferFileRequest.newBuilder().setChunk(FileChunk.newBuilder()
                .setJobId("job-1").setData(ByteString.copyFrom(content)).setUncompressedSize(content.length)).build());
        request.onNext(TransferFileRequest.newBuilder().setChunk(FileChunk.newBuilder()
                .setJobId("job-1").setOffset(content.length).setIsLastChunk(true)
                .setFileChecksum(trailerChecksum)).build());
        request.onCompleted();
        return response.get(10, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("Приемник не коммитит файл, если его сумма не совпала с суммой отправителя")
    void receiverRejectsMismatchedChecksum() throws Exception {
        Path target = tmp.resolve("target/part-0");

        TransferFileResponse response = sendToRealReceiver(target, sha256("другие байты".getBytes(StandardCharsets.UTF_8)));

        assertFalse(response.getSuccess(), "Файл той же длины с другим содержимым не должен быть закоммичен");
        assertFalse(Files.exists(target));
    }

    @Test
    @DisplayName("Приемник коммитит файл при совпадении сумм")
    void receiverCommitsOnMatchingChecksum() throws Exception {
        Path target = tmp.resolve("target/part-0");

        TransferFileResponse response = sendToRealReceiver(target, sha256(content));

        assertTrue(response.getSuccess(), response.getMessage());
        assertTrue(Files.exists(target));
    }

    @Test
    @DisplayName("Отправитель завершает поток чанком с SHA-256 прочитанных байт")
    void senderSendsChecksumTrailer() throws Exception {
        Path source = tmp.resolve("source-part-0");
        Files.write(source, content);
        List<TransferFileRequest> received = new CopyOnWriteArrayList<>();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new DataTransferServiceGrpc.DataTransferServiceImplBase() {
                    @Override
                    public StreamObserver<TransferFileRequest> transferFile(StreamObserver<TransferFileResponse> out) {
                        return new StreamObserver<>() {
                            @Override public void onNext(TransferFileRequest value) { received.add(value); }
                            @Override public void onError(Throwable t) { }
                            @Override public void onCompleted() {
                                out.onNext(TransferFileResponse.newBuilder().setSuccess(true).build());
                                out.onCompleted();
                            }
                        };
                    }
                })
                .build().start();

        TaskItemDto task = new TaskItemDto();
        task.setId("task-1");
        task.setJobId("job-1");
        task.setSourcePath(source.toString());
        task.setTargetPath("/backup/part-0");
        task.setFileSize(content.length);
        task.setTargetAddress("inprocess:" + name);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        org.mockito.Mockito.when(orchestrator.requestTokenGrant(org.mockito.ArgumentMatchers.any()))
                .thenReturn(OrchestratorGrpcClient.TokenGrant.immediate());
        try (ReplicationSender sender = new ReplicationSender("worker-1", orchestrator,
                new HadoopFsManager(null, null, null), null, 16 * 1024)) {
            assertTrue(sender.transferTask(task));
        }

        TransferFileRequest last = received.get(received.size() - 1);
        assertTrue(last.hasChunk());
        assertEquals(sha256(content), last.getChunk().getFileChecksum());
    }
}
