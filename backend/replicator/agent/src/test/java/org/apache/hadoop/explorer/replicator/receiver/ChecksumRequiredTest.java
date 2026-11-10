package org.apache.hadoop.explorer.replicator.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.DataTransferServiceGrpc;
import org.apache.hadoop.explorer.replicator.generated.FileChunk;
import org.apache.hadoop.explorer.replicator.generated.FileMetadata;
import org.apache.hadoop.explorer.replicator.generated.TarStreamChunk;
import org.apache.hadoop.explorer.replicator.generated.TarStreamMetadata;
import org.apache.hadoop.explorer.replicator.generated.TarStreamRequest;
import org.apache.hadoop.explorer.replicator.generated.TarStreamResponse;
import org.apache.hadoop.explorer.replicator.generated.TransferFileRequest;
import org.apache.hadoop.explorer.replicator.generated.TransferFileResponse;
import org.apache.hadoop.explorer.replicator.model.TaskItemDto;
import org.apache.hadoop.explorer.replicator.sender.ReplicationSender;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * With agents of different versions the checksum used to be skipped silently: an old sender sends
 * none, an old receiver ignores the trailer, and only the length was compared. With the check
 * required both sides refuse instead: the receiver does not commit without the sender checksum,
 * the sender fails the task when the receiver did not confirm its checksum.
 */
@DisplayName("Обязательная контрольная сумма при агентах разных версий")
class ChecksumRequiredTest {

    @TempDir
    Path tmp;

    private DataTransferServiceImpl service;
    private Server server;
    private final byte[] content = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);

    @BeforeEach
    void setUp() {
        service = new DataTransferServiceImpl(tmp.resolve("staging").toString(),
                new HadoopFsManager(null, null, null), new LocalBandwidthLimiter(0.0));
        service.setRequireSenderChecksum(true);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.shutdownNow();
    }

    @Test
    @DisplayName("Проверка включена по умолчанию в конфигурации агента")
    void requiredByDefault() {
        assertTrue(new ReplicatorAgentConfig().isRequireChecksum());
    }

    @Test
    @DisplayName("Приемник не коммитит файл от отправителя без контрольной суммы")
    void receiverRefusesFileWithoutSenderChecksum() throws Exception {
        Path target = tmp.resolve("target/part-0.json");
        CompletableFuture<TransferFileResponse> response = new CompletableFuture<>();
        StreamObserver<TransferFileRequest> request = service.transferFile(new StreamObserver<>() {
            @Override public void onNext(TransferFileResponse value) { response.complete(value); }
            @Override public void onError(Throwable t) { response.completeExceptionally(t); }
            @Override public void onCompleted() { }
        });
        request.onNext(TransferFileRequest.newBuilder().setMetadata(FileMetadata.newBuilder()
                .setJobId("job-1").setTargetPath(target.toString()).setTotalBytes(content.length)).build());
        request.onNext(TransferFileRequest.newBuilder().setChunk(FileChunk.newBuilder()
                .setJobId("job-1").setData(ByteString.copyFrom(content)).setUncompressedSize(content.length)).build());
        request.onCompleted();

        assertFalse(response.get(10, TimeUnit.SECONDS).getSuccess());
        assertFalse(Files.exists(target));
    }

    @Test
    @DisplayName("Приемник не коммитит бандл от отправителя без контрольной суммы")
    void receiverRefusesBundleWithoutSenderChecksum() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(out)) {
            TarArchiveEntry entry = new TarArchiveEntry("part-0.json");
            entry.setSize(content.length);
            tar.putArchiveEntry(entry);
            tar.write(content);
            tar.closeArchiveEntry();
            tar.finish();
        }
        Path targetDir = tmp.resolve("target");
        CompletableFuture<TarStreamResponse> response = new CompletableFuture<>();
        StreamObserver<TarStreamRequest> request = service.transferTarStream(new StreamObserver<>() {
            @Override public void onNext(TarStreamResponse value) { response.complete(value); }
            @Override public void onError(Throwable t) { response.completeExceptionally(t); }
            @Override public void onCompleted() { }
        });
        request.onNext(TarStreamRequest.newBuilder().setMetadata(TarStreamMetadata.newBuilder()
                .setTaskId("task-1").setJobId("job-1").setBaseTargetPath(targetDir.toString()).setTotalFiles(1)).build());
        request.onNext(TarStreamRequest.newBuilder().setChunk(TarStreamChunk.newBuilder()
                .setTaskId("task-1").setData(ByteString.copyFrom(out.toByteArray()))
                .setUncompressedSize(out.size())).build());
        request.onCompleted();

        assertFalse(response.get(15, TimeUnit.SECONDS).getSuccess());
        assertFalse(Files.exists(targetDir.resolve("part-0.json")));
    }

    /** Приемник, который отвечает успехом и возвращает заданные суммы, как это делал бы другой выпуск агента. */
    private String startReceiverAnswering(String fileChecksum, String streamChecksum) throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new DataTransferServiceGrpc.DataTransferServiceImplBase() {
                    @Override
                    public StreamObserver<TransferFileRequest> transferFile(StreamObserver<TransferFileResponse> out) {
                        return new StreamObserver<>() {
                            @Override public void onNext(TransferFileRequest value) { }
                            @Override public void onError(Throwable t) { }
                            @Override public void onCompleted() {
                                out.onNext(TransferFileResponse.newBuilder().setSuccess(true).setChecksum(fileChecksum).build());
                                out.onCompleted();
                            }
                        };
                    }

                    @Override
                    public StreamObserver<TarStreamRequest> transferTarStream(StreamObserver<TarStreamResponse> out) {
                        return new StreamObserver<>() {
                            @Override public void onNext(TarStreamRequest value) { }
                            @Override public void onError(Throwable t) { }
                            @Override public void onCompleted() {
                                out.onNext(TarStreamResponse.newBuilder().setSuccess(true).setFilesCommitted(1)
                                        .setStreamChecksum(streamChecksum).build());
                                out.onCompleted();
                            }
                        };
                    }
                })
                .build().start();
        return "inprocess:" + name;
    }

    private boolean send(String taskType, String address, boolean requireReceiverChecksum) throws Exception {
        Path source = tmp.resolve("source/part-0.json");
        Files.createDirectories(source.getParent());
        Files.write(source, content);
        TaskItemDto task = new TaskItemDto();
        task.setId("task-1");
        task.setJobId("job-1");
        task.setTaskType(taskType);
        task.setFileSize(content.length);
        task.setTargetAddress(address);
        if ("BUNDLE_TAR".equals(taskType)) {
            task.setSourcePath(source.getParent().toString());
            task.setTargetPath("/backup/dir");
            task.setFileCount(1);
            task.setBundleManifest(new ObjectMapper().writeValueAsString(List.of(new ReplicationSender.BundleFileEntry(
                    source.toString(), "part-0.json", content.length))));
        } else {
            task.setSourcePath(source.toString());
            task.setTargetPath("/backup/dir/part-0.json");
        }
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        when(orchestrator.requestTokenGrant(any())).thenReturn(OrchestratorGrpcClient.TokenGrant.immediate());
        try (ReplicationSender sender = new ReplicationSender("worker-1", orchestrator,
                new HadoopFsManager(null, null, null), null, 16 * 1024, false, null, null, null, false,
                1024 * 1024L, 16 * 1024 * 1024L, 500)) {
            sender.setRequireReceiverChecksum(requireReceiverChecksum);
            return sender.transferTask(task);
        }
    }

    @Test
    @DisplayName("Отправитель считает передачу неуспешной, если приемник записал другие байты")
    void senderFailsOnDifferentReceiverChecksum() throws Exception {
        assertFalse(send("FILE", startReceiverAnswering("00".repeat(32), ""), false));
    }

    @Test
    @DisplayName("Отправитель считает передачу неуспешной, если приемник не подтвердил сумму файла")
    void senderFailsWhenReceiverConfirmsNoFileChecksum() throws Exception {
        assertFalse(send("FILE", startReceiverAnswering("", ""), true));
    }

    @Test
    @DisplayName("Отправитель считает бандл неуспешным, если приемник не подтвердил сумму потока")
    void senderFailsWhenReceiverConfirmsNoStreamChecksum() throws Exception {
        assertFalse(send("BUNDLE_TAR", startReceiverAnswering("", ""), true));
    }

    @Test
    @DisplayName("С выключенной проверкой приемник без подтверждения суммы принимается")
    void senderAcceptsUnconfirmedChecksumWhenNotRequired() throws Exception {
        assertTrue(send("FILE", startReceiverAnswering("", ""), false));
        if (server != null) server.shutdownNow();
        assertTrue(send("BUNDLE_TAR", startReceiverAnswering("", ""), false));
    }
}
