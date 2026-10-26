package org.apache.hadoop.explorer.replicator.receiver;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.DataTransferServiceGrpc;
import org.apache.hadoop.explorer.replicator.generated.FileChunk;
import org.apache.hadoop.explorer.replicator.generated.FileMetadata;
import org.apache.hadoop.explorer.replicator.generated.TransferFileRequest;
import org.apache.hadoop.explorer.replicator.generated.TransferFileResponse;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Коммит принятого файла при расхождении с заявленным размером.
 *
 * <p>Приемник считал SHA-256 и количество принятых байт, но не сверял их ни с чем: файл
 * переименовывался в целевой путь, даже если пришла половина. Усеченный файл вставал поверх
 * валидного, а отправитель получал успех.
 */
@DisplayName("Целостность: усеченный файл не коммитится")
class TruncatedTransferTest {

    private Path targetDir;
    private Server server;
    private ManagedChannel channel;
    private DataTransferServiceGrpc.DataTransferServiceStub stub;

    @BeforeEach
    void setUp() throws Exception {
        targetDir = Files.createTempDirectory("truncated-transfer");
        Path staging = Files.createTempDirectory("truncated-staging");

        DataTransferServiceImpl service = new DataTransferServiceImpl(
                staging.toFile().getCanonicalPath(),
                new HadoopFsManager("file:///", null, null, null),
                new LocalBandwidthLimiter(0.0));

        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = DataTransferServiceGrpc.newStub(channel);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
    }

    private TransferFileResponse transfer(File target, byte[] data, long declaredBytes) throws Exception {
        CompletableFuture<TransferFileResponse> result = new CompletableFuture<>();
        StreamObserver<TransferFileRequest> request = stub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {
                result.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                result.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
        });

        request.onNext(TransferFileRequest.newBuilder().setMetadata(FileMetadata.newBuilder()
                .setJobId("job-truncated")
                .setSourcePath("/warehouse/sales/part-0.parquet")
                .setTargetPath(target.getAbsolutePath())
                .setTotalBytes(declaredBytes)
                .setRunAsServiceAccount(true)
                .build()).build());
        request.onNext(TransferFileRequest.newBuilder().setChunk(FileChunk.newBuilder()
                .setJobId("job-truncated")
                .setOffset(0)
                .setData(ByteString.copyFrom(data))
                .setIsLastChunk(true)
                .build()).build());
        request.onCompleted();

        return result.get(5, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("Принято меньше заявленного — коммита нет")
    void shortTransferShouldNotCommit() throws Exception {
        File target = targetDir.resolve("part-0.parquet").toFile();
        byte[] half = new byte[512];

        TransferFileResponse response = transfer(target, half, 1024);

        assertFalse(response.getSuccess(), "Усеченная передача не может считаться успешной");
        assertFalse(target.exists(), "Усеченный файл не должен вставать на целевой путь");
    }

    @Test
    @DisplayName("Валидный файл на целевом пути не затирается усеченной передачей")
    void existingFileShouldSurviveTruncatedTransfer() throws Exception {
        File target = targetDir.resolve("existing.parquet").toFile();
        Files.writeString(target.toPath(), "валидные данные предыдущей передачи");
        long sizeBefore = target.length();

        transfer(target, new byte[100], 4096);

        assertEquals(sizeBefore, target.length(), "Прежний файл должен остаться нетронутым");
    }

    @Test
    @DisplayName("Полная передача коммитится как прежде")
    void completeTransferShouldStillCommit() throws Exception {
        File target = targetDir.resolve("complete.parquet").toFile();
        byte[] data = new byte[2048];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 251);
        }

        TransferFileResponse response = transfer(target, data, data.length);

        assertTrue(response.getSuccess(), "Полная передача должна коммититься: " + response.getMessage());
        assertTrue(target.exists());
        assertEquals(data.length, target.length());
    }
}
