package org.apache.hadoop.explorer.replicator.receiver;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class DataTransferServiceGrpcTest {

    private String serverName;
    private Server inProcessServer;
    private ManagedChannel inProcessChannel;
    private File tempStagingDir;
    private File tempTargetDir;
    private HadoopFsManager fsManager;

    @BeforeEach
    public void setUp() throws Exception {
        serverName = InProcessServerBuilder.generateName();
        tempStagingDir = Files.createTempDirectory("repl-staging-").toFile();
        tempTargetDir = Files.createTempDirectory("repl-target-").toFile();

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
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (inProcessChannel != null) {
            inProcessChannel.shutdownNow();
        }
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
        }
        deleteRecursively(tempStagingDir);
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
    public void testSuccessfulFileTransfer() throws Exception {
        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(inProcessChannel);

        String jobId = "test-job-123";
        File targetFile = new File(tempTargetDir, "output.dat");
        byte[] testData = "Hello, Hadoop gRPC Replicator Java Agent!".getBytes();

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] expectedHashBytes = md.digest(testData);
        StringBuilder sb = new StringBuilder();
        for (byte b : expectedHashBytes) sb.append(String.format("%02x", b));
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

        // 1. Отправляем метаданные
        FileMetadata metadata = FileMetadata.newBuilder()
                .setJobId(jobId)
                .setSourcePath("/test/source/file.dat")
                .setTargetPath(targetFile.getAbsolutePath())
                .setTotalBytes(testData.length)
                .setRunAsServiceAccount(true)
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

        // 2. Отправляем чанк
        FileChunk chunk = FileChunk.newBuilder()
                .setJobId(jobId)
                .setOffset(0)
                .setData(ByteString.copyFrom(testData))
                .setIsLastChunk(true)
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setChunk(chunk).build());

        // 3. Завершаем стрим
        requestObserver.onCompleted();

        TransferFileResponse response = responseFuture.get(5, TimeUnit.SECONDS);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals(jobId, response.getJobId());
        assertEquals(testData.length, response.getBytesWritten());
        assertEquals(expectedChecksum, response.getChecksum());
        assertTrue(targetFile.exists(), "Целевой файл должен существовать после коммита");
        assertArrayEquals(testData, Files.readAllBytes(targetFile.toPath()));
    }
}
