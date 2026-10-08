package org.apache.hadoop.explorer.replicator.receiver;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.sender.ReplicationSender;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Интеграционный тест защищенной передачи данных gRPC через TLS/SSL.
 */
public class GrpcTlsDataTransferTest {

    private Server tlsServer;
    private ManagedChannel tlsChannel;
    private File tempStagingDir;
    private File tempTargetDir;
    private File tempSourceDir;
    private HadoopFsManager fsManager;
    private SelfSignedCertificate ssc;
    private int serverPort;

    @BeforeEach
    public void setUp() throws Exception {
        ssc = new SelfSignedCertificate("localhost");
        tempStagingDir = Files.createTempDirectory("repl-staging-tls-").toFile();
        tempTargetDir = Files.createTempDirectory("repl-target-tls-").toFile();
        tempSourceDir = Files.createTempDirectory("repl-source-tls-").toFile();

        fsManager = new HadoopFsManager(null, null, null);
        LocalBandwidthLimiter limiter = new LocalBandwidthLimiter(0.0);

        DataTransferServiceImpl service = new DataTransferServiceImpl(
                tempStagingDir.getAbsolutePath(),
                fsManager,
                limiter
        );

        SslContext serverSslContext = GrpcSslContexts.configure(
                SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
        ).build();

        tlsServer = NettyServerBuilder.forPort(0)
                .sslContext(serverSslContext)
                .addService(service)
                .build()
                .start();

        serverPort = tlsServer.getPort();

        SslContext clientSslContext = GrpcSslContexts.forClient()
                .trustManager(ssc.certificate())
                .build();

        tlsChannel = NettyChannelBuilder.forAddress("localhost", serverPort)
                .sslContext(clientSslContext)
                .build();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (tlsChannel != null) {
            tlsChannel.shutdownNow();
            tlsChannel.awaitTermination(3, TimeUnit.SECONDS);
        }
        if (tlsServer != null) {
            tlsServer.shutdownNow();
            tlsServer.awaitTermination(3, TimeUnit.SECONDS);
        }
        if (ssc != null) {
            ssc.delete();
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
    public void testDirectGrpcTlsStreamTransfer() throws Exception {
        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(tlsChannel);

        String jobId = "tls-job-1";
        File targetFile = new File(tempTargetDir, "tls-output.dat");
        byte[] testData = "Confidential data over gRPC TLS channel!".getBytes();

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

        // Отправка метаданных
        FileMetadata metadata = FileMetadata.newBuilder()
                .setJobId(jobId)
                .setSourcePath("/some/source/path.dat")
                .setTargetPath(targetFile.getAbsolutePath())
                .setTotalBytes(testData.length)
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

        // Отправка чанка
        FileChunk chunk = FileChunk.newBuilder()
                .setJobId(jobId)
                .setOffset(0)
                .setData(ByteString.copyFrom(testData))
                .setIsLastChunk(true)
                .build();
        requestObserver.onNext(TransferFileRequest.newBuilder().setChunk(chunk).build());
        requestObserver.onCompleted();

        TransferFileResponse response = responseFuture.get(10, TimeUnit.SECONDS);

        assertTrue(response.getSuccess(), "Передача должна быть успешной: " + response.getMessage());
        assertEquals(testData.length, response.getBytesWritten());
        assertEquals(expectedChecksum, response.getChecksum());
        assertTrue(targetFile.exists());
        assertArrayEquals(testData, Files.readAllBytes(targetFile.toPath()));
    }

    @Test
    public void testReplicationSenderWithTlsAndInsecureSkipVerify() throws Exception {
        File sourceFile = new File(tempSourceDir, "sender-source.dat");
        File targetFile = new File(tempTargetDir, "sender-target.dat");
        byte[] payload = "Payload transferred via ReplicationSender with TLS".getBytes();
        Files.write(sourceFile.toPath(), payload);

        OrchestratorClient dummyOrchestrator = new OrchestratorClient("http://127.0.0.1:65535", "secret", true) {
            @Override
            public void updateJobProgress(String jobId, org.apache.hadoop.explorer.replicator.model.UpdateJobRequest req) {
                // no-op
            }
            @Override
            public double requestNetworkTokens(org.apache.hadoop.explorer.replicator.model.TokenRequest req) {
                return 0.0;
            }
        };
        LocalBandwidthLimiter limiter = new LocalBandwidthLimiter(0.0);

        ReplicationSender sender = new ReplicationSender(
                "test-tls-worker",
                dummyOrchestrator,
                fsManager,
                limiter,
                1024,
                true, // tlsEnabled
                null, // trustCertCollectionPath
                null, // clientCertChainPath
                null, // clientPrivateKeyPath
                true  // insecureSkipVerify
        );

        JobDto job = new JobDto();
        job.setId("tls-sender-job-99");
        job.setSourcePath(sourceFile.getAbsolutePath());
        job.setTargetPath(targetFile.getAbsolutePath());
        job.setTotalBytes((long) payload.length);
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");

        boolean success = sender.transferFile(job, "localhost:" + serverPort);
        assertTrue(success, "ReplicationSender должен успешно передать файл по TLS");
        assertTrue(targetFile.exists());
        assertArrayEquals(payload, Files.readAllBytes(targetFile.toPath()));
    }
}
