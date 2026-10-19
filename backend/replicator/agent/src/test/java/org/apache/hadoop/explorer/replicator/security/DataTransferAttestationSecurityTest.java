package org.apache.hadoop.explorer.replicator.security;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DataTransferAttestationSecurityTest {

    private static final String SECRET = "secure-agent-attestation-secret-key-999";
    private String serverName;
    private Server inProcessServer;
    private ManagedChannel inProcessChannel;
    private File tempStagingDir;
    private File tempTargetDir;
    private HadoopFsManager fsManager;
    private DataTransferServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        serverName = InProcessServerBuilder.generateName();
        tempStagingDir = Files.createTempDirectory("sec-staging-").toFile();
        tempTargetDir = Files.createTempDirectory("sec-target-").toFile();

        fsManager = new HadoopFsManager(null, null, null);
        LocalBandwidthLimiter limiter = new LocalBandwidthLimiter(0.0);

        // Ресивер с включенной проверкой токенов и секретом
        service = new DataTransferServiceImpl(
                tempStagingDir.getAbsolutePath(),
                fsManager,
                limiter,
                null,
                4,
                8,
                1024 * 1024,
                60_000L,
                SECRET,
                false // enforceTaskAttestation = false для тестирования обоих режимов
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
    void tearDown() throws Exception {
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
                for (File child : files) {
                    deleteRecursively(child);
                }
            }
            f.delete();
        }
    }

    @Test
    @DisplayName("Успешная передача файла с валидным HMAC-токеном аттестации задачи")
    void testTransferFileWithValidAttestationToken() throws Exception {
        String taskId = "task-valid-01";
        String jobId = "job-42";
        String targetFile = new File(tempTargetDir, "valid.txt").getAbsolutePath();
        String principal = "alice@CORP.NET";
        boolean runAsService = false;

        String token = TaskAttestationService.generateToken(
                taskId, jobId, targetFile, principal, runAsService, SECRET
        );

        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(inProcessChannel);
        CompletableFuture<TransferFileResponse> future = new CompletableFuture<>();

        StreamObserver<TransferFileRequest> stream = stub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {}
        });

        FileMetadata metadata = FileMetadata.newBuilder()
                .setJobId(jobId)
                .setTaskId(taskId)
                .setSourcePath("/src/valid.txt")
                .setTargetPath(targetFile)
                .setTotalBytes(12)
                .setExecutionPrincipal(principal)
                .setRunAsServiceAccount(runAsService)
                .setTaskToken(token)
                .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                .build();

        stream.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

        FileChunk chunk = FileChunk.newBuilder()
                .setJobId(jobId)
                .setOffset(0)
                .setData(ByteString.copyFrom("Hello Secure".getBytes(StandardCharsets.UTF_8)))
                .setIsLastChunk(true)
                .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                .build();

        stream.onNext(TransferFileRequest.newBuilder().setChunk(chunk).build());
        stream.onCompleted();

        TransferFileResponse resp = future.get(5, TimeUnit.SECONDS);
        assertNotNull(resp);
        assertTrue(resp.getSuccess(), "Передача с валидным токеном должна быть успешной");
        assertEquals(12, resp.getBytesWritten());
        assertTrue(new File(targetFile).exists(), "Файл должен быть успешно сохранен");
    }

    @Test
    @DisplayName("Блокировка эскалации привилегий: отказ PERMISSION_DENIED при подмене принципала на суперпользователя hdfs")
    void testRejectPrivilegeEscalationWithTamperedPrincipal() {
        String taskId = "task-tamper-02";
        String jobId = "job-42";
        String targetFile = new File(tempTargetDir, "privilege_test.txt").getAbsolutePath();
        String legitPrincipal = "alice@CORP.NET";

        // Оркестратор выдал токен для alice
        String token = TaskAttestationService.generateToken(
                taskId, jobId, targetFile, legitPrincipal, false, SECRET
        );

        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(inProcessChannel);
        CompletableFuture<TransferFileResponse> future = new CompletableFuture<>();

        StreamObserver<TransferFileRequest> stream = stub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {}
        });

        // Воркер-злоумышленник пытается передать executionPrincipal = "hdfs" под чужим токеном
        FileMetadata metadata = FileMetadata.newBuilder()
                .setJobId(jobId)
                .setTaskId(taskId)
                .setSourcePath("/src/privilege_test.txt")
                .setTargetPath(targetFile)
                .setTotalBytes(10)
                .setExecutionPrincipal("hdfs")
                .setRunAsServiceAccount(false)
                .setTaskToken(token)
                .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                .build();

        stream.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof StatusRuntimeException);
        StatusRuntimeException sre = (StatusRuntimeException) ex.getCause();
        assertEquals(Status.Code.PERMISSION_DENIED, sre.getStatus().getCode(),
                "Попытка подмены принципала должна завершаться со статусом PERMISSION_DENIED");
    }

    @Test
    @DisplayName("Блокировка подмены целевого пути (Path Substitution): отказ PERMISSION_DENIED")
    void testRejectTamperedTargetPath() {
        String taskId = "task-tamper-03";
        String jobId = "job-42";
        String legitTarget = new File(tempTargetDir, "legit.txt").getAbsolutePath();
        String tamperedTarget = new File(tempTargetDir, "critical_system_file.conf").getAbsolutePath();

        String token = TaskAttestationService.generateToken(
                taskId, jobId, legitTarget, "alice@CORP.NET", false, SECRET
        );

        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(inProcessChannel);
        CompletableFuture<TransferFileResponse> future = new CompletableFuture<>();

        StreamObserver<TransferFileRequest> stream = stub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) { future.complete(value); }
            @Override
            public void onError(Throwable t) { future.completeExceptionally(t); }
            @Override
            public void onCompleted() {}
        });

        FileMetadata metadata = FileMetadata.newBuilder()
                .setJobId(jobId)
                .setTaskId(taskId)
                .setSourcePath("/src/legit.txt")
                .setTargetPath(tamperedTarget)
                .setTotalBytes(10)
                .setExecutionPrincipal("alice@CORP.NET")
                .setRunAsServiceAccount(false)
                .setTaskToken(token)
                .build();

        stream.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof StatusRuntimeException);
        StatusRuntimeException sre = (StatusRuntimeException) ex.getCause();
        assertEquals(Status.Code.PERMISSION_DENIED, sre.getStatus().getCode(),
                "Подмена целевого пути должна приводить к PERMISSION_DENIED");
    }

    @Test
    @DisplayName("Режим строгого принуждения (enforceTaskAttestation = true) отклоняет запросы без токена")
    void testEnforceTaskAttestationRejectsMissingToken() throws Exception {
        // Создаем сервис со строгим режимом
        String strictServerName = InProcessServerBuilder.generateName();
        DataTransferServiceImpl strictService = new DataTransferServiceImpl(
                tempStagingDir.getAbsolutePath(),
                fsManager,
                new LocalBandwidthLimiter(0.0),
                null,
                4,
                8,
                1024 * 1024,
                60_000L,
                SECRET,
                true // enforceTaskAttestation = true
        );

        Server strictServer = InProcessServerBuilder.forName(strictServerName)
                .directExecutor()
                .addService(strictService)
                .build()
                .start();

        ManagedChannel strictChannel = InProcessChannelBuilder.forName(strictServerName)
                .directExecutor()
                .build();

        try {
            DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(strictChannel);
            CompletableFuture<TransferFileResponse> future = new CompletableFuture<>();

            StreamObserver<TransferFileRequest> stream = stub.transferFile(new StreamObserver<>() {
                @Override
                public void onNext(TransferFileResponse value) { future.complete(value); }
                @Override
                public void onError(Throwable t) { future.completeExceptionally(t); }
                @Override
                public void onCompleted() {}
            });

            // Запрос БЕЗ токена
            FileMetadata metadata = FileMetadata.newBuilder()
                    .setJobId("job-strict-99")
                    .setTaskId("task-strict-99")
                    .setSourcePath("/src/strict.txt")
                    .setTargetPath(new File(tempTargetDir, "strict.txt").getAbsolutePath())
                    .setTotalBytes(5)
                    .setExecutionPrincipal("alice@CORP.NET")
                    .build();

            stream.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

            ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
            assertTrue(ex.getCause() instanceof StatusRuntimeException);
            StatusRuntimeException sre = (StatusRuntimeException) ex.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, sre.getStatus().getCode(),
                    "В строгом режиме запрос без токена должен быть отклонен с PERMISSION_DENIED");
        } finally {
            strictChannel.shutdownNow();
            strictServer.shutdownNow();
        }
    }
}
