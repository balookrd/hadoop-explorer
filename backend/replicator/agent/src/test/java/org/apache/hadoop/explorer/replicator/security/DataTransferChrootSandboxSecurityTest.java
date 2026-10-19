package org.apache.hadoop.explorer.replicator.security;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SEC-10: Chroot Sandbox Security Tests for DataTransferService")
class DataTransferChrootSandboxSecurityTest {

    private Path tempSandboxDir;
    private String sandboxRoot;
    private HadoopFsManager fsManager;
    private DataTransferServiceImpl dataTransferService;
    private Server inProcessServer;
    private ManagedChannel inProcessChannel;
    private DataTransferServiceGrpc.DataTransferServiceStub asyncStub;
    private DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub;

    @BeforeEach
    void setUp() throws IOException {
        tempSandboxDir = Files.createTempDirectory("replicator-chroot-test");
        sandboxRoot = tempSandboxDir.toFile().getCanonicalPath();

        fsManager = new HadoopFsManager("file:///", null, null, sandboxRoot);
        dataTransferService = new DataTransferServiceImpl(
                sandboxRoot + "/staging",
                fsManager,
                new LocalBandwidthLimiter(0.0),
                null,
                4,
                4,
                1024 * 1024,
                5000L,
                null,
                false
        );

        String serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(dataTransferService)
                .build()
                .start();

        inProcessChannel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        asyncStub = DataTransferServiceGrpc.newStub(inProcessChannel);
        blockingStub = DataTransferServiceGrpc.newBlockingStub(inProcessChannel);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (inProcessChannel != null) {
            inProcessChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
        if (inProcessServer != null) {
            inProcessServer.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
        if (dataTransferService != null) {
            dataTransferService.close();
        }
        if (tempSandboxDir != null) {
            FileUtils.deleteDirectory(tempSandboxDir.toFile());
        }
    }

    @Test
    @DisplayName("Chroot Sandbox: Запись файла внутри песочницы завершается успешно")
    void testTransferFileInsideSandboxAllowed() throws Exception {
        String targetPath = sandboxRoot + "/warehouse/data/table1.parquet";
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TransferFileResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<TransferFileRequest> requestObserver = asyncStub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {
                responseRef.set(value);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        // 1. Метаданные файла
        requestObserver.onNext(TransferFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder()
                        .setJobId("job-chroot-1")
                        .setSourcePath("/src/table1.parquet")
                        .setTargetPath(targetPath)
                        .setTotalBytes(13)
                        .setExecutionPrincipal("hdfs@CORP.LOCAL")
                        .setRunAsServiceAccount(true)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());

        // 2. Чанк
        byte[] payload = "Hello Sandbox".getBytes(StandardCharsets.UTF_8);
        requestObserver.onNext(TransferFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder()
                        .setJobId("job-chroot-1")
                        .setOffset(0)
                        .setData(ByteString.copyFrom(payload))
                        .setIsLastChunk(true)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());

        requestObserver.onCompleted();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(errorRef.get(), "Не должно быть ошибок при записи внутри песочницы");
        assertNotNull(responseRef.get());
        assertTrue(responseRef.get().getSuccess());
        assertTrue(new File(targetPath).exists(), "Файл должен быть успешно записан внутри песочницы");
    }

    @Test
    @DisplayName("Chroot Sandbox: Запись файла за пределами песочницы блокируется со статусом PERMISSION_DENIED")
    void testTransferFileOutsideSandboxBlocked() throws Exception {
        String outsideTarget = "/tmp/escaped_pwned_" + System.currentTimeMillis() + ".txt";
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<TransferFileRequest> requestObserver = asyncStub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {}

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        // Посылаем метаданные с путем вне песочницы
        requestObserver.onNext(TransferFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder()
                        .setJobId("job-chroot-escape")
                        .setSourcePath("/src/exploit.txt")
                        .setTargetPath(outsideTarget)
                        .setTotalBytes(100)
                        .setExecutionPrincipal("hdfs@CORP.LOCAL")
                        .setRunAsServiceAccount(true)
                        .setCompressionCodec(CompressionCodec.COMPRESSION_NONE)
                        .build())
                .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(errorRef.get(), "Должна быть получена ошибка безопасности");
        assertTrue(errorRef.get() instanceof StatusRuntimeException);
        StatusRuntimeException sre = (StatusRuntimeException) errorRef.get();
        assertEquals(Status.Code.PERMISSION_DENIED, sre.getStatus().getCode());
        assertTrue(sre.getStatus().getDescription().contains("Нарушение Chroot-песочницы")
                || sre.getStatus().getDescription().contains("запрещен"));

        assertFalse(new File(outsideTarget).exists(), "Вредоносный файл вне песочницы не должен быть создан");
    }

    @Test
    @DisplayName("Chroot Sandbox: Удаление путей за пределами песочницы отклоняется")
    void testDeletePathOutsideSandboxBlocked() {
        DeletePathResponse response = blockingStub.deletePath(DeletePathRequest.newBuilder()
                .setJobId("job-del-escape")
                .setPath("/tmp/outside_file.txt")
                .setRecursive(false)
                .setExecutionPrincipal("hdfs@CORP.LOCAL")
                .setRunAsServiceAccount(true)
                .build());

        assertFalse(response.getSuccess(), "Удаление пути вне песочницы должно завершаться ошибкой");
        assertTrue(response.getMessage().contains("Нарушение Chroot-песочницы")
                || response.getMessage().contains("Ошибка удаления"));
    }

    @Test
    @DisplayName("Chroot Sandbox: Переименование путей за пределами песочницы отклоняется")
    void testRenamePathOutsideSandboxBlocked() {
        RenamePathResponse response = blockingStub.renamePath(RenamePathRequest.newBuilder()
                .setJobId("job-mv-escape")
                .setSourcePath(sandboxRoot + "/data/valid.txt")
                .setTargetPath("/tmp/stolen_file.txt")
                .setExecutionPrincipal("hdfs@CORP.LOCAL")
                .setRunAsServiceAccount(true)
                .build());

        assertFalse(response.getSuccess(), "Переименование наружу песочницы должно завершаться ошибкой");
        assertTrue(response.getMessage().contains("Нарушение Chroot-песочницы")
                || response.getMessage().contains("Ошибка переименования"));
    }

    @Test
    @DisplayName("Chroot Sandbox: Доступ к критическим системным путям (/etc, ~/.ssh) блокируется")
    void testSystemAndSshPathsBlocked() {
        assertThrows(SecurityException.class, () -> {
            fsManager.validateSandboxAccess("/etc/shadow");
        });

        assertThrows(SecurityException.class, () -> {
            fsManager.validateSandboxAccess("/home/user/.ssh/authorized_keys");
        });

        assertThrows(SecurityException.class, () -> {
            fsManager.validateSandboxAccess("/bin/sh");
        });
    }
}
