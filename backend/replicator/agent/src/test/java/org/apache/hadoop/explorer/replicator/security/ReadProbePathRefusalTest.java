package org.apache.hadoop.explorer.replicator.security;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.CheckFileRequest;
import org.apache.hadoop.explorer.replicator.generated.CheckFileResponse;
import org.apache.hadoop.explorer.replicator.generated.DataTransferServiceGrpc;
import org.apache.hadoop.explorer.replicator.generated.DirectoryManifestRequest;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Отказ read-проб приемника на запрещенных путях.
 *
 * <p>{@code checkFile} и {@code getDirectoryManifest} отвечали на отвергнутый путь
 * «файла нет»: доступ к данным не открывался, но отказ политики безопасности выглядел как
 * обычное отсутствие файла — ни в логе приемника, ни на стороне отправителя его было не
 * отличить, а отправитель трактовал ответ как «цель пуста» и планировал полное копирование.
 * Отказ должен быть явным статусом.
 */
@DisplayName("SEC: read-пробы приемника явно отказывают на запрещенных путях")
class ReadProbePathRefusalTest {

    private String sandboxRoot;
    private Server inProcessServer;
    private ManagedChannel inProcessChannel;
    private DataTransferServiceGrpc.DataTransferServiceBlockingStub stub;

    @BeforeEach
    void setUp() throws IOException {
        Path sandbox = Files.createTempDirectory("replicator-read-probe");
        sandboxRoot = sandbox.toFile().getCanonicalPath();

        HadoopFsManager fsManager = new HadoopFsManager("file:///", null, null, sandboxRoot);
        DataTransferServiceImpl service = new DataTransferServiceImpl(
                sandboxRoot + "/staging", fsManager, new LocalBandwidthLimiter(0.0));

        String serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor().addService(service).build().start();
        inProcessChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        stub = DataTransferServiceGrpc.newBlockingStub(inProcessChannel);
    }

    @AfterEach
    void tearDown() {
        if (inProcessChannel != null) inProcessChannel.shutdownNow();
        if (inProcessServer != null) inProcessServer.shutdownNow();
    }

    @Test
    @DisplayName("checkFile на системном пути ОС отвечает PERMISSION_DENIED, а не 'файла нет'")
    void checkFileShouldRefuseForbiddenPath() {
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () ->
                stub.checkFile(CheckFileRequest.newBuilder()
                        .setPath("/etc/shadow")
                        .setRunAsServiceAccount(true)
                        .build()));

        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode(),
                "Отказ политики безопасности должен быть виден отправителю отдельным статусом");
    }

    @Test
    @DisplayName("getDirectoryManifest за пределами песочницы отвечает PERMISSION_DENIED")
    void manifestShouldRefusePathOutsideSandbox() {
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () ->
                stub.getDirectoryManifest(DirectoryManifestRequest.newBuilder()
                        .setPath("/var/tmp")
                        .setRunAsServiceAccount(true)
                        .build()));

        assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode(),
                "Выход за границы Chroot-песочницы должен отвергаться явно");
    }

    @Test
    @DisplayName("Разрешенный путь внутри песочницы по-прежнему отвечает обычным ответом")
    void allowedPathShouldStillBeAnswered() {
        CheckFileResponse response = stub.checkFile(CheckFileRequest.newBuilder()
                .setPath(sandboxRoot + "/staging/missing.dat")
                .setRunAsServiceAccount(true)
                .build());

        assertFalse(response.getExists(), "Отсутствующий файл внутри песочницы — это обычный ответ, не отказ");
        assertEquals(0L, response.getSize());
    }
}
