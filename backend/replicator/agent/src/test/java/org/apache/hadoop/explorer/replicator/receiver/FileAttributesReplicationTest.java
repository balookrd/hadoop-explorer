package org.apache.hadoop.explorer.replicator.receiver;

import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.TaskItemDto;
import org.apache.hadoop.explorer.replicator.sender.ReplicationSender;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The target file keeps the permissions and modification time of the source file.
 *
 * <p>Nothing was carried over: every replicated file got the default mode of the writing account
 * and the time of the copy, so the target could not be compared with the source by mtime.
 */
@DisplayName("Перенос атрибутов файла на приемник")
class FileAttributesReplicationTest {

    private static final long SOURCE_MTIME = 1_700_000_000_000L;

    @TempDir
    Path tmp;

    private Server server;

    @AfterEach
    void tearDown() {
        if (server != null) server.shutdownNow();
    }

    @Test
    @DisplayName("HadoopFsManager читает и применяет права и mtime")
    void attributesRoundTripOnFileSystem() throws Exception {
        Path file = tmp.resolve("a.parquet");
        Files.writeString(file, "x");
        HadoopFsManager fs = new HadoopFsManager(null, null, null);

        fs.applyAttributes(file.toString(), new HadoopFsManager.FileAttributes(0640, SOURCE_MTIME, null, null), null, true);

        HadoopFsManager.FileAttributes read = fs.getAttributes(file.toString(), null, true);
        assertEquals(0640, read.mode());
        assertEquals(SOURCE_MTIME, read.modificationTime());
        assertTrue(read.owner() != null && !read.owner().isBlank());
    }

    @Test
    @DisplayName("Переданный файл получает права и mtime источника")
    void transferredFileKeepsModeAndMtime() throws Exception {
        Path source = tmp.resolve("source/part-0.parquet");
        Files.createDirectories(source.getParent());
        Files.write(source, "partition data\n".repeat(1000).getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(source, PosixFilePermissions.fromString("rw-r-----"));
        Files.setLastModifiedTime(source, FileTime.fromMillis(SOURCE_MTIME));
        Path target = tmp.resolve("target/part-0.parquet");

        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new DataTransferServiceImpl(tmp.resolve("staging").toString(),
                        new HadoopFsManager(null, null, null), new LocalBandwidthLimiter(0.0)))
                .build().start();

        TaskItemDto task = new TaskItemDto();
        task.setId("task-1");
        task.setJobId("job-1");
        task.setSourcePath(source.toString());
        task.setTargetPath(target.toString());
        task.setFileSize(Files.size(source));
        task.setTargetAddress("inprocess:" + name);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        when(orchestrator.requestTokenGrant(any())).thenReturn(OrchestratorGrpcClient.TokenGrant.immediate());
        try (ReplicationSender sender = new ReplicationSender("worker-1", orchestrator,
                new HadoopFsManager(null, null, null), null, 16 * 1024)) {
            assertTrue(sender.transferTask(task));
        }

        assertEquals("rw-r-----", PosixFilePermissions.toString(Files.getPosixFilePermissions(target)));
        assertEquals(SOURCE_MTIME, Files.getLastModifiedTime(target).toMillis());
    }
}
