package io.github.balookrd.pristan.replicator.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.github.balookrd.pristan.replicator.client.OrchestratorGrpcClient;
import io.github.balookrd.pristan.replicator.fs.HadoopFsManager;
import io.github.balookrd.pristan.replicator.fs.HadoopFsManager.FileAttributes;
import io.github.balookrd.pristan.replicator.model.TaskItemDto;
import io.github.balookrd.pristan.replicator.sender.ReplicationSender;
import io.github.balookrd.pristan.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Replication factor, ACL entries and xattrs read on the source reach {@code applyAttributes} on
 * the receiver, both for a single file and for a file inside a TAR bundle.
 *
 * <p>The local file system of the test has none of the three, so the source attributes are
 * stubbed and the call on the receiver is captured. The HDFS calls themselves are checked on the stand.
 */
@DisplayName("ACL, xattr и фактор репликации доходят до приемника")
class ExtendedAttributesWireTest {

    private static final byte[] XATTR_VALUE = {0x00, (byte) 0xFF, 0x10, 'a'};
    private static final FileAttributes SOURCE = new FileAttributes(0640, 1_700_000_000_123L, "alice", "etl",
            2, List.of("user:bob:r--", "group:analysts:r-x"), Map.of("user.origin", XATTR_VALUE));

    @TempDir
    Path tmp;

    private Server server;
    private HadoopFsManager senderFs;
    private HadoopFsManager receiverFs;
    private Path source;
    private String address;

    @BeforeEach
    void setUp() throws Exception {
        source = tmp.resolve("source/part-0.json");
        Files.createDirectories(source.getParent());
        Files.write(source, "{\"id\":1}".getBytes(StandardCharsets.UTF_8));

        senderFs = spy(new HadoopFsManager(null, null, null));
        doReturn(SOURCE).when(senderFs).getAttributes(eq(source.toString()), any(), anyBoolean());
        receiverFs = spy(new HadoopFsManager(null, null, null));

        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new DataTransferServiceImpl(tmp.resolve("staging").toString(), receiverFs,
                        new LocalBandwidthLimiter(0.0)))
                .build().start();
        address = "inprocess:" + name;
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.shutdownNow();
    }

    private ReplicationSender sender() {
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        when(orchestrator.requestTokenGrant(any())).thenReturn(OrchestratorGrpcClient.TokenGrant.immediate());
        return new ReplicationSender("worker-1", orchestrator, senderFs, null, 16 * 1024, false, null, null, null,
                false, 1024 * 1024L, 16 * 1024 * 1024L, 500);
    }

    private TaskItemDto task(String type, String targetPath) throws Exception {
        TaskItemDto task = new TaskItemDto();
        task.setId("task-1");
        task.setJobId("job-1");
        task.setTaskType(type);
        task.setFileSize(Files.size(source));
        task.setTargetAddress(address);
        task.setTargetPath(targetPath);
        if ("BUNDLE_TAR".equals(type)) {
            task.setSourcePath(source.getParent().toString());
            task.setFileCount(1);
            task.setBundleManifest(new ObjectMapper().writeValueAsString(List.of(new ReplicationSender.BundleFileEntry(
                    source.toString(), source.getFileName().toString(), Files.size(source)))));
        } else {
            task.setSourcePath(source.toString());
        }
        return task;
    }

    private void assertAppliedOnReceiver() throws Exception {
        ArgumentCaptor<FileAttributes> applied = ArgumentCaptor.forClass(FileAttributes.class);
        verify(receiverFs).applyAttributes(anyString(), applied.capture(), any(), anyBoolean());
        FileAttributes attributes = applied.getValue();
        assertEquals(2, attributes.replication());
        assertEquals(List.of("user:bob:r--", "group:analysts:r-x"), attributes.aclEntries());
        assertEquals(1, attributes.xattrs().size());
        assertArrayEquals(XATTR_VALUE, attributes.xattrs().get("user.origin"));
        assertEquals("alice", attributes.owner());
    }

    @Test
    @DisplayName("Пофайловая передача")
    void singleFileTransferCarriesExtendedAttributes() throws Exception {
        try (ReplicationSender sender = sender()) {
            assertTrue(sender.transferTask(task("FILE", tmp.resolve("target/part-0.json").toString())));
        }
        assertAppliedOnReceiver();
    }

    @Test
    @DisplayName("Файл в TAR-бандле")
    void bundledFileCarriesExtendedAttributes() throws Exception {
        try (ReplicationSender sender = sender()) {
            assertTrue(sender.transferTask(task("BUNDLE_TAR", tmp.resolve("target").toString())));
        }
        assertAppliedOnReceiver();
    }
}
