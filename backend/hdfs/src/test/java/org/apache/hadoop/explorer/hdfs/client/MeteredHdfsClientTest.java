package org.apache.hadoop.explorer.hdfs.client;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.hadoop.explorer.hdfs.exception.HdfsLocalizedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class MeteredHdfsClientTest {

    private SimpleMeterRegistry registry;
    private MockHdfsClient delegate;
    private MeteredHdfsClient client;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        delegate = new MockHdfsClient();
        client = new MeteredHdfsClient(delegate, registry, "dev-cluster");
    }

    @Test
    void countsSuccessfulOperationsPerClusterAndOperation() {
        client.listStatus("/", "testuser");
        client.mkdirs("/tmp/metrics_dir", "testuser");
        client.rename("/tmp/metrics_dir", "/tmp/metrics_dir_renamed", "testuser");
        client.delete("/tmp/metrics_dir_renamed", "testuser", true);

        assertEquals(1.0, operations("list_status", "success"));
        assertEquals(1.0, operations("mkdirs", "success"));
        assertEquals(1.0, operations("rename", "success"));
        assertEquals(1.0, operations("delete", "success"));
    }

    @Test
    void countsFailedOperationAndRethrowsOriginalException() {
        assertThrows(HdfsLocalizedException.class,
            () -> client.getFileStatus("/no/such/file", "testuser"));

        assertEquals(1.0, operations("get_file_status", "failed"));
        assertEquals(0.0, operations("get_file_status", "success"));
    }

    @Test
    void countsUploadedAndDownloadedBytes() throws Exception {
        byte[] payload = "0123456789".getBytes(StandardCharsets.UTF_8);

        client.create("/tmp/metrics_bytes.txt", new ByteArrayInputStream(payload), "testuser", true);
        try (InputStream in = client.open("/tmp/metrics_bytes.txt", "testuser", 0, null)) {
            assertArrayEquals(payload, in.readAllBytes());
        }

        assertEquals(10.0, bytes("upload"));
        assertEquals(10.0, bytes("download"));
        assertEquals(1.0, operations("create", "success"));
        assertEquals(1.0, operations("open", "success"));
    }

    @Test
    void exposesWrappedClient() {
        assertSame(delegate, client.delegate());
    }

    private double operations(String operation, String status) {
        Counter counter = registry.find(MeteredHdfsClient.OPERATIONS_METRIC)
            .tags("cluster", "dev-cluster", "operation", operation, "status", status)
            .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private double bytes(String direction) {
        Counter counter = registry.find(MeteredHdfsClient.BYTES_METRIC)
            .tags("cluster", "dev-cluster", "direction", direction)
            .counter();
        return counter == null ? 0.0 : counter.count();
    }
}
