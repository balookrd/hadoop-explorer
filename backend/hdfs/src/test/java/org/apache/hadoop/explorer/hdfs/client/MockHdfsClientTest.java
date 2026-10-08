package org.apache.hadoop.explorer.hdfs.client;

import org.apache.hadoop.explorer.hdfs.dto.file.HdfsFileStatus;
import org.apache.hadoop.explorer.hdfs.exception.HdfsLocalizedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MockHdfsClientTest {

    private MockHdfsClient client;

    @BeforeEach
    void setUp() {
        client = new MockHdfsClient();
    }

    @Test
    void testInitialListing() {
        List<HdfsFileStatus> rootFiles = client.listStatus("/", "testuser");
        assertNotNull(rootFiles);
        assertFalse(rootFiles.isEmpty());

        List<String> names = rootFiles.stream().map(HdfsFileStatus::getPathSuffix).toList();
        assertTrue(names.contains("tmp"));
        assertTrue(names.contains("user"));
        assertTrue(names.contains("data"));
    }

    @Test
    void testFileCreateAndRead() throws Exception {
        String testPath = "/tmp/my_test_file.txt";
        byte[] payload = "Hello HDFS from Java 21!".getBytes(StandardCharsets.UTF_8);

        client.create(testPath, new ByteArrayInputStream(payload), "testuser", true);

        HdfsFileStatus status = client.getFileStatus(testPath, "testuser");
        assertEquals("FILE", status.getType());
        assertEquals(payload.length, status.getLength());
        assertEquals("testuser", status.getOwner());

        try (InputStream in = client.open(testPath, "testuser", 0, null)) {
            byte[] readBytes = in.readAllBytes();
            assertEquals("Hello HDFS from Java 21!", new String(readBytes, StandardCharsets.UTF_8));
        }
    }

    @Test
    void testMkdirsAndRename() {
        client.mkdirs("/data/test_folder/nested", "admin");
        HdfsFileStatus folderStatus = client.getFileStatus("/data/test_folder/nested", "admin");
        assertEquals("DIRECTORY", folderStatus.getType());

        client.rename("/data/test_folder/nested", "/data/test_folder/renamed", "admin");
        HdfsFileStatus renamedStatus = client.getFileStatus("/data/test_folder/renamed", "admin");
        assertEquals("DIRECTORY", renamedStatus.getType());

        assertThrows(HdfsLocalizedException.class, () ->
            client.getFileStatus("/data/test_folder/nested", "admin"));
    }

    @Test
    void testDelete() {
        client.create("/tmp/to_delete.txt", new ByteArrayInputStream("del".getBytes()), "user", true);
        assertTrue(client.delete("/tmp/to_delete.txt", "user", false));
        assertThrows(HdfsLocalizedException.class, () ->
            client.getFileStatus("/tmp/to_delete.txt", "user"));
    }

    @Test
    void testUserHomeAutoCreation() {
        client.listStatus("/", "developer_alex");
        HdfsFileStatus home = client.getFileStatus("/user/developer_alex", "developer_alex");
        assertEquals("DIRECTORY", home.getType());
        assertEquals("developer_alex", home.getOwner());
    }
}
