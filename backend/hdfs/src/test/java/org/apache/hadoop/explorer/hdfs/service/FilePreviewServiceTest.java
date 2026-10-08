package org.apache.hadoop.explorer.hdfs.service;

import org.apache.hadoop.explorer.hdfs.dto.file.FilePreviewResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class FilePreviewServiceTest {

    public static class UgiPatch {
        public static UserGroupInformation getCurrentUser() throws IOException {
            return UserGroupInformation.getLoginUser();
        }
    }

    @BeforeAll
    static void initHadoopPatch() {
        try {
            ByteBuddyAgent.install();
            new ByteBuddy()
                .redefine(UserGroupInformation.class)
                .method(ElementMatchers.named("getCurrentUser"))
                .intercept(MethodDelegation.to(UgiPatch.class))
                .make()
                .load(UserGroupInformation.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());
        } catch (Throwable t) {
            // Игнорируем, если не требуется (например, на Java 21)
        }
    }

    private FilePreviewService previewService;

    @BeforeEach
    void setUp() {
        previewService = new FilePreviewService();
    }

    @Test
    void testTextPreview() {
        String content = "Строка 1\nСтрока 2\nСтрока 3";
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

        FilePreviewResponse resp = previewService.generatePreview(
            "dev", "/tmp/notes.txt", new ByteArrayInputStream(bytes), bytes.length, 1024);

        assertEquals("txt", resp.fileType());
        assertEquals(content, resp.content());
        assertFalse(resp.truncated());
        assertNull(resp.error());
    }

    @Test
    void testCsvPreview() {
        String csv = "id,name,role\n1,Alice,Admin\n2,Bob,User\n";
        byte[] bytes = csv.getBytes(StandardCharsets.UTF_8);

        FilePreviewResponse resp = previewService.generatePreview(
            "dev", "/data/users.csv", new ByteArrayInputStream(bytes), bytes.length, 1024);

        assertEquals("csv", resp.fileType());
        assertNotNull(resp.columns());
        assertEquals(3, resp.columns().size());
        assertEquals("id", resp.columns().get(0));
        assertEquals("name", resp.columns().get(1));
        assertEquals("role", resp.columns().get(2));
        assertNotNull(resp.rows());
        assertEquals(2, resp.rows().size());
        assertEquals(2, resp.rowCount());
    }

    @Test
    void testOrcPreviewWithRealFile() throws Exception {
        File orcFile = new File("../../../demo/hdfs/hadoop/sample_data/transactions.orc");
        if (!orcFile.exists()) {
            orcFile = new File("demo/hdfs/hadoop/sample_data/transactions.orc");
        }

        if (orcFile.exists()) {
            try (FileInputStream in = new FileInputStream(orcFile)) {
                FilePreviewResponse resp = previewService.generatePreview(
                    "dev", "/data/transactions.orc", in, orcFile.length(), 1024 * 1024);

                assertEquals("orc", resp.fileType());
                assertNull(resp.error());
                assertNotNull(resp.columns());
                assertFalse(resp.columns().isEmpty());
                assertNotNull(resp.rows());
                assertFalse(resp.rows().isEmpty());
                assertTrue(resp.rowCount() > 0);
                assertNotNull(resp.content());
                assertTrue(resp.content().contains("Apache ORC"));
            }
        }
    }

    @Test
    void testParquetPreviewWithRealFile() throws Exception {
        File parquetFile = new File("../../../demo/hdfs/hadoop/sample_data/customers.parquet");
        if (!parquetFile.exists()) {
            parquetFile = new File("demo/hdfs/hadoop/sample_data/customers.parquet");
        }

        if (parquetFile.exists()) {
            try (FileInputStream in = new FileInputStream(parquetFile)) {
                FilePreviewResponse resp = previewService.generatePreview(
                    "dev", "/data/customers.parquet", in, parquetFile.length(), 1024 * 1024);

                assertEquals("parquet", resp.fileType());
                assertNull(resp.error());
                assertNotNull(resp.columns());
                assertFalse(resp.columns().isEmpty());
                assertNotNull(resp.rows());
                assertFalse(resp.rows().isEmpty());
                assertTrue(resp.rowCount() > 0);
                assertNotNull(resp.content());
                assertTrue(resp.content().contains("Apache Parquet"));
            }
        }
    }

    @Test
    void testCorruptedColumnarFileDoesNotCrash() {
        FilePreviewResponse resp = previewService.generatePreview(
            "dev", "/warehouse/corrupted.parquet", new ByteArrayInputStream(new byte[]{1, 2, 3, 4}), 4, 1024);

        assertEquals("parquet", resp.fileType());
        assertNotNull(resp.error());
    }
}
