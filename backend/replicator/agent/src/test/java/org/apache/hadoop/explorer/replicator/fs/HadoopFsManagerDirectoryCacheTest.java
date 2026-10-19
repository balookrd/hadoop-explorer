package org.apache.hadoop.explorer.replicator.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class HadoopFsManagerDirectoryCacheTest {

    private File tempDir;
    private HadoopFsManager fsManager;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("fsmanager-cache-test").toFile();
        fsManager = new HadoopFsManager("file:///", null, null, tempDir.getAbsolutePath());
        fsManager.clearVerifiedDirectoriesCache();
    }

    @AfterEach
    void tearDown() throws IOException {
        fsManager.clearVerifiedDirectoriesCache();
        if (tempDir != null && tempDir.exists()) {
            Files.walk(tempDir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("PERF-04: Сессионный кэш должен исключать повторные вызовы fs.exists и fs.mkdirs к NameNode")
    void testSessionDirectoryCacheEliminatesRedundantRpc() throws IOException {
        FileSystem mockFs = mock(FileSystem.class);
        when(mockFs.exists(any(Path.class))).thenReturn(false, true);
        when(mockFs.mkdirs(any(Path.class))).thenReturn(true);

        Path parentDir = new Path("hdfs://nameservice1/warehouse/tables/sales_data/2026/04");
        Set<String> sessionCache = ConcurrentHashMap.newKeySet();

        // Первый вызов: каталог отсутствует в кэше -> вызывается fs.exists и fs.mkdirs
        fsManager.ensureParentDirectoryExists(parentDir, mockFs, sessionCache);

        verify(mockFs, times(1)).exists(parentDir);
        verify(mockFs, times(1)).mkdirs(parentDir);
        assertTrue(sessionCache.contains(parentDir.toString()));

        // Следующие 50 обращений к этой же директории должны обслуживаться из кэша без RPC к NameNode
        for (int i = 0; i < 50; i++) {
            fsManager.ensureParentDirectoryExists(parentDir, mockFs, sessionCache);
        }

        verify(mockFs, times(1)).exists(parentDir);
        verify(mockFs, times(1)).mkdirs(parentDir);
    }

    @Test
    @DisplayName("PERF-04: Глобальный LRU-кэш должен предотвращать повторные обращения между разными сессиями")
    void testGlobalLruCacheSharesDirectoryKnowledgeAcrossSessions() throws IOException {
        FileSystem mockFs = mock(FileSystem.class);
        when(mockFs.exists(any(Path.class))).thenReturn(true);

        Path parentDir = new Path("hdfs://nameservice1/warehouse/common/dim_customers");

        // Сессия 1: проверяет каталог и заполняет глобальный кэш
        Set<String> session1 = ConcurrentHashMap.newKeySet();
        fsManager.ensureParentDirectoryExists(parentDir, mockFs, session1);
        verify(mockFs, times(1)).exists(parentDir);

        // Сессия 2: отдельный локальный кэш, но каталог уже есть в глобальном LRU
        Set<String> session2 = ConcurrentHashMap.newKeySet();
        fsManager.ensureParentDirectoryExists(parentDir, mockFs, session2);

        // Повторного вызова к NameNode быть не должно!
        verify(mockFs, times(1)).exists(parentDir);
        assertTrue(session2.contains(parentDir.toString()));
    }

    @Test
    @DisplayName("PERF-04: Прямая запись 100 файлов в одну поддиректорию с кэшем директорий")
    void testWriteDirectWithSessionCache() throws Exception {
        File subDir = new File(tempDir, "bulk_subfolder");
        Set<String> sessionCache = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < 100; i++) {
            File targetFile = new File(subDir, "part_" + i + ".parquet");
            byte[] content = ("test-data-row-" + i).getBytes(StandardCharsets.UTF_8);
            fsManager.writeDirect(targetFile.getAbsolutePath(), content, null, true, sessionCache);
            assertTrue(targetFile.exists());
            assertEquals(content.length, targetFile.length());
        }

        assertTrue(sessionCache.contains(subDir.getAbsolutePath()));
        File[] files = subDir.listFiles();
        assertNotNull(files);
        assertEquals(100, files.length);
    }
}
