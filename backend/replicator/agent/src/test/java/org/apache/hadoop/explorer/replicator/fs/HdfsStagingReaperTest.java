package org.apache.hadoop.explorer.replicator.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.explorer.replicator.security.KerberosUgiProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("HdfsStagingReaper Unit Tests")
class HdfsStagingReaperTest {

    private Path tempDir;
    private HdfsStagingReaper reaper;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("staging-reaper-test");
        Configuration conf = new Configuration();
        KerberosUgiProvider ugiProvider = new KerberosUgiProvider(conf);
        reaper = new HdfsStagingReaper(conf, ugiProvider, "file:///");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("cleanStagingFiles: удаляет только осиротевшие staging-файлы с подходящим jobId и возрастом")
    void testCleanStagingFiles() throws Exception {
        // Создаем легитимный файл данных
        Path legitimateFile = tempDir.resolve("important_data.parquet");
        Files.writeString(legitimateFile, "parquet data");

        // Создаем staging файл для job-123
        Path stagingJob123 = tempDir.resolve("important_data.parquet._staging_job-123");
        Files.writeString(stagingJob123, "partial data 123");

        // Создаем staging файл для job-456
        Path stagingJob456 = tempDir.resolve("other_data.orc._staging_job-456");
        Files.writeString(stagingJob456, "partial data 456");

        // Очистка только для job-123 (без фильтра по возрасту)
        int cleaned = reaper.cleanStagingFiles(tempDir.toString(), "job-123", 0L, null, true);
        assertEquals(1, cleaned);

        assertFalse(Files.exists(stagingJob123), "Staging файл job-123 должен быть удален");
        assertTrue(Files.exists(legitimateFile), "Легитимный файл не должен быть удален");
        assertTrue(Files.exists(stagingJob456), "Staging файл job-456 не должен быть затронут");

        // Очистка всех staging файлов без фильтра jobId
        int cleanedRemaining = reaper.cleanStagingFiles(tempDir.toString(), null, 0L, null, true);
        assertEquals(1, cleanedRemaining);
        assertFalse(Files.exists(stagingJob456), "Staging файл job-456 должен быть удален");
        assertTrue(Files.exists(legitimateFile), "Легитимный файл по-прежнему на месте");
    }

    @Test
    @DisplayName("cleanStagingFiles: пустой или несуществующий путь возвращает 0")
    void testCleanStagingFilesNonExistent() {
        assertEquals(0, reaper.cleanStagingFiles(null, null, 0L, null, true));
        assertEquals(0, reaper.cleanStagingFiles("", null, 0L, null, true));
        assertEquals(0, reaper.cleanStagingFiles("/non/existent/path/for/sure/123", null, 0L, null, true));
    }
}
