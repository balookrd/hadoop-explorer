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

    @Test
    @DisplayName("cleanStagingFiles: легитимные файлы пользователей со словом staging в имени НЕ удаляются")
    void testLegitimateUserFileWithStagingInNameIsNotDeleted() throws Exception {
        Path userStagingFile1 = tempDir.resolve("orders_staging_daily.parquet");
        Path userStagingFile2 = tempDir.resolve("etl_staging_log.orc");
        Path validStagingFile = tempDir.resolve("data.parquet._staging_uuid-999");

        Files.writeString(userStagingFile1, "user data 1");
        Files.writeString(userStagingFile2, "user data 2");
        Files.writeString(validStagingFile, "temporary replica chunk");

        // Устанавливаем mtime в прошлое для всех трех файлов (2 часа назад)
        long twoHoursAgo = System.currentTimeMillis() - (2 * 60 * 60 * 1000L);
        new File(userStagingFile1.toString()).setLastModified(twoHoursAgo);
        new File(userStagingFile2.toString()).setLastModified(twoHoursAgo);
        new File(validStagingFile.toString()).setLastModified(twoHoursAgo);

        // Запуск очистки без фильтра по jobId и с TTL = 1 час (3600000 мс)
        int cleaned = reaper.cleanStagingFiles(tempDir.toString(), null, 3600000L, null, true);

        assertEquals(1, cleaned, "Должен быть удален только реальный staging-файл");
        assertFalse(Files.exists(validStagingFile), "Staging файл с суффиксом ._staging_<id> должен быть удален");
        assertTrue(Files.exists(userStagingFile1), "orders_staging_daily.parquet НЕ должен быть удален");
        assertTrue(Files.exists(userStagingFile2), "etl_staging_log.orc НЕ должен быть удален");
    }

    @Test
    @DisplayName("cleanStagingFiles: активные staging-файлы в состоянии under construction не удаляются рипером")
    void testFileUnderConstructionIsNotDeleted() throws Exception {
        Path activeStaging = tempDir.resolve("big_file.parquet._staging_job-active");
        Files.writeString(activeStaging, "active stream in progress");

        // Устанавливаем mtime в прошлое (2 часа назад)
        long twoHoursAgo = System.currentTimeMillis() - (2 * 60 * 60 * 1000L);
        new File(activeStaging.toString()).setLastModified(twoHoursAgo);

        // Создаем подкласс рипера с переопределенным isFileUnderConstruction = true
        HdfsStagingReaper mockUnderConstructionReaper = new HdfsStagingReaper(new Configuration(), null, "file:///") {
            @Override
            boolean isFileUnderConstruction(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path path) {
                return true;
            }
        };

        // Запуск очистки для HDFS пути через Mock-рипер
        int cleaned = mockUnderConstructionReaper.cleanStagingFiles(tempDir.toUri().toString(), "job-active", 3600000L, null, true);
        assertEquals(0, cleaned, "Файл under construction не должен быть удален");
        assertTrue(Files.exists(activeStaging), "Активный файл должен остаться на диске");
    }
}
