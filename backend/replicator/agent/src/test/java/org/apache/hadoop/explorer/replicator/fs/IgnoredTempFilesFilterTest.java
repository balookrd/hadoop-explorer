package org.apache.hadoop.explorer.replicator.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("IgnoredTempFilesFilterTest: Проверка фильтрации временных файлов и каталогов")
class IgnoredTempFilesFilterTest {

    private File tempDir;
    private HadoopFsManager fsManager;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("temp_filter_test_").toFile();
        fsManager = new HadoopFsManager("file:///");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tempDir != null && tempDir.exists()) {
            Files.walk(tempDir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Проверка фильтрации служебных каталогов (начинающиеся с _ и .)")
    void testIsIgnoredDirectory() {
        // Папки коммиттеров Spark / MapReduce / Hive
        assertTrue(HadoopFsManager.isIgnoredDirectory("_temporary"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("_staging"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("_distcp"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("_tmp"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("_custom_dir"));

        // Скрытые каталоги и Hive / Tez / Spark staging
        assertTrue(HadoopFsManager.isIgnoredDirectory(".spark-staging-123"));
        assertTrue(HadoopFsManager.isIgnoredDirectory(".staging"));
        assertTrue(HadoopFsManager.isIgnoredDirectory(".tmp"));
        assertTrue(HadoopFsManager.isIgnoredDirectory(".Trash"));
        assertTrue(HadoopFsManager.isIgnoredDirectory(".git"));
        assertTrue(HadoopFsManager.isIgnoredDirectory(".idea"));
        assertTrue(HadoopFsManager.isIgnoredDirectory(".hive-staging_hive_2026-10-09_12-00-00_123_456"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("hive-staging-query999"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("_hive_staging_session"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("-ext-10000"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("-ext-10001"));
        assertTrue(HadoopFsManager.isIgnoredDirectory(".tez-staging-app"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("tez-staging-run"));

        // Системные и временные суффиксы
        assertTrue(HadoopFsManager.isIgnoredDirectory("lost+found"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("LOST+FOUND"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("data.tmp"));
        assertTrue(HadoopFsManager.isIgnoredDirectory("data.staging"));

        // Легитимные каталоги
        assertFalse(HadoopFsManager.isIgnoredDirectory("_delta_log")); // Транзакционные логи Delta Lake
        assertFalse(HadoopFsManager.isIgnoredDirectory("users"));
        assertFalse(HadoopFsManager.isIgnoredDirectory("sales_2026"));
        assertFalse(HadoopFsManager.isIgnoredDirectory("partition_date=2026-10-09"));
        assertFalse(HadoopFsManager.isIgnoredDirectory("parquet_tables"));
    }

    @Test
    @DisplayName("Проверка фильтрации временных, скрытых и незавершенных файлов")
    void testIsIgnoredFile() {
        // Скрытые файлы
        assertTrue(HadoopFsManager.isIgnoredFile(".DS_Store"));
        assertTrue(HadoopFsManager.isIgnoredFile(".part-00000.parquet.crc"));
        assertTrue(HadoopFsManager.isIgnoredFile(".hidden_file"));

        // Системный мусор ОС
        assertTrue(HadoopFsManager.isIgnoredFile("Thumbs.db"));
        assertTrue(HadoopFsManager.isIgnoredFile("thumbs.db"));
        assertTrue(HadoopFsManager.isIgnoredFile("desktop.ini"));

        // Расширения незавершенной записи
        assertTrue(HadoopFsManager.isIgnoredFile("data.tmp"));
        assertTrue(HadoopFsManager.isIgnoredFile("data.temp"));
        assertTrue(HadoopFsManager.isIgnoredFile("stream.inprogress"));
        assertTrue(HadoopFsManager.isIgnoredFile("job.staging"));
        assertTrue(HadoopFsManager.isIgnoredFile("upload.pending"));
        assertTrue(HadoopFsManager.isIgnoredFile("transfer.copying"));
        assertTrue(HadoopFsManager.isIgnoredFile("part-00000.part"));
        assertTrue(HadoopFsManager.isIgnoredFile("part-00000.partial"));
        assertTrue(HadoopFsManager.isIgnoredFile("report.swp"));
        assertTrue(HadoopFsManager.isIgnoredFile("report.swo"));
        assertTrue(HadoopFsManager.isIgnoredFile("backup.txt~"));

        // Маркеры копирования утилит
        assertTrue(HadoopFsManager.isIgnoredFile("part-00000.parquet._copying_"));
        assertTrue(HadoopFsManager.isIgnoredFile("file._COPYING_"));
        assertTrue(HadoopFsManager.isIgnoredFile("_copying_temp.dat"));

        // Временные файлы коммиттеров и движков
        assertTrue(HadoopFsManager.isIgnoredFile("_temporary_state"));
        assertTrue(HadoopFsManager.isIgnoredFile("_tmp_file"));
        assertTrue(HadoopFsManager.isIgnoredFile("query.hive-staging-task.xml"));
        assertTrue(HadoopFsManager.isIgnoredFile("data.spark-staging"));

        // Staging файлы Replicator (Zero-Staging временные файлы)
        assertTrue(HadoopFsManager.isIgnoredFile("large_table.parquet._staging_job-123"));
        assertTrue(HadoopFsManager.isIgnoredFile("data.csv._staging_task-456"));
        assertTrue(HadoopFsManager.isIgnoredFile("event.json.staging.tmp"));

        // ВАЖНО: Легитимные файлы метаданных НЕ должны отсекаться!
        assertFalse(HadoopFsManager.isIgnoredFile("_SUCCESS"));
        assertFalse(HadoopFsManager.isIgnoredFile("_SUCCESS.crc"));
        assertFalse(HadoopFsManager.isIgnoredFile("_metadata"));
        assertFalse(HadoopFsManager.isIgnoredFile("_common_metadata"));
        assertFalse(HadoopFsManager.isIgnoredFile("part-00000-c000.snappy.parquet"));
        assertFalse(HadoopFsManager.isIgnoredFile("customers.csv"));
        assertFalse(HadoopFsManager.isIgnoredFile("orders.orc"));
    }

    @Test
    @DisplayName("Проверка иерархических путей isIgnoredPath")
    void testIsIgnoredPath() {
        // Файлы внутри временных папок _temporary
        assertTrue(HadoopFsManager.isIgnoredPath("_temporary/0/task_20261009/part-0000.parquet"));
        assertTrue(HadoopFsManager.isIgnoredPath("warehouse/table/_temporary/part.parquet"));
        assertTrue(HadoopFsManager.isIgnoredPath("warehouse/table/_staging/task.dat"));

        // Файлы внутри скрытых папок .spark-staging / .tmp
        assertTrue(HadoopFsManager.isIgnoredPath(".spark-staging-12345/app.jar"));
        assertTrue(HadoopFsManager.isIgnoredPath("data/.tmp/file.parquet"));
        assertTrue(HadoopFsManager.isIgnoredPath("data/.staging/job.xml"));
        assertTrue(HadoopFsManager.isIgnoredPath(".Trash/Current/warehouse/table/part.parquet"));

        // Временные файлы внутри легитимных папок
        assertTrue(HadoopFsManager.isIgnoredPath("warehouse/table/part-00000.parquet.tmp"));
        assertTrue(HadoopFsManager.isIgnoredPath("warehouse/table/part-00000.parquet.inprogress"));
        assertTrue(HadoopFsManager.isIgnoredPath("warehouse/table/.DS_Store"));
        assertTrue(HadoopFsManager.isIgnoredPath("warehouse/table/part-00000.parquet._COPYING_"));

        // Легитимные файлы внутри легитимных папок
        assertFalse(HadoopFsManager.isIgnoredPath("warehouse/table/part-00000.parquet"));
        assertFalse(HadoopFsManager.isIgnoredPath("warehouse/table/subfolder/data.csv"));
        assertFalse(HadoopFsManager.isIgnoredPath("warehouse/table/_SUCCESS"));
        assertFalse(HadoopFsManager.isIgnoredPath("warehouse/table/_metadata"));
        assertFalse(HadoopFsManager.isIgnoredPath("warehouse/table/_common_metadata"));
        assertFalse(HadoopFsManager.isIgnoredPath("_SUCCESS"));
    }

    @Test
    @DisplayName("Интеграционный тест: рекурсивный обход listFilesRecursively отфильтровывает все временные файлы и папки")
    void testListFilesRecursivelyFiltering() throws IOException {
        // 1. Создаем легитимные файлы
        createFile("data_part1.parquet");
        createFile("data_part2.parquet");
        createFile("_SUCCESS");
        createFile("_metadata");
        createFile("nested/partition_1/data_part3.parquet");

        // 2. Создаем временные папки и файлы коммиттеров
        createFile("_temporary/0/task_001/part-00000.parquet");
        createFile("_staging/job_999/task.dat");
        createFile(".spark-staging-12345/executor.jar");
        createFile(".hive-staging_hive_2026-10-09/-ext-10000/000000_0");
        createFile("nested/hive-staging-task/query.xml");
        createFile(".hidden_dir/file.txt");
        createFile("nested/.tmp/temp_cache.bin");

        // 3. Создаем временные файлы незавершенной записи и мусор ОС
        createFile("invalid.tmp");
        createFile("upload.inprogress");
        createFile(".DS_Store");
        createFile("nested/Thumbs.db");
        createFile("part-00000.parquet._copying_");

        // Выполняем рекурсивный сбор файлов
        List<HadoopFsManager.FileItem> items = fsManager.listFilesRecursively(tempDir.getAbsolutePath());

        List<String> relativePaths = items.stream()
                .map(HadoopFsManager.FileItem::relativePath)
                .sorted()
                .toList();

        // Проверяем, что в списке только 5 легитимных файлов
        assertEquals(5, relativePaths.size(), "Должны присутствовать только легитимные файлы: " + relativePaths);
        assertTrue(relativePaths.contains("_SUCCESS"));
        assertTrue(relativePaths.contains("_metadata"));
        assertTrue(relativePaths.contains("data_part1.parquet"));
        assertTrue(relativePaths.contains("data_part2.parquet"));
        assertTrue(relativePaths.contains("nested/partition_1/data_part3.parquet"));

        // Проверяем, что ни один временный файл или путь не попал в выборку
        for (String rel : relativePaths) {
            assertFalse(rel.contains("_temporary"), "Не должно содержать _temporary: " + rel);
            assertFalse(rel.contains("_staging"), "Не должно содержать _staging: " + rel);
            assertFalse(rel.contains(".spark-staging"), "Не должно содержать .spark-staging: " + rel);
            assertFalse(rel.contains(".tmp"), "Не должно содержать .tmp: " + rel);
            assertFalse(rel.endsWith(".tmp"), "Не должно заканчиваться на .tmp: " + rel);
            assertFalse(rel.endsWith(".inprogress"), "Не должно заканчиваться на .inprogress: " + rel);
            assertFalse(rel.contains("_copying_"), "Не должно содержать _copying_: " + rel);
            assertFalse(rel.contains(".DS_Store"), "Не должно содержать .DS_Store: " + rel);
            assertFalse(rel.contains("Thumbs.db"), "Не должно содержать Thumbs.db: " + rel);
        }
    }

    private void createFile(String relativePath) throws IOException {
        File target = new File(tempDir, relativePath);
        File parent = target.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        Files.writeString(target.toPath(), "test data for " + relativePath);
    }
}
