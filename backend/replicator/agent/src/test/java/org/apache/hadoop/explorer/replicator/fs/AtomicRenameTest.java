package org.apache.hadoop.explorer.replicator.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Атомарность коммита файла на целевой файловой системе.
 *
 * <p>Ключевая гарантия (DATA-LOSS): коммит не должен иметь окна, в котором прежняя версия
 * целевого файла уже удалена, а новая еще не переименована. Схема
 * {@code exists → delete → rename} такое окно создает: сбой между delete и rename
 * (failover NameNode, пропавший staging, отказ прав) безвозвратно уничтожает данные.
 */
@DisplayName("Атомарный overwrite-rename при коммите файла")
class AtomicRenameTest {

    private File tempDir;
    private FileSystem fs;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("atomic-rename-test").toFile();
        // LocalFileSystem инициализируется напрямую: FileSystem.get() на JDK 21 упирается
        // в UGI (Subject.getSubject больше не поддерживается), а для проверки семантики
        // коммита достаточно локальной ФС.
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(new URI("file:///"), new Configuration());
        fs = localFs;
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
    @DisplayName("Неудачный коммит сохраняет прежнюю версию целевого файла")
    void testFailedRenameKeepsPreviousTargetVersion() throws Exception {
        File target = new File(tempDir, "sales.parquet");
        Files.writeString(target.toPath(), "PREVIOUS_PRODUCTION_DATA");
        Path missingStaging = new Path(new File(tempDir, "sales.parquet._staging_gone").getAbsolutePath());

        // Сбой коммита допустимо сигнализировать и исключением, и false — важно лишь то,
        // что прежняя версия целевого файла при этом остается целой.
        boolean renamed;
        try {
            renamed = HadoopFsManager.atomicRenameOverwrite(fs, missingStaging, new Path(target.getAbsolutePath()));
        } catch (IOException expected) {
            renamed = false;
        }

        assertFalse(renamed, "Переименование отсутствующего staging-файла не может считаться успешным");
        assertTrue(target.isFile(),
                "Прежняя версия целевого файла удалена при неудачном коммите — это безвозвратная потеря данных");
        assertEquals("PREVIOUS_PRODUCTION_DATA", Files.readString(target.toPath()),
                "Содержимое прежней версии должно остаться неизменным");
    }

    @Test
    @DisplayName("Успешный коммит перезаписывает целевой файл и убирает staging")
    void testSuccessfulRenameOverwritesTarget() throws Exception {
        File target = new File(tempDir, "sales.parquet");
        Files.writeString(target.toPath(), "OLD_DATA");
        File staging = new File(tempDir, "sales.parquet._staging_1");
        Files.writeString(staging.toPath(), "NEW_DATA");

        boolean renamed = HadoopFsManager.atomicRenameOverwrite(
                fs, new Path(staging.getAbsolutePath()), new Path(target.getAbsolutePath()));

        assertTrue(renamed, "Коммит существующего staging-файла должен проходить");
        assertEquals("NEW_DATA", Files.readString(target.toPath()), "Целевой файл обязан быть перезаписан");
        assertFalse(staging.exists(), "Staging-файл должен исчезнуть после переименования");
    }

    @Test
    @DisplayName("Коммит в отсутствующий целевой путь создает файл")
    void testRenameToNewTargetCreatesFile() throws Exception {
        File staging = new File(tempDir, "new_table.parquet._staging_1");
        Files.writeString(staging.toPath(), "FRESH_DATA");
        File target = new File(tempDir, "new_table.parquet");

        boolean renamed = HadoopFsManager.atomicRenameOverwrite(
                fs, new Path(staging.getAbsolutePath()), new Path(target.getAbsolutePath()));

        assertTrue(renamed);
        assertEquals("FRESH_DATA", Files.readString(target.toPath()));
    }
}
