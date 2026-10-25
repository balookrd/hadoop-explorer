package org.apache.hadoop.explorer.replicator.security;

import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Обход защиты путей через схему URI.
 *
 * <p>Проверка запрещенных системных префиксов разбирала только форму {@code scheme://host/path},
 * поэтому {@code file:/etc/shadow} до нее не доходил — путь оставался строкой «file:/etc/...»,
 * которая ни с одним префиксом не совпадает. А решение «локальный ли путь» принималось по
 * файловой системе по умолчанию: при {@code fs.defaultFS = hdfs://...} путь с явной схемой
 * {@code file://}
 * считался удаленным, и проверка границ Chroot-песочницы пропускалась целиком, хотя Hadoop
 * открывает такой путь через LocalFileSystem — то есть читает файл хоста.
 */
@DisplayName("SEC: обход валидации путей через схему URI")
class PathSchemeSandboxBypassTest {

    private String sandboxRoot;

    @BeforeEach
    void setUp() throws IOException {
        Path sandbox = Files.createTempDirectory("replicator-scheme-bypass");
        sandboxRoot = sandbox.toFile().getCanonicalPath();
    }

    @Test
    @DisplayName("Системные пути ОС заблокированы и в форме 'file:/path' без двойного слеша")
    void singleSlashSchemeShouldNotSkipForbiddenPrefixes() {
        assertThrows(SecurityException.class,
                () -> HadoopFsManager.validatePathSafety("file:/etc/shadow"),
                "file:/etc/shadow должен быть отвергнут так же, как /etc/shadow");
        assertThrows(SecurityException.class,
                () -> HadoopFsManager.validatePathSafety("FILE:/proc/self/environ"),
                "Схема регистронезависима: FILE:/proc/... тоже системный путь");
        assertThrows(SecurityException.class,
                () -> HadoopFsManager.validatePathSafety("file:/var/run/secrets/token"),
                "file:/var/run/... должен быть отвергнут");
    }

    @Test
    @DisplayName("Точечные сегменты не прячут системный путь от проверки")
    void dotSegmentsShouldNotHideForbiddenPrefixes() {
        assertThrows(SecurityException.class,
                () -> HadoopFsManager.validatePathSafety("file:///./etc/shadow"),
                "Сегмент '.' не должен обходить проверку префиксов");
        assertThrows(SecurityException.class,
                () -> HadoopFsManager.validatePathSafety("/etc//passwd"),
                "Двойной слеш не должен обходить проверку префиксов");
    }

    @Test
    @DisplayName("Путь со схемой file:// считается локальным даже при HDFS по умолчанию")
    void fileSchemeShouldBeTreatedAsLocal() {
        HadoopFsManager fsManager = new HadoopFsManager("hdfs://nameservice1", null, null, sandboxRoot);

        assertTrue(fsManager.isLocalPath("file:///var/tmp/leak"),
                "Явная схема file:// означает локальную ФС независимо от fs.defaultFS");
        assertTrue(fsManager.isLocalPath("file:/var/tmp/leak"),
                "Форма file:/path — тоже локальная ФС");
        assertFalse(fsManager.isLocalPath("hdfs://nameservice1/data/warehouse"),
                "Путь HDFS остается удаленным");
        assertFalse(fsManager.isLocalPath("s3a://bucket/key"),
                "Чужая схема объектного хранилища не является локальной ФС");
    }

    @Test
    @DisplayName("Chroot-песочница закрывает локальный путь со схемой file:// при HDFS по умолчанию")
    void fileSchemeShouldNotEscapeChrootSandbox() {
        HadoopFsManager fsManager = new HadoopFsManager("hdfs://nameservice1", null, null, sandboxRoot);

        assertThrows(SecurityException.class,
                () -> fsManager.validateSandboxAccess("file:///var/tmp/leak"),
                "Локальный путь вне песочницы должен быть отвергнут, даже если fs.defaultFS — HDFS");
        assertThrows(SecurityException.class,
                () -> fsManager.validateSandboxAccess("file:" + sandboxRoot + "-neighbour/leak"),
                "Префиксное совпадение с каталогом песочницы не делает путь разрешенным");
    }

    @Test
    @DisplayName("Разрешенные пути остаются доступными")
    void legitimatePathsShouldStillPass() {
        HadoopFsManager fsManager = new HadoopFsManager("hdfs://nameservice1", null, null, sandboxRoot);

        assertDoesNotThrow(() -> fsManager.validateSandboxAccess("hdfs://nameservice1/user/hive/warehouse"),
                "Путь HDFS не подпадает под ограничения локальной песочницы");
        assertDoesNotThrow(() -> fsManager.validateSandboxAccess("/data/warehouse/tables"),
                "Путь без схемы при HDFS по умолчанию остается путем HDFS");
        assertDoesNotThrow(() -> fsManager.validateSandboxAccess("file://" + sandboxRoot + "/staging/part-0"),
                "Локальный путь внутри песочницы разрешен");
        assertDoesNotThrow(() -> HadoopFsManager.validatePathSafety("file:///tmp/replicator/staging"),
                "Обычный временный путь не является системным");
    }
}
