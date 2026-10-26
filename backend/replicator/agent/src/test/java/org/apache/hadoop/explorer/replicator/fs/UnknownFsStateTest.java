package org.apache.hadoop.explorer.replicator.fs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * «Не знаю» не должно превращаться в «нет».
 *
 * <p>Проверки состояния пути глотали любое исключение и отвечали «файла нет» или «размер 0».
 * При истекшем тикете Kerberos или NameNode в safemode отправитель считал все файлы
 * отсутствующими: бандл уходил пустым, приемник отвечал успехом на нуле файлов, задача
 * закрывалась как выполненная, а данные не переносились.
 */
@DisplayName("Fail-open: ошибка доступа к ФС не выдается за отсутствие файла")
class UnknownFsStateTest {

    private static final String FAILING_PATH = FailingFileSystem.SCHEME + "://nameservice/warehouse/sales/part-0";

    private HadoopFsManager fsManager;

    @BeforeEach
    void setUp() {
        fsManager = new HadoopFsManager("hdfs://nameservice", null, null, null);
        fsManager.getConfiguration().set("fs." + FailingFileSystem.SCHEME + ".impl",
                FailingFileSystem.class.getName());
        fsManager.getConfiguration().setBoolean("fs." + FailingFileSystem.SCHEME + ".impl.disable.cache", true);
    }

    @Test
    @DisplayName("exists сообщает об ошибке доступа, а не отвечает «нет»")
    void existsShouldNotHideAccessFailure() {
        RuntimeException error = assertThrows(RuntimeException.class, () -> fsManager.exists(FAILING_PATH));

        assertTrue(error.getMessage() != null && error.getMessage().contains(FAILING_PATH),
                "В ошибке должен быть путь: " + error.getMessage());
    }

    @Test
    @DisplayName("getFileSize сообщает об ошибке доступа, а не отвечает «0 байт»")
    void fileSizeShouldNotHideAccessFailure() {
        assertThrows(java.io.IOException.class, () -> fsManager.getFileSize(FAILING_PATH));
    }

    @Test
    @DisplayName("isDirectory сообщает об ошибке доступа")
    void isDirectoryShouldNotHideAccessFailure() {
        assertThrows(RuntimeException.class, () -> fsManager.isDirectory(FAILING_PATH));
    }

    @Test
    @DisplayName("Время изменения сообщает об ошибке доступа")
    void modificationTimeShouldNotHideAccessFailure() {
        assertThrows(RuntimeException.class,
                () -> fsManager.getFileModificationTime(FAILING_PATH, null, true));
    }
}
