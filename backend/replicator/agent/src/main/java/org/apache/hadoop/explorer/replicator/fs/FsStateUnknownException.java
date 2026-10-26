package org.apache.hadoop.explorer.replicator.fs;

/**
 * Состояние пути неизвестно: файловая система ответила ошибкой, а не «есть» или «нет».
 *
 * <p>Прежде такие ошибки глотались и превращались в «файла нет». При истекшем тикете Kerberos
 * или NameNode в safemode отправитель считал все файлы отсутствующими: бандл уходил пустым,
 * приемник отвечал успехом на нуле файлов, а задача закрывалась как выполненная. Непереданные
 * данные обнаруживались только при следующей сверке — или не обнаруживались вовсе.
 */
public class FsStateUnknownException extends RuntimeException {

    public FsStateUnknownException(String operation, String path, Throwable cause) {
        super("Не удалось определить состояние пути '" + path + "' (" + operation + "): "
                + (cause != null ? cause.getMessage() : "причина неизвестна"), cause);
    }
}
