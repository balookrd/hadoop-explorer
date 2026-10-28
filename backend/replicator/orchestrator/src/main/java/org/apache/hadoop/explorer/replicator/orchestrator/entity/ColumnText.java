package org.apache.hadoop.explorer.replicator.orchestrator.entity;

/**
 * Длины текстовых колонок и усечение текстов под них.
 *
 * <p>Сообщения и ошибки приходят от агентов и из исключений Hadoop/Hive и бывают сколь угодно
 * длинными. Строка длиннее колонки на PostgreSQL откатывает всю транзакцию, а в отчете агента
 * вместе с ней пропадали бы позиция CDC, журнал событий и DLQ. Пути не усекаются: обрезанный
 * путь хуже отказа.
 */
public final class ColumnText {

    /** Сообщения и тексты ошибок (V10). */
    public static final int MESSAGE_LENGTH = 4096;
    /** Пути и URI файлов и каталогов (V10). */
    public static final int PATH_LENGTH = 8192;
    /** Имя партиции вида k1=v1/k2=v2 (V10). */
    public static final int PARTITION_NAME_LENGTH = 2048;

    private static final String ELLIPSIS = "…";

    private ColumnText() {
    }

    public static String fit(String text, int maxLength) {
        if (text == null || text.length() <= maxLength) {
            return text;
        }
        return text.substring(0, maxLength - ELLIPSIS.length()) + ELLIPSIS;
    }

    public static String message(String text) {
        return fit(text, MESSAGE_LENGTH);
    }
}
