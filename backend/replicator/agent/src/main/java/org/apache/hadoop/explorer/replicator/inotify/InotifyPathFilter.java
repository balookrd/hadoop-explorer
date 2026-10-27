package org.apache.hadoop.explorer.replicator.inotify;

import java.util.List;

/**
 * Фильтр путей для HDFS Inotify событий.
 * Отсеивает временные и staging пути Spark/Hive/MapReduce
 * для предотвращения репликации неполных или промежуточных данных до момента коммита.
 */
public class InotifyPathFilter {

    private static final List<String> STAGING_MARKERS = List.of(
            "/_temporary/",
            "/_temporary",
            "/.hive-staging",
            "/.spark-staging",
            "/.staging/",
            "/.staging",
            "/.tmp/",
            "/.tmp",
            "/.Trash/",
            "/.Trash"
    );

    private static final List<String> IGNORED_SUFFIXES = List.of(
            ".tmp",
            ".crc",
            ".inprogress"
    );

    /**
     * Проверяет, является ли путь staging или временным каталогом.
     */
    public static boolean isStagedPath(String path) {
        if (path == null || path.isBlank()) {
            return false;
        }

        for (String marker : STAGING_MARKERS) {
            if (path.contains(marker)) {
                return true;
            }
        }

        for (String suffix : IGNORED_SUFFIXES) {
            if (path.endsWith(suffix)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Проверяет, является ли операция RenameEvent коммитом данных из staging в постоянное хранилище.
     */
    public static boolean isStagingCommit(String srcPath, String dstPath) {
        return isStagedPath(srcPath) && !isStagedPath(dstPath);
    }

    /**
     * Проверяет, принадлежит ли путь целевому каталогу репликации задачи.
     */
    public static boolean matchesJobPrefix(String eventPath, String jobSourcePath) {
        if (eventPath == null || jobSourcePath == null) {
            return false;
        }
        String normalizedEvent = pathOf(eventPath);
        String normalizedJob = pathOf(jobSourcePath);

        // Граница каталога: задача на /db.db не забирает события соседнего /db.dbx
        return normalizedEvent.equals(normalizedJob)
                || normalizedJob.equals("/")
                || normalizedEvent.startsWith(normalizedJob + "/");
    }

    /**
     * Путь без схемы и адреса NameNode, с ведущим и без завершающего слеша.
     *
     * <p>События inotify приходят с путями без схемы, а каталог задачи бывает полным URI:
     * компаньон HMS-задачи хранит расположение базы так, как его отдает метастор
     * ({@code hdfs://nn:9000/warehouse/db.db}). Сравнение строк целиком не совпадало ни с одним
     * событием, и стриминг HMS-задач не переносил ничего.
     */
    static String pathOf(String p) {
        if (p == null) {
            return "/";
        }
        String s = p.trim();
        int scheme = s.indexOf("://");
        if (scheme > 0) {
            int pathStart = s.indexOf('/', scheme + 3);
            s = pathStart >= 0 ? s.substring(pathStart) : "/";
        }
        if (!s.startsWith("/")) {
            s = "/" + s;
        }
        while (s.endsWith("/") && s.length() > 1) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }
}
