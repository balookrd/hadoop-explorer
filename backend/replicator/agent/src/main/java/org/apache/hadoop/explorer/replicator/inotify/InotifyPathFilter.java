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
        String normalizedEvent = normalize(eventPath);
        String normalizedJob = normalize(jobSourcePath);

        return normalizedEvent.startsWith(normalizedJob);
    }

    private static String normalize(String p) {
        String s = p.trim();
        if (!s.startsWith("/")) {
            s = "/" + s;
        }
        if (s.endsWith("/") && s.length() > 1) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }
}
