package org.apache.hadoop.explorer.replicator.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.explorer.replicator.security.KerberosUgiProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Сервис обнаружения и очистки осиротевших staging-файлов (*._staging_*) в HDFS и локальной ФС.
 * Предотвращает накопление мусора при сбоях передачи данных, падениях агентов и WAN-таймаутах.
 */
public class HdfsStagingReaper {

    private static final Logger logger = LoggerFactory.getLogger(HdfsStagingReaper.class);
    public static final Pattern STAGING_FILE_PATTERN = Pattern.compile(".*\\._staging_[a-zA-Z0-9_-]+$");

    private final Configuration conf;
    private final KerberosUgiProvider ugiProvider;
    private final String defaultFsUri;

    public static boolean isStagingFile(String fileName, String jobIdFilter) {
        if (fileName == null || !STAGING_FILE_PATTERN.matcher(fileName).matches()) {
            return false;
        }
        if (jobIdFilter != null && !jobIdFilter.isBlank()) {
            return fileName.endsWith("._staging_" + jobIdFilter.trim());
        }
        return true;
    }

    public HdfsStagingReaper(Configuration conf, KerberosUgiProvider ugiProvider, String defaultFsUri) {
        this.conf = conf != null ? conf : new Configuration();
        this.ugiProvider = ugiProvider != null ? ugiProvider : new KerberosUgiProvider(this.conf);
        this.defaultFsUri = defaultFsUri;
    }

    public int cleanStagingFiles(String baseDirStr, String jobIdFilter, long olderThanMillis,
                                 String executionPrincipal, boolean runAsServiceAccount) {
        if (baseDirStr == null || baseDirStr.isBlank()) {
            return 0;
        }
        HadoopFsManager.validatePathSafety(baseDirStr);

        boolean targetIsHdfs = baseDirStr.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(baseDirStr));
        long now = System.currentTimeMillis();
        long cutoffTime = olderThanMillis > 0 ? (now - olderThanMillis) : Long.MAX_VALUE;

        if (targetIsHdfs) {
            try {
                UserGroupInformation ugi = ugiProvider.getEffectiveUgi(executionPrincipal, runAsServiceAccount);
                return ugi.doAs((PrivilegedExceptionAction<Integer>) () -> {
                    Path p = new Path(baseDirStr);
                    FileSystem fs = p.getFileSystem(conf);
                    if (!fs.exists(p)) {
                        return 0;
                    }
                    int deleted = 0;
                    if (!fs.getFileStatus(p).isDirectory()) {
                        String name = p.getName();
                        if (isStagingFile(name, jobIdFilter)) {
                            boolean matchAge = olderThanMillis <= 0 || (fs.getFileStatus(p).getModificationTime() <= cutoffTime);
                            if (matchAge) {
                                if (isFileUnderConstruction(fs, p)) {
                                    logger.info("Пропуск staging-файла HDFS {}: файл активно дописывается клиентом (under construction)", p);
                                    return 0;
                                }
                                if (fs.delete(p, false)) deleted++;
                            }
                        }
                        return deleted;
                    }

                    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(p, true);
                    while (iter.hasNext()) {
                        LocatedFileStatus status = iter.next();
                        String name = status.getPath().getName();
                        if (isStagingFile(name, jobIdFilter)) {
                            boolean matchAge = olderThanMillis <= 0 || (status.getModificationTime() <= cutoffTime);
                            if (matchAge) {
                                if (isFileUnderConstruction(fs, status.getPath())) {
                                    logger.info("Пропуск staging-файла HDFS {}: файл активно дописывается клиентом (under construction)", status.getPath());
                                    continue;
                                }
                                try {
                                    if (fs.delete(status.getPath(), false)) {
                                        deleted++;
                                        logger.info("Удален осиротевший staging-файл HDFS: {} (модифицирован {} мс назад)",
                                                status.getPath(), (now - status.getModificationTime()));
                                    }
                                } catch (Exception e) {
                                    logger.warn("Не удалось удалить staging-файл HDFS {}: {}", status.getPath(), e.getMessage());
                                }
                            }
                        }
                    }
                    return deleted;
                });
            } catch (Exception e) {
                logger.warn("Ошибка при очистке HDFS staging-файлов в {}: {}", baseDirStr, e.getMessage());
            }
        }

        // Локальная файловая система
        File baseFile = new File(baseDirStr);
        if (!baseFile.exists()) {
            return 0;
        }

        AtomicInteger deletedLocal = new AtomicInteger(0);
        if (baseFile.isFile()) {
            String name = baseFile.getName();
            if (isStagingFile(name, jobIdFilter)) {
                boolean matchAge = olderThanMillis <= 0 || (baseFile.lastModified() <= cutoffTime);
                if (matchAge && baseFile.delete()) {
                    deletedLocal.incrementAndGet();
                }
            }
            return deletedLocal.get();
        }

        try {
            Files.walkFileTree(baseFile.toPath(), new SimpleFileVisitor<java.nio.file.Path>() {
                @Override
                public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) {
                    String name = file.getFileName().toString();
                    if (isStagingFile(name, jobIdFilter)) {
                        boolean matchAge = olderThanMillis <= 0 || (attrs.lastModifiedTime().toMillis() <= cutoffTime);
                        if (matchAge) {
                            try {
                                if (Files.deleteIfExists(file)) {
                                    deletedLocal.incrementAndGet();
                                    logger.info("Удален staging-файл в локальной ФС: {}", file);
                                }
                            } catch (IOException ignored) {}
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.warn("Ошибка при обходе локальных staging-файлов в {}: {}", baseDirStr, e.getMessage());
        }

        return deletedLocal.get();
    }

    public int cleanStagingFiles(String baseDirStr, String jobIdFilter) {
        return cleanStagingFiles(baseDirStr, jobIdFilter, 0L, null, true);
    }

    public int cleanStagingFiles(String baseDirStr, long olderThanMillis) {
        return cleanStagingFiles(baseDirStr, null, olderThanMillis, null, true);
    }

    boolean isFileUnderConstruction(FileSystem fs, Path path) {
        try {
            if (fs instanceof org.apache.hadoop.hdfs.DistributedFileSystem dfs) {
                return !dfs.isFileClosed(path);
            }
        } catch (Throwable t) {
            logger.debug("Не удалось проверить isFileClosed для {}: {}", path, t.getMessage());
        }
        return false;
    }

    private boolean isLocalPath(String path) {
        if (path == null) return true;
        if (path.startsWith("hdfs://")) return false;
        if (path.startsWith("file://")) return true;
        return path.startsWith("/") || path.matches("^[a-zA-Z]:[/\\\\].*");
    }
}
