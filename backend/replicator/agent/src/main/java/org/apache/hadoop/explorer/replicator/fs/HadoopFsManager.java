package org.apache.hadoop.explorer.replicator.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Менеджер взаимодействия с HDFS и локальной файловой системой узлов Hadoop.
 *
 * <p>Обеспечивает:
 * <ul>
 *   <li>Автоматическое подключение к HDFS с использованием системных xml-конфигов (core-site, hdfs-site).</li>
 *   <li>Аутентификацию системной техучетки по Kerberos keytab (Proxy User).</li>
 *   <li>Hadoop doAs-имперсонацию конкретного пользователя для аудита и применения политик Apache Ranger.</li>
 *   <li>Потоковое чтение и атомарный коммит файлов (rename/copy).</li>
 * </ul>
 */
public class HadoopFsManager {

    private static final Logger logger = LoggerFactory.getLogger(HadoopFsManager.class);
    private static final String DEFAULT_SERVICE_PRINCIPAL = "hdfs-replicator@REALM.LOCAL";

    private final Configuration conf;
    private final String defaultFsUri;
    private final String servicePrincipal;
    private final String keytabPath;
    private volatile boolean kerberosLoggedIn = false;

    public HadoopFsManager(String defaultFsUri, String keytabPath, String principal) {
        this.conf = new Configuration();
        this.defaultFsUri = defaultFsUri;

        if (defaultFsUri != null && !defaultFsUri.isBlank()) {
            this.conf.set("fs.defaultFS", defaultFsUri);
        }

        String envServicePrincipal = System.getenv("REPLICATOR_SERVICE_PRINCIPAL");
        this.servicePrincipal = (principal != null && !principal.isBlank()) ? principal :
                (envServicePrincipal != null && !envServicePrincipal.isBlank() ? envServicePrincipal : DEFAULT_SERVICE_PRINCIPAL);

        String envKeytab = System.getenv("REPLICATOR_KEYTAB_PATH");
        if (envKeytab == null || envKeytab.isBlank()) {
            envKeytab = System.getenv("KRB5_KEYTAB");
        }
        this.keytabPath = (keytabPath != null && !keytabPath.isBlank()) ? keytabPath : envKeytab;

        // Инициализация Kerberos аутентификации системной техучетки (Proxy User)
        initKerberos();
    }

    public HadoopFsManager() {
        this("file:///", null, null);
    }

    public HadoopFsManager(String defaultFsUri) {
        this(defaultFsUri, null, null);
    }

    private void initKerberos() {
        if (keytabPath != null && !keytabPath.isBlank()) {
            try {
                this.conf.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(servicePrincipal, keytabPath);
                this.kerberosLoggedIn = true;
                logger.info("Успешная Kerberos-аутентификация Proxy User техучетки '{}' по keytab: {}", servicePrincipal, keytabPath);
            } catch (IOException e) {
                logger.error("Ошибка Kerberos аутентификации Proxy User: {}", e.getMessage(), e);
            }
        } else {
            try {
                if (UserGroupInformation.isSecurityEnabled()) {
                    logger.info("Активен внешний Kerberos контекст UGI (например YARN delegation tokens): {}",
                            UserGroupInformation.getCurrentUser());
                    this.kerberosLoggedIn = true;
                }
            } catch (Exception ignored) {
            }
        }
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(conf);
    }

    public String getServicePrincipal() {
        return servicePrincipal;
    }

    public String getKeytabPath() {
        return keytabPath;
    }

    /**
     * Извлечение чистого имени пользователя из Kerberos Principal (например 'alice@REALM.LOCAL' -> 'alice').
     */
    public static String extractUsername(String principal) {
        if (principal == null || principal.isBlank()) {
            return "hdfs";
        }
        String p = principal.trim();
        int atIdx = p.indexOf('@');
        if (atIdx > 0) {
            p = p.substring(0, atIdx);
        }
        int slashIdx = p.indexOf('/');
        if (slashIdx > 0) {
            p = p.substring(0, slashIdx);
        }
        return p;
    }

    /**
     * Получение эффективного Kerberos UGI контекста с поддержкой doAs-имперсонации для Apache Ranger аудита.
     */
    public UserGroupInformation getEffectiveUgi(String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        if (!UserGroupInformation.isSecurityEnabled() && !kerberosLoggedIn) {
            return UserGroupInformation.getCurrentUser();
        }

        UserGroupInformation baseUgi = UserGroupInformation.getLoginUser();
        if (baseUgi == null) {
            baseUgi = UserGroupInformation.getCurrentUser();
        }

        if (runAsServiceAccount || executionPrincipal == null || executionPrincipal.isBlank()
                || executionPrincipal.equals(servicePrincipal)) {
            // Прямое выполнение от системной техучетки
            return baseUgi;
        }

        // Выполнение с doAs имперсонацией конечного пользователя
        String impersonateUser = extractUsername(executionPrincipal);
        logger.debug("Создание Hadoop Proxy User doAs контекста для '{}' (техучетка: '{}')",
                impersonateUser, baseUgi.getUserName());
        return UserGroupInformation.createProxyUser(impersonateUser, baseUgi);
    }

    /**
     * Открывает InputStream для чтения файла с учетом Kerberos контекста и doAs имперсонации.
     */
    public InputStream openInputStream(String pathStr, String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        if (isLocalPath(pathStr)) {
            File file = new File(pathStr);
            if (!file.exists()) {
                throw new FileNotFoundException("Локальный файл не найден: " + pathStr);
            }
            return new FileInputStream(file);
        }

        try {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            String doAsUser = (!runAsServiceAccount && executionPrincipal != null) ? extractUsername(executionPrincipal) : null;
            logger.info("Открытие HDFS файла '{}' (doAs: {})", pathStr, doAsUser != null ? doAsUser : "service_account");

            return ugi.doAs((PrivilegedExceptionAction<InputStream>) () -> {
                Path path = new Path(pathStr);
                FileSystem fs = path.getFileSystem(conf);
                if (!fs.exists(path)) {
                    File fallback = new File(pathStr);
                    if (fallback.exists()) {
                        return new FileInputStream(fallback);
                    }
                    throw new FileNotFoundException("Файл не найден в HDFS: " + pathStr);
                }
                return fs.open(path);
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Прервано выполнение операции открытия HDFS потока: " + e.getMessage(), e);
        } catch (Exception e) {
            if (e instanceof IOException) throw (IOException) e;
            throw new IOException("Ошибка доступа к HDFS: " + e.getMessage(), e);
        }
    }

    public InputStream openInputStream(String pathStr) throws IOException {
        return openInputStream(pathStr, null, true);
    }

    /**
     * Получает размер файла в байтах.
     */
    public long getFileSize(String pathStr, String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        if (isLocalPath(pathStr)) {
            File file = new File(pathStr);
            if (file.exists()) {
                return file.length();
            }
        }

        try {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<Long>) () -> {
                Path path = new Path(pathStr);
                FileSystem fs = path.getFileSystem(conf);
                if (fs.exists(path)) {
                    return fs.getFileStatus(path).getLen();
                }
                File fallback = new File(pathStr);
                if (fallback.exists()) {
                    return fallback.length();
                }
                return 0L;
            });
        } catch (Exception e) {
            File fallback = new File(pathStr);
            if (fallback.exists()) return fallback.length();
            return 0L;
        }
    }

    public long getFileSize(String pathStr) throws IOException {
        return getFileSize(pathStr, null, true);
    }

    /**
     * Проверяет существование файла.
     */
    public boolean exists(String pathStr) {
        try {
            if (isLocalPath(pathStr)) {
                return new File(pathStr).exists();
            }
            Path path = new Path(pathStr);
            FileSystem fs = path.getFileSystem(conf);
            return fs.exists(path) || new File(pathStr).exists();
        } catch (Exception e) {
            return new File(pathStr).exists();
        }
    }

    /**
     * Элемент файла при рекурсивном обходе директории.
     */
    public record FileItem(String relativePath, String fullPath, long size, long modificationTime) {
    }

    /**
     * Проверяет, является ли путь директорией.
     */
    public boolean isDirectory(String pathStr, String executionPrincipal, boolean runAsServiceAccount) {
        if (isLocalPath(pathStr)) {
            File file = new File(pathStr);
            return file.exists() && file.isDirectory();
        }

        try {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> {
                Path path = new Path(pathStr);
                FileSystem fs = path.getFileSystem(conf);
                if (fs.exists(path)) {
                    return fs.getFileStatus(path).isDirectory();
                }
                File fallback = new File(pathStr);
                return fallback.exists() && fallback.isDirectory();
            });
        } catch (Exception e) {
            File fallback = new File(pathStr);
            return fallback.exists() && fallback.isDirectory();
        }
    }

    public boolean isDirectory(String pathStr) {
        return isDirectory(pathStr, null, true);
    }

    /**
     * Получает время последней модификации файла в миллисекундах.
     */
    public long getFileModificationTime(String pathStr, String executionPrincipal, boolean runAsServiceAccount) {
        if (isLocalPath(pathStr)) {
            File file = new File(pathStr);
            return file.exists() ? file.lastModified() : 0L;
        }

        try {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<Long>) () -> {
                Path path = new Path(pathStr);
                FileSystem fs = path.getFileSystem(conf);
                if (fs.exists(path)) {
                    return fs.getFileStatus(path).getModificationTime();
                }
                File fallback = new File(pathStr);
                return fallback.exists() ? fallback.lastModified() : 0L;
            });
        } catch (Exception e) {
            File fallback = new File(pathStr);
            return fallback.exists() ? fallback.lastModified() : 0L;
        }
    }

    /**
     * Рекурсивно собирает все регулярные файлы внутри директории (или одиночный файл, если путь указывает на файл).
     */
    public List<FileItem> listFilesRecursively(String basePathStr, String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        if (isLocalPath(basePathStr)) {
            return listLocalFilesRecursively(basePathStr);
        }

        try {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<List<FileItem>>) () -> {
                List<FileItem> items = new ArrayList<>();
                Path basePath = new Path(basePathStr);
                FileSystem fs = basePath.getFileSystem(conf);

                if (!fs.exists(basePath)) {
                    File fallback = new File(basePathStr);
                    if (fallback.exists()) {
                        return listLocalFilesRecursively(basePathStr);
                    }
                    return items;
                }

                if (!fs.getFileStatus(basePath).isDirectory()) {
                    FileStatus st = fs.getFileStatus(basePath);
                    if (!isIgnoredFile(basePath.getName())) {
                        items.add(new FileItem(basePath.getName(), basePath.toString(), st.getLen(), st.getModificationTime()));
                    }
                    return items;
                }

                String basePrefix = basePath.toString().replaceAll("/+$", "") + "/";
                RemoteIterator<LocatedFileStatus> iter = fs.listFiles(basePath, true);
                while (iter.hasNext()) {
                    LocatedFileStatus st = iter.next();
                    String fullPath = st.getPath().toString();
                    String relPath = fullPath.startsWith(basePrefix)
                            ? fullPath.substring(basePrefix.length())
                            : st.getPath().getName();
                    if (isIgnoredPath(relPath)) {
                        logger.debug("Пропуск временного файла при сканировании HDFS: {}", relPath);
                        continue;
                    }
                    items.add(new FileItem(relPath, fullPath, st.getLen(), st.getModificationTime()));
                }
                return items;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Прервано рекурсивное сканирование HDFS каталога: " + e.getMessage(), e);
        } catch (Exception e) {
            File fallback = new File(basePathStr);
            if (fallback.exists()) {
                return listLocalFilesRecursively(basePathStr);
            }
            if (e instanceof IOException ioException) throw ioException;
            throw new IOException("Ошибка обхода каталога HDFS " + basePathStr + ": " + e.getMessage(), e);
        }
    }

    public List<FileItem> listFilesRecursively(String basePathStr) throws IOException {
        return listFilesRecursively(basePathStr, null, true);
    }

    private List<FileItem> listLocalFilesRecursively(String basePathStr) {
        List<FileItem> items = new ArrayList<>();
        File baseFile = new File(basePathStr);
        if (!baseFile.exists()) {
            return items;
        }
        if (baseFile.isFile()) {
            if (!isIgnoredFile(baseFile.getName())) {
                items.add(new FileItem(baseFile.getName(), baseFile.getAbsolutePath(), baseFile.length(), baseFile.lastModified()));
            }
            return items;
        }

        java.nio.file.Path startPath = baseFile.toPath();
        try {
            Files.walkFileTree(startPath, new SimpleFileVisitor<java.nio.file.Path>() {
                @Override
                public FileVisitResult preVisitDirectory(java.nio.file.Path dir, BasicFileAttributes attrs) {
                    if (!dir.equals(startPath)) {
                        String dirName = dir.getFileName().toString();
                        if (isIgnoredDirectory(dirName)) {
                            logger.debug("Пропуск временного каталога при обходе: {}", dir);
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) {
                    String rel = startPath.relativize(file).toString().replace(File.separatorChar, '/');
                    if (!isIgnoredPath(rel)) {
                        items.add(new FileItem(rel, file.toAbsolutePath().toString(), attrs.size(), attrs.lastModifiedTime().toMillis()));
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.warn("Ошибка рекурсивного обхода локальной директории {}: {}", basePathStr, e.getMessage());
        }
        return items;
    }

    /**
     * Нормализует путь, удаляя схему (hdfs://, file://) и ведущие/замыкающие слэши.
     */
    public static String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return "";
        }
        String clean = path.replace('\\', '/').trim();
        int protoIdx = clean.indexOf("://");
        if (protoIdx != -1) {
            clean = clean.substring(protoIdx + 3);
            int slashIdx = clean.indexOf('/');
            clean = (slashIdx != -1) ? clean.substring(slashIdx) : "";
        }
        while (clean.startsWith("/")) {
            clean = clean.substring(1);
        }
        while (clean.endsWith("/")) {
            clean = clean.substring(0, clean.length() - 1);
        }
        return clean;
    }

    /**
     * Проверяет, является ли каталог служебным или временным:
     * <ul>
     *   <li>Каталоги, начинающиеся с '.' (скрытые: .spark-staging, .staging, .tmp, .Trash, .git, .hive-staging)</li>
     *   <li>Каталоги, начинающиеся с '_' (служебные каталоги коммиттеров: _temporary, _staging, _distcp, _tmp)</li>
     *   <li>Системные каталоги HDFS/POSIX (lost+found)</li>
     *   <li>Каталоги с временными суффиксами (.tmp, .temp, .staging)</li>
     * </ul>
     */
    public static boolean isIgnoredDirectory(String dirName) {
        if (dirName == null || dirName.isBlank()) {
            return false;
        }
        String name = dirName.trim();
        if (name.equals(".") || name.equals("..")) {
            return true;
        }

        String lower = name.toLowerCase(Locale.ROOT);

        // Легитимные транзакционные каталоги Delta Lake
        if ("_delta_log".equals(lower)) {
            return false;
        }

        // Каталоги, начинающиеся с '.' (.spark-staging, .hive-staging, .staging, .tmp, .Trash, .git и т.д.)
        // или '_' (служебные каталоги коммиттеров: _temporary, _staging, _distcp, _tmp и т.д.)
        if (name.startsWith(".") || name.startsWith("_")) {
            return true;
        }

        // Вспомогательные подкаталоги внешних задач Hive MapReduce/Tez (-ext-10000, -ext-*, _ext-*)
        if (lower.startsWith("-ext-")) {
            return true;
        }

        // Каталоги staging движков Hive / Spark / Tez (независимо от настроек префикса)
        if (lower.contains("hive-staging") || lower.contains("hive_staging") ||
                lower.contains("spark-staging") || lower.contains("spark_staging") ||
                lower.contains("tez-staging") || lower.contains("tez_staging")) {
            return true;
        }

        if ("lost+found".equalsIgnoreCase(name)) {
            return true;
        }

        return lower.endsWith(".tmp") || lower.endsWith(".temp") || lower.endsWith(".staging");
    }

    /**
     * Проверяет, является ли файл временным, служебным или файлом незавершенной записи:
     * <ul>
     *   <li>Скрытые файлы (начинающиеся с '.')</li>
     *   <li>Служебные файлы ОС (Thumbs.db, desktop.ini, .DS_Store)</li>
     *   <li>Временные staging-файлы Hive, Spark, Tez (содержащие hive-staging, spark-staging)</li>
     *   <li>Временные расширения незавершенной записи (.tmp, .temp, .inprogress, .staging, .pending, .copying, .part, .partial, .swp, .swo, ~)</li>
     *   <li>Файлы промежуточного копирования (*_copying_*)</li>
     *   <li>Временные файлы коммиттеров (_temporary*, _tmp*)</li>
     *   <li>Легитимные маркеры и метаданные (_SUCCESS, _metadata, _common_metadata) НЕ отсекаются</li>
     * </ul>
     */
    public static boolean isIgnoredFile(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return false;
        }
        String name = fileName.trim();

        // Легитимные маркеры и метаданные НЕ должны отсекаться!
        if ("_SUCCESS".equalsIgnoreCase(name) ||
                "_SUCCESS.crc".equalsIgnoreCase(name) ||
                "_metadata".equalsIgnoreCase(name) ||
                "_common_metadata".equalsIgnoreCase(name)) {
            return false;
        }

        // Скрытые файлы
        if (name.startsWith(".")) {
            return true;
        }

        // Системные файлы ОС
        if ("thumbs.db".equalsIgnoreCase(name) || "desktop.ini".equalsIgnoreCase(name) || ".ds_store".equalsIgnoreCase(name)) {
            return true;
        }

        String lower = name.toLowerCase(Locale.ROOT);

        // Временные файлы staging Hive / Spark / Tez
        if (lower.contains("hive-staging") || lower.contains("hive_staging") ||
                lower.contains("spark-staging") || lower.contains("spark_staging")) {
            return true;
        }

        // Временные расширения и суффиксы незавершенной записи
        if (lower.endsWith(".tmp") ||
                lower.endsWith(".temp") ||
                lower.endsWith(".inprogress") ||
                lower.endsWith(".staging") ||
                lower.endsWith(".pending") ||
                lower.endsWith(".copying") ||
                lower.endsWith(".part") ||
                lower.endsWith(".partial") ||
                lower.endsWith(".swp") ||
                lower.endsWith(".swo") ||
                name.endsWith("~")) {
            return true;
        }

        // Промежуточные файлы утилит копирования (DistCp / FsShell)
        if (lower.contains("_copying_")) {
            return true;
        }

        // Временные файлы коммиттеров
        if (lower.startsWith("_temporary") || lower.startsWith("_tmp")) {
            return true;
        }

        // Staging-файлы Replicator (содержащие ._staging_ или _staging_)
        if (lower.contains("._staging_") || lower.contains(".staging.") || lower.contains("_staging_")) {
            return true;
        }

        return false;
    }

    /**
     * Проверяет, должен ли файл (по относительному или абсолютному пути) быть исключен из репликации:
     * проверяются все промежуточные каталоги в пути (начинающиеся с _ или .) и сам файл.
     */
    public static boolean isIgnoredPath(String relativePath) {
        if (relativePath == null || relativePath.isBlank()) {
            return false;
        }
        String clean = normalizePath(relativePath);
        if (clean.isEmpty()) {
            return false;
        }

        String[] segments = clean.split("/+");
        if (segments.length == 0) {
            return false;
        }

        for (int i = 0; i < segments.length - 1; i++) {
            if (isIgnoredDirectory(segments[i])) {
                return true;
            }
        }

        String leaf = segments[segments.length - 1];
        return isIgnoredFile(leaf);
    }

    /**
     * Проверяет, является ли путь к каталогу временным или служебным.
     */
    public static boolean isIgnoredDirectoryPath(String dirPath) {
        if (dirPath == null || dirPath.isBlank()) {
            return false;
        }
        String clean = normalizePath(dirPath);
        if (clean.isEmpty()) {
            return false;
        }

        String[] segments = clean.split("/+");
        for (String segment : segments) {
            if (isIgnoredDirectory(segment)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Создает снэпшот каталога (если включена поддержка HDFS Snapshots).
     */
    public String createSnapshot(String pathStr, String snapshotName, String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        try {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<String>) () -> {
                Path path = new Path(pathStr);
                FileSystem fs = path.getFileSystem(conf);
                if (fs instanceof DistributedFileSystem dfs) {
                    Path snapPath = dfs.createSnapshot(path, snapshotName);
                    logger.info("Успешно создан HDFS снэпшот: {} (директория: {})", snapshotName, pathStr);
                    return snapPath != null ? snapPath.toString() : snapshotName;
                }
                throw new UnsupportedOperationException("Снэпшоты не поддерживаются файловой системой: " + fs.getClass().getName());
            });
        } catch (Exception e) {
            throw new IOException("Ошибка создания HDFS снэпшота " + snapshotName + " для " + pathStr + ": " + e.getMessage(), e);
        }
    }

    /**
     * Удаляет устаревший снэпшот каталога (Snapshot Retention / Pruning).
     */
    public boolean deleteSnapshot(String pathStr, String snapshotName, String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        try {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> {
                Path path = new Path(pathStr);
                FileSystem fs = path.getFileSystem(conf);
                if (fs instanceof DistributedFileSystem dfs) {
                    dfs.deleteSnapshot(path, snapshotName);
                    logger.info("Успешно удален устаревший HDFS снэпшот: {} (директория: {})", snapshotName, pathStr);
                    return true;
                }
                return false;
            });
        } catch (Exception e) {
            throw new IOException("Ошибка удаления HDFS снэпшота " + snapshotName + " для " + pathStr + ": " + e.getMessage(), e);
        }
    }

    /**
     * Атомарный коммит принятого файла из staging в финальный целевой путь (HDFS или локальный).
     *
     * @param stagingFilePath локальный путь к принятому временному файлу
     * @param targetPath      целевой путь назначения
     * @param executionPrincipal Kerberos principal для исполнения
     * @param runAsServiceAccount признак прямого выполнения от техучетки (false = doAs имперсонация для Apache Ranger)
     * @return финальный путь
     */
    public String commitFile(String stagingFilePath, String targetPath, String executionPrincipal, boolean runAsServiceAccount) throws Exception {
        File stagingFile = new File(stagingFilePath);
        if (!stagingFile.exists()) {
            throw new FileNotFoundException("Staging файл не найден: " + stagingFilePath);
        }

        boolean targetIsHdfs = targetPath.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(targetPath));

        if (targetIsHdfs) {
            try {
                UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
                String impersonateUser = (!runAsServiceAccount && executionPrincipal != null) ? extractUsername(executionPrincipal) : null;
                logger.info("Коммит файла в HDFS: '{}' -> '{}' (doAs: {})",
                        stagingFilePath, targetPath, impersonateUser != null ? impersonateUser : "service_account");

                ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                    Path hdfsTarget = new Path(targetPath);
                    FileSystem fs = hdfsTarget.getFileSystem(conf);

                    Path parentDir = hdfsTarget.getParent();
                    if (parentDir != null && !fs.exists(parentDir)) {
                        fs.mkdirs(parentDir);
                    }

                    // Копируем из локального staging в HDFS от имени эффективного UGI (с doAs)
                    try (InputStream in = new BufferedInputStream(new FileInputStream(stagingFile));
                         OutputStream out = fs.create(hdfsTarget, true)) {
                        IOUtils.copyBytes(in, out, conf, false);
                    }
                    return null;
                });

                // Удаляем временный staging-файл после успешной записи в HDFS
                if (!stagingFile.delete()) {
                    stagingFile.deleteOnExit();
                }
                logger.info("Файл успешно закоммичен в HDFS: {} (doAs: {})",
                        targetPath, impersonateUser != null ? impersonateUser : "service_account");
                return targetPath;
            } catch (Exception e) {
                logger.warn("Не удалось записать в HDFS ({}), выполняем fallback сохранение в локальную ФС: {}", e.getMessage(), targetPath);
            }
        }
            // Локальная файловая система
            logger.info("Коммит файла в локальную ФС: '{}' -> '{}'", stagingFilePath, targetPath);
            File destFile = new File(targetPath);
            File parentDir = destFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }

            try {
                Files.move(stagingFile.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                return targetPath;
            } catch (Exception e) {
                logger.warn("Не удалось выполнить Files.move ({}), пробуем копирование: {}", e.getMessage(), targetPath);
                Files.copy(stagingFile.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                stagingFile.delete();
                return targetPath;
            }
        }


    public String commitFile(String stagingFilePath, String targetPath, String executionPrincipal) throws Exception {
        return commitFile(stagingFilePath, targetPath, executionPrincipal, false);
    }

    /**
     * Прямая запись байтов файла в HDFS или локальную ФС без создания временного staging-файла на локальном диске (Zero-Staging).
     */
    public void writeDirect(String targetPath, byte[] content, String executionPrincipal, boolean runAsServiceAccount) throws Exception {
        boolean targetIsHdfs = targetPath.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(targetPath));

        if (targetIsHdfs) {
            try {
                UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
                ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                    Path hdfsTarget = new Path(targetPath);
                    FileSystem fs = hdfsTarget.getFileSystem(conf);
                    Path parentDir = hdfsTarget.getParent();
                    if (parentDir != null && !fs.exists(parentDir)) {
                        fs.mkdirs(parentDir);
                    }
                    try (OutputStream out = fs.create(hdfsTarget, true)) {
                        out.write(content);
                    }
                    return null;
                });
                return;
            } catch (Exception e) {
                logger.warn("Не удалось записать напрямую в HDFS ({}), fallback на локальную ФС: {}", e.getMessage(), targetPath);
            }
        }

        // Локальная файловая система
        File destFile = new File(targetPath);
        File parentDir = destFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        Files.write(destFile.toPath(), content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Открытие потока вывода для записи файла напрямую в HDFS или локальную ФС.
     */
    public OutputStream openOutputStreamForWrite(String targetPath, String executionPrincipal, boolean runAsServiceAccount) throws Exception {
        boolean targetIsHdfs = targetPath.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(targetPath));

        if (targetIsHdfs) {
            try {
                UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
                return ugi.doAs((PrivilegedExceptionAction<OutputStream>) () -> {
                    Path hdfsTarget = new Path(targetPath);
                    FileSystem fs = hdfsTarget.getFileSystem(conf);
                    Path parentDir = hdfsTarget.getParent();
                    if (parentDir != null && !fs.exists(parentDir)) {
                        fs.mkdirs(parentDir);
                    }
                    return fs.create(hdfsTarget, true);
                });
            } catch (Exception e) {
                logger.warn("Не удалось открыть OutputStream в HDFS ({}), fallback на локальную ФС: {}", e.getMessage(), targetPath);
            }
        }

        File destFile = new File(targetPath);
        File parentDir = destFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        return new FileOutputStream(destFile);
    }

    /**
     * Атомарное переименование файла в целевой ФС (HDFS или локальной).
     */
    public boolean renameFile(String srcPath, String dstPath, String executionPrincipal, boolean runAsServiceAccount) throws Exception {
        boolean targetIsHdfs = dstPath.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(dstPath));

        if (targetIsHdfs) {
            try {
                UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
                return ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> {
                    Path src = new Path(srcPath);
                    Path dst = new Path(dstPath);
                    FileSystem fs = dst.getFileSystem(conf);
                    Path parent = dst.getParent();
                    if (parent != null && !fs.exists(parent)) {
                        fs.mkdirs(parent);
                    }
                    if (fs.exists(dst)) {
                        fs.delete(dst, false);
                    }
                    return fs.rename(src, dst);
                });
            } catch (Exception e) {
                logger.warn("Не удалось выполнить rename в HDFS ({}), fallback на локальную ФС: {} -> {}", e.getMessage(), srcPath, dstPath);
            }
        }

        File srcFile = new File(srcPath);
        File dstFile = new File(dstPath);
        if (dstFile.getParentFile() != null && !dstFile.getParentFile().exists()) {
            dstFile.getParentFile().mkdirs();
        }
        try {
            Files.move(srcFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (Exception e) {
            Files.copy(srcFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            srcFile.delete();
            return true;
        }
    }

    /**
     * Удаление файла или директории в HDFS или локальной ФС.
     */
    public boolean deletePath(String pathStr, String executionPrincipal, boolean runAsServiceAccount) {
        return deletePath(pathStr, true, executionPrincipal, runAsServiceAccount);
    }

    public boolean deletePath(String pathStr, boolean recursive, String executionPrincipal, boolean runAsServiceAccount) {
        boolean targetIsHdfs = pathStr.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(pathStr));
        if (targetIsHdfs) {
            try {
                UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
                return ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> {
                    Path p = new Path(pathStr);
                    FileSystem fs = p.getFileSystem(conf);
                    return fs.delete(p, recursive);
                });
            } catch (Exception e) {
                logger.debug("Не удалось удалить HDFS путь {}: {}", pathStr, e.getMessage());
            }
        }
        try {
            File f = new File(pathStr);
            if (f.exists()) {
                if (f.isDirectory() && recursive) {
                    org.apache.commons.io.FileUtils.deleteDirectory(f);
                    return true;
                }
                return f.delete();
            }
        } catch (Exception ignored) {}
        return false;
    }

    public int cleanStagingFiles(String baseDirStr, String jobIdFilter, long olderThanMillis, String executionPrincipal, boolean runAsServiceAccount) {
        if (baseDirStr == null || baseDirStr.isBlank()) {
            return 0;
        }

        boolean targetIsHdfs = baseDirStr.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(baseDirStr));
        long now = System.currentTimeMillis();
        long cutoffTime = olderThanMillis > 0 ? (now - olderThanMillis) : Long.MAX_VALUE;

        if (targetIsHdfs) {
            try {
                UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
                return ugi.doAs((PrivilegedExceptionAction<Integer>) () -> {
                    Path p = new Path(baseDirStr);
                    FileSystem fs = p.getFileSystem(conf);
                    if (!fs.exists(p)) {
                        return 0;
                    }
                    int deleted = 0;
                    if (!fs.getFileStatus(p).isDirectory()) {
                        String name = p.getName();
                        if (name.contains("._staging_") || name.contains("_staging_")) {
                            boolean matchJob = (jobIdFilter == null || jobIdFilter.isBlank()) || name.contains(jobIdFilter);
                            boolean matchAge = olderThanMillis <= 0 || (fs.getFileStatus(p).getModificationTime() <= cutoffTime);
                            if (matchJob && matchAge) {
                                if (fs.delete(p, false)) deleted++;
                            }
                        }
                        return deleted;
                    }

                    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(p, true);
                    while (iter.hasNext()) {
                        LocatedFileStatus status = iter.next();
                        String name = status.getPath().getName();
                        if (name.contains("._staging_") || name.contains("_staging_")) {
                            boolean matchJob = (jobIdFilter == null || jobIdFilter.isBlank()) || name.contains(jobIdFilter);
                            boolean matchAge = olderThanMillis <= 0 || (status.getModificationTime() <= cutoffTime);
                            if (matchJob && matchAge) {
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
            if (name.contains("._staging_") || name.contains("_staging_")) {
                boolean matchJob = (jobIdFilter == null || jobIdFilter.isBlank()) || name.contains(jobIdFilter);
                boolean matchAge = olderThanMillis <= 0 || (baseFile.lastModified() <= cutoffTime);
                if (matchJob && matchAge && baseFile.delete()) {
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
                    if (name.contains("._staging_") || name.contains("_staging_")) {
                        boolean matchJob = (jobIdFilter == null || jobIdFilter.isBlank()) || name.contains(jobIdFilter);
                        boolean matchAge = olderThanMillis <= 0 || (attrs.lastModifiedTime().toMillis() <= cutoffTime);
                        if (matchJob && matchAge) {
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

    private boolean isLocalPath(String path) {
        if (path == null) return true;
        if (path.startsWith("hdfs://")) return false;
        if (defaultFsUri != null && defaultFsUri.startsWith("hdfs://")) return false;
        return true;
    }
}
