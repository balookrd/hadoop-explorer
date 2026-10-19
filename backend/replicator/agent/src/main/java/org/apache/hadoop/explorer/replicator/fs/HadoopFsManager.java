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
import org.apache.hadoop.explorer.replicator.security.KerberosUgiProvider;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
    private final String localSandboxDir;
    private final KerberosUgiProvider ugiProvider;
    private final HdfsStagingReaper stagingReaper;
    private volatile boolean kerberosLoggedIn = false;

    /**
     * Потокобезопасный Bounded LRU-кэш проверенных родительских директорий (до 5000 путей)
     * для исключения избыточных вызовов fs.exists/mkdirs к NameNode (PERF-04).
     */
    private final Set<String> verifiedDirectoriesGlobalCache = Collections.synchronizedSet(
            Collections.newSetFromMap(
                    new LinkedHashMap<String, Boolean>(5000, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                            return size() > 5000;
                        }
                    }
            )
    );

    public HadoopFsManager(String defaultFsUri, String keytabPath, String principal, String localSandboxDir) {
        this.conf = new Configuration();
        this.defaultFsUri = defaultFsUri;
        this.localSandboxDir = (localSandboxDir != null && !localSandboxDir.isBlank()) ? localSandboxDir : System.getenv("REPLICATOR_LOCAL_SANDBOX");

        if (defaultFsUri != null && !defaultFsUri.isBlank()) {
            this.conf.set("fs.defaultFS", defaultFsUri);
        }

        String dataTransferProtection = System.getenv().getOrDefault("HDFS_DATA_TRANSFER_PROTECTION", "integrity");
        if (dataTransferProtection != null && !dataTransferProtection.isBlank()) {
            this.conf.set("dfs.data.transfer.protection", dataTransferProtection);
        }

        String envServicePrincipal = System.getenv("REPLICATOR_SERVICE_PRINCIPAL");
        this.servicePrincipal = (principal != null && !principal.isBlank()) ? principal :
                (envServicePrincipal != null && !envServicePrincipal.isBlank() ? envServicePrincipal : DEFAULT_SERVICE_PRINCIPAL);

        String envKeytab = System.getenv("REPLICATOR_KEYTAB_PATH");
        if (envKeytab == null || envKeytab.isBlank()) {
            envKeytab = System.getenv("KRB5_KEYTAB");
        }
        this.keytabPath = (keytabPath != null && !keytabPath.isBlank()) ? keytabPath : envKeytab;

        // Делегирование инициализации Kerberos и UGI провайдеру
        this.ugiProvider = new KerberosUgiProvider(this.conf, this.servicePrincipal, this.keytabPath);
        this.kerberosLoggedIn = this.ugiProvider.isKerberosLoggedIn();
        this.stagingReaper = new HdfsStagingReaper(this.conf, this.ugiProvider, this.defaultFsUri);
    }

    public HadoopFsManager(String defaultFsUri, String keytabPath, String principal) {
        this(defaultFsUri, keytabPath, principal, null);
    }

    public HadoopFsManager() {
        this("file:///", null, null, null);
    }

    public HadoopFsManager(String defaultFsUri) {
        this(defaultFsUri, null, null, null);
    }

    public KerberosUgiProvider getUgiProvider() {
        return ugiProvider;
    }

    public HdfsStagingReaper getStagingReaper() {
        return stagingReaper;
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
        return KerberosUgiProvider.extractUsername(principal);
    }

    /**
     * Валидация безопасности принципала имперсонации: запрет doAs под системными учетками ОС и инъекций.
     */
    public static void validatePrincipalSafety(String principal) {
        KerberosUgiProvider.validatePrincipalSafety(principal);
    }

    /**
     * Получение эффективного Kerberos UGI контекста с поддержкой doAs-имперсонации для Apache Ranger аудита.
     */
    public UserGroupInformation getEffectiveUgi(String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        return ugiProvider.getEffectiveUgi(executionPrincipal, runAsServiceAccount);
    }

    public String getLocalSandboxDir() {
        return localSandboxDir;
    }

    private static final Set<String> FORBIDDEN_OS_PREFIXES = Set.of(
            "/bin", "/sbin", "/usr", "/lib", "/lib64", "/opt", "/boot",
            "/etc", "/proc", "/sys", "/dev", "/root",
            "/System", "/Library", "/Applications",
            "/var/run", "/var/spool", "/var/log", "/var/lib", "/var/mail", "/var/root", "/var/db", "/var/cron"
    );

    private static final List<String> SENSITIVE_PATH_PATTERNS = List.of(
            "/.ssh", "/.gnupg", "/.bashrc", "/.zshrc", "/.profile",
            "/.bash_profile", "/.bash_history", "/.zsh_history", "/.env", "/crontab"
    );

    /**
     * Валидация безопасности пути для предотвращения Path Traversal и доступа к системным файлам ОС.
     */
    public static void validatePathSafety(String pathStr) {
        if (pathStr == null || pathStr.isBlank()) {
            throw new IllegalArgumentException("Путь не может быть пустым");
        }
        if (pathStr.indexOf('\0') >= 0) {
            throw new SecurityException("Попытка инъекции нулевого байта (Null Byte Injection) в пути: " + pathStr);
        }
        String normalized = pathStr.replace('\\', '/').trim();
        if (normalized.contains("..")) {
            throw new SecurityException("Попытка Path Traversal ('..') в пути: " + pathStr);
        }
        String pathOnly = normalized;
        if (normalized.contains("://")) {
            try {
                java.net.URI uri = java.net.URI.create(normalized);
                if (uri.getPath() != null && !uri.getPath().isEmpty()) {
                    pathOnly = uri.getPath();
                }
            } catch (Exception ignored) {
                int schemeIdx = normalized.indexOf("://");
                int slashIdx = normalized.indexOf('/', schemeIdx + 3);
                if (slashIdx >= 0) {
                    pathOnly = normalized.substring(slashIdx);
                }
            }
        }
        for (String forbidden : FORBIDDEN_OS_PREFIXES) {
            if (pathOnly.equalsIgnoreCase(forbidden) || pathOnly.toLowerCase().startsWith(forbidden + "/")) {
                throw new SecurityException("Доступ к системным путям ОС запрещен политикой безопасности: " + pathStr);
            }
        }
        for (String sensitive : SENSITIVE_PATH_PATTERNS) {
            String lower = pathOnly.toLowerCase();
            if (lower.equals(sensitive) || lower.endsWith(sensitive) || lower.contains(sensitive + "/")) {
                throw new SecurityException("Доступ к конфиденциальным файлам и каталогам запрещен политикой безопасности: " + pathStr);
            }
        }
    }

    /**
     * Валидация доступа к пути с учетом Chroot-песочницы (для локальных путей) и базовых правил безопасности.
     */
    public void validateSandboxAccess(String pathStr) {
        validateSandboxAccess(pathStr, this.localSandboxDir, isLocalPath(pathStr));
    }

    public static void validateSandboxAccess(String pathStr, String sandboxDir, boolean isLocal) {
        validatePathSafety(pathStr);
        if (isLocal && sandboxDir != null && !sandboxDir.isBlank()) {
            try {
                File sandboxRoot = new File(sandboxDir).getCanonicalFile();
                String rawPath = pathStr;
                if (rawPath.startsWith("file://")) {
                    try {
                        rawPath = java.net.URI.create(rawPath).getPath();
                    } catch (Exception ignored) {
                        rawPath = rawPath.substring(7);
                    }
                }
                File target = new File(rawPath).getCanonicalFile();
                java.nio.file.Path targetNorm = target.toPath().normalize();
                java.nio.file.Path rootNorm = sandboxRoot.toPath().normalize();
                if (!targetNorm.startsWith(rootNorm)) {
                    throw new SecurityException("Нарушение Chroot-песочницы: целевой локальный путь '" + pathStr +
                            "' выходит за пределы разрешенной директории '" + rootNorm + "'");
                }
            } catch (SecurityException se) {
                throw se;
            } catch (Exception e) {
                throw new SecurityException("Ошибка проверки границ Chroot-песочницы для пути '" + pathStr + "': " + e.getMessage(), e);
            }
        }
    }

    /**
     * Открывает InputStream для чтения файла с учетом Kerberos контекста и doAs имперсонации.
     */
    public InputStream openInputStream(String pathStr, String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        validateSandboxAccess(pathStr);
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
        validateSandboxAccess(pathStr);
        if (isLocalPath(pathStr)) {
            File file = new File(pathStr);
            if (file.exists()) {
                return file.length();
            }
            return 0L;
        }

        try {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<Long>) () -> {
                Path path = new Path(pathStr);
                FileSystem fs = path.getFileSystem(conf);
                if (fs.exists(path)) {
                    return fs.getFileStatus(path).getLen();
                }
                return 0L;
            });
        } catch (Exception e) {
            return 0L;
        }
    }

    public long getFileSize(String pathStr) throws IOException {
        return getFileSize(pathStr, null, true);
    }

    /**
     * Проверяет существование файла или каталога с учетом Kerberos UGI.
     */
    public boolean exists(String pathStr, String executionPrincipal, boolean runAsServiceAccount) {
        validateSandboxAccess(pathStr);
        if (isLocalPath(pathStr)) {
            return new File(pathStr).exists();
        }
        try {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> {
                Path path = new Path(pathStr);
                FileSystem fs = path.getFileSystem(conf);
                return fs.exists(path);
            });
        } catch (Exception e) {
            return false;
        }
    }

    public boolean exists(String pathStr) {
        return exists(pathStr, null, true);
    }

    /**
     * Элемент файла при рекурсивном обходе директории.
     */
    public record FileItem(String relativePath, String fullPath, long size, long modificationTime) {
    }

    public boolean isDirectory(String pathStr, String executionPrincipal, boolean runAsServiceAccount) {
        validateSandboxAccess(pathStr);
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
                return false;
            });
        } catch (Exception e) {
            return false;
        }
    }

    public boolean isDirectory(String pathStr) {
        return isDirectory(pathStr, null, true);
    }

    /**
     * Получает время последней модификации файла в миллисекундах.
     */
    public long getFileModificationTime(String pathStr, String executionPrincipal, boolean runAsServiceAccount) {
        validateSandboxAccess(pathStr);
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
                return 0L;
            });
        } catch (Exception e) {
            return 0L;
        }
    }

    /**
     * Рекурсивно собирает все регулярные файлы внутри директории (или одиночный файл, если путь указывает на файл).
     */
    public List<FileItem> listFilesRecursively(String basePathStr, String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        validateSandboxAccess(basePathStr);
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

        // Staging-файлы Replicator (строгий суффикс ._staging_<id>)
        if (lower.contains("._staging_")) {
            int idx = lower.lastIndexOf("._staging_");
            String after = lower.substring(idx + "._staging_".length());
            if (!after.isEmpty() && after.matches("^[a-zA-Z0-9_-]+$")) {
                return true;
            }
        }
        if (lower.contains(".staging.") && (lower.startsWith(".") || lower.contains("/."))) {
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
        validateSandboxAccess(targetPath);
        validatePathSafety(stagingFilePath);

        File stagingFile = new File(stagingFilePath);
        if (!stagingFile.exists()) {
            throw new FileNotFoundException("Staging файл не найден: " + stagingFilePath);
        }

        boolean targetIsHdfs = targetPath.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(targetPath));

        if (targetIsHdfs) {
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
        }

        // Локальная файловая система (только если целевой путь явно локальный)
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
     * Обеспечивает существование родительской директории с использованием двухуровневого кэша:
     * локального для бандла/сессии (sessionVerifiedDirs) и глобального LRU-кэша (verifiedDirectoriesGlobalCache).
     * Устраняет до 99% синхронных RPC-вызовов fs.exists/mkdirs к NameNode (PERF-04).
     */
    public void ensureParentDirectoryExists(Path parentDir, FileSystem fs, Set<String> sessionVerifiedDirs) throws IOException {
        if (parentDir == null) return;
        String pathStr = parentDir.toString();
        if (sessionVerifiedDirs != null && sessionVerifiedDirs.contains(pathStr)) {
            return;
        }
        if (verifiedDirectoriesGlobalCache.contains(pathStr)) {
            if (sessionVerifiedDirs != null) {
                sessionVerifiedDirs.add(pathStr);
            }
            return;
        }
        if (!fs.exists(parentDir)) {
            fs.mkdirs(parentDir);
        }
        verifiedDirectoriesGlobalCache.add(pathStr);
        if (sessionVerifiedDirs != null) {
            sessionVerifiedDirs.add(pathStr);
        }
    }

    public void ensureParentDirectoryExists(File parentDir, Set<String> sessionVerifiedDirs) {
        if (parentDir == null) return;
        String pathStr = parentDir.getAbsolutePath();
        if (sessionVerifiedDirs != null && sessionVerifiedDirs.contains(pathStr)) {
            return;
        }
        if (verifiedDirectoriesGlobalCache.contains(pathStr)) {
            if (sessionVerifiedDirs != null) {
                sessionVerifiedDirs.add(pathStr);
            }
            return;
        }
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }
        verifiedDirectoriesGlobalCache.add(pathStr);
        if (sessionVerifiedDirs != null) {
            sessionVerifiedDirs.add(pathStr);
        }
    }

    public void clearVerifiedDirectoriesCache() {
        verifiedDirectoriesGlobalCache.clear();
    }

    /**
     * Прямая запись байтов файла в HDFS или локальную ФС без создания временного staging-файла на локальном диске (Zero-Staging)
     * с поддержкой сессионного кэширования родительских каталогов (PERF-04).
     */
    public void writeDirect(String targetPath, byte[] content, String executionPrincipal, boolean runAsServiceAccount, Set<String> sessionVerifiedDirs) throws Exception {
        validateSandboxAccess(targetPath);
        boolean targetIsHdfs = targetPath.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(targetPath));

        if (targetIsHdfs) {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                Path hdfsTarget = new Path(targetPath);
                FileSystem fs = hdfsTarget.getFileSystem(conf);
                ensureParentDirectoryExists(hdfsTarget.getParent(), fs, sessionVerifiedDirs);
                try (OutputStream out = fs.create(hdfsTarget, true)) {
                    out.write(content);
                }
                return null;
            });
            return;
        }

        // Локальная файловая система (только если путь явно локальный)
        File destFile = new File(targetPath);
        ensureParentDirectoryExists(destFile.getParentFile(), sessionVerifiedDirs);
        Files.write(destFile.toPath(), content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public void writeDirect(String targetPath, byte[] content, String executionPrincipal, boolean runAsServiceAccount) throws Exception {
        writeDirect(targetPath, content, executionPrincipal, runAsServiceAccount, null);
    }

    /**
     * Открытие потока вывода для записи файла напрямую в HDFS или локальную ФС с кэшированием директорий.
     */
    public OutputStream openOutputStreamForWrite(String targetPath, String executionPrincipal, boolean runAsServiceAccount, Set<String> sessionVerifiedDirs) throws Exception {
        validateSandboxAccess(targetPath);
        boolean targetIsHdfs = targetPath.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(targetPath));

        if (targetIsHdfs) {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<OutputStream>) () -> {
                Path hdfsTarget = new Path(targetPath);
                FileSystem fs = hdfsTarget.getFileSystem(conf);
                ensureParentDirectoryExists(hdfsTarget.getParent(), fs, sessionVerifiedDirs);
                return fs.create(hdfsTarget, true);
            });
        }

        File destFile = new File(targetPath);
        ensureParentDirectoryExists(destFile.getParentFile(), sessionVerifiedDirs);
        return new FileOutputStream(destFile);
    }

    public OutputStream openOutputStreamForWrite(String targetPath, String executionPrincipal, boolean runAsServiceAccount) throws Exception {
        return openOutputStreamForWrite(targetPath, executionPrincipal, runAsServiceAccount, null);
    }

    /**
     * Атомарное переименование файла в целевой ФС (HDFS или локальной).
     */
    public boolean renameFile(String srcPath, String dstPath, String executionPrincipal, boolean runAsServiceAccount) throws Exception {
        validateSandboxAccess(srcPath);
        validateSandboxAccess(dstPath);
        boolean targetIsHdfs = dstPath.startsWith("hdfs://") || (defaultFsUri != null && !isLocalPath(dstPath));

        if (targetIsHdfs) {
            UserGroupInformation ugi = getEffectiveUgi(executionPrincipal, runAsServiceAccount);
            return ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> {
                Path src = new Path(srcPath);
                Path dst = new Path(dstPath);
                FileSystem fs = dst.getFileSystem(conf);
                ensureParentDirectoryExists(dst.getParent(), fs, null);
                if (fs.exists(dst)) {
                    fs.delete(dst, false);
                }
                return fs.rename(src, dst);
            });
        }

        File srcFile = new File(srcPath);
        File dstFile = new File(dstPath);
        ensureParentDirectoryExists(dstFile.getParentFile(), null);

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
        validateSandboxAccess(pathStr);
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
                return false;
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
        return stagingReaper.cleanStagingFiles(baseDirStr, jobIdFilter, olderThanMillis, executionPrincipal, runAsServiceAccount);
    }

    public int cleanStagingFiles(String baseDirStr, String jobIdFilter) {
        return stagingReaper.cleanStagingFiles(baseDirStr, jobIdFilter);
    }

    public int cleanStagingFiles(String baseDirStr, long olderThanMillis) {
        return stagingReaper.cleanStagingFiles(baseDirStr, olderThanMillis);
    }

    public boolean isLocalPath(String path) {
        if (path == null) return true;
        if (path.startsWith("hdfs://")) return false;
        if (defaultFsUri != null && defaultFsUri.startsWith("hdfs://")) return false;
        return true;
    }
}
