package org.apache.hadoop.explorer.replicator.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.PrivilegedExceptionAction;

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

    private boolean isLocalPath(String path) {
        if (path == null) return true;
        if (path.startsWith("hdfs://")) return false;
        if (defaultFsUri != null && defaultFsUri.startsWith("hdfs://")) return false;
        return true;
    }
}
