package org.apache.hadoop.explorer.replicator.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Провайдер управления контекстами безопасности Kerberos UGI и doAs-имперсонации.
 * Обеспечивает безопасное взаимодействие с Apache Ranger, Kerberos KDC и аудит доступов.
 */
public class KerberosUgiProvider {

    private static final Logger logger = LoggerFactory.getLogger(KerberosUgiProvider.class);

    public static final String DEFAULT_SERVICE_PRINCIPAL = "hdfs-replicator@REALM.LOCAL";

    public static final Set<String> FORBIDDEN_PRINCIPALS = Set.of(
            "root", "daemon", "bin", "sys", "sync", "games", "man", "lp", "mail", "news",
            "uucp", "proxy", "adm", "shutdown", "halt", "operator", "nobody", "0"
    );

    public static final Pattern SAFE_PRINCIPAL_PATTERN =
            Pattern.compile("^[a-zA-Z0-9_.-]+(/[a-zA-Z0-9_.-]+)?(@[a-zA-Z0-9_.-]+)?$");

    private final String servicePrincipal;
    private final String keytabPath;
    private final Configuration conf;

    /** Кэш proxy-UGI по имени имперсонируемого пользователя для текущей техучетки. */
    private final java.util.concurrent.ConcurrentMap<String, UserGroupInformation> proxyUgiCache =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final Object proxyUgiLock = new Object();
    private volatile UserGroupInformation proxyUgiBase;
    private volatile boolean kerberosLoggedIn = false;

    public KerberosUgiProvider(Configuration conf, String servicePrincipal, String keytabPath) {
        this.conf = conf != null ? conf : new Configuration();

        String envServicePrincipal = System.getenv("REPLICATOR_SERVICE_PRINCIPAL");
        this.servicePrincipal = (servicePrincipal != null && !servicePrincipal.isBlank()) ? servicePrincipal :
                (envServicePrincipal != null && !envServicePrincipal.isBlank() ? envServicePrincipal : DEFAULT_SERVICE_PRINCIPAL);

        String envKeytab = System.getenv("REPLICATOR_KEYTAB_PATH");
        if (envKeytab == null || envKeytab.isBlank()) {
            envKeytab = System.getenv("KRB5_KEYTAB");
        }
        this.keytabPath = (keytabPath != null && !keytabPath.isBlank()) ? keytabPath : envKeytab;

        initKerberos();
    }

    public KerberosUgiProvider(Configuration conf) {
        this(conf, null, null);
    }

    public KerberosUgiProvider() {
        this(new Configuration(), null, null);
    }

    private void initKerberos() {
        if (keytabPath != null && !keytabPath.isBlank()) {
            try {
                this.conf.set("hadoop.security.authentication", "kerberos");
                this.conf.set("dfs.namenode.kerberos.principal.pattern", "*");
                String nnPrincipal = System.getenv().getOrDefault("HDFS_NAMENODE_PRINCIPAL", "nn/*@COMPANY.LOCAL");
                this.conf.set("dfs.namenode.kerberos.principal", nnPrincipal);
                this.conf.set("dfs.datanode.kerberos.principal", "dn/*@COMPANY.LOCAL");
                String dataTransferProtection = System.getenv().getOrDefault("HDFS_DATA_TRANSFER_PROTECTION", "integrity");
                this.conf.set("dfs.data.transfer.protection", dataTransferProtection);
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(servicePrincipal, keytabPath);
                this.kerberosLoggedIn = true;
                logger.info("Успешная Kerberos-аутентификация Proxy User техучетки '{}' по keytab: {} (qop: {})",
                        servicePrincipal, keytabPath, dataTransferProtection);
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
     * Валидация безопасности принципала имперсонации: запрет doAs под системными учетками ОС и инъекций.
     */
    public static void validatePrincipalSafety(String principal) {
        if (principal == null || principal.isBlank()) {
            return;
        }
        String trimmed = principal.trim();
        if (trimmed.contains("..") || trimmed.contains("\0") || !SAFE_PRINCIPAL_PATTERN.matcher(trimmed).matches()) {
            throw new SecurityException("Попытка инъекции или недопустимые символы в имени принципала: " + principal);
        }
        String username = extractUsername(trimmed).toLowerCase(Locale.ROOT);
        if (FORBIDDEN_PRINCIPALS.contains(username)) {
            throw new SecurityException("Попытка имперсонации запрещенной системной учетной записи: " + principal);
        }
    }

    /**
     * Получение эффективного Kerberos UGI контекста с поддержкой doAs-имперсонации для Apache Ranger аудита.
     */
    public UserGroupInformation getEffectiveUgi(String executionPrincipal, boolean runAsServiceAccount) throws IOException {
        validatePrincipalSafety(executionPrincipal);
        if (!UserGroupInformation.isSecurityEnabled() && !kerberosLoggedIn) {
            try {
                return UserGroupInformation.getCurrentUser();
            } catch (Throwable t) {
                try {
                    return UserGroupInformation.getLoginUser();
                } catch (Throwable t2) {
                    return UserGroupInformation.createRemoteUser(executionPrincipal != null ? extractUsername(executionPrincipal) : "hdfs");
                }
            }
        }

        UserGroupInformation baseUgi;
        try {
            baseUgi = UserGroupInformation.getLoginUser();
        } catch (Throwable t) {
            baseUgi = null;
        }
        if (baseUgi == null) {
            try {
                baseUgi = UserGroupInformation.getCurrentUser();
            } catch (Throwable t) {
                baseUgi = UserGroupInformation.createRemoteUser(servicePrincipal != null ? extractUsername(servicePrincipal) : "hdfs");
            }
        }

        if (runAsServiceAccount || executionPrincipal == null || executionPrincipal.isBlank()
                || executionPrincipal.equals(servicePrincipal)) {
            // Прямое выполнение от системной техучетки
            return baseUgi;
        }

        // Выполнение с doAs имперсонацией конечного пользователя
        String impersonateUser = extractUsername(executionPrincipal);
        return proxyUserFor(impersonateUser, baseUgi);
    }

    /**
     * Proxy-UGI для имперсонации пользователя поверх системной техучетки.
     *
     * <p>UGI кэшируется по паре (пользователь, техучетка): {@code FileSystem.Cache} ключуется
     * по UGI, а {@code UGI.equals} сравнивает {@code Subject} по ссылке, поэтому новый proxy-UGI
     * на каждую операцию навсегда оставлял в кэше новый {@code DistributedFileSystem} с
     * собственными сокетами и lease-renewer-потоком. Кэш сбрасывается при смене техучетки
     * (перелогин по keytab), чтобы не удерживать устаревшие билеты.
     */
    UserGroupInformation proxyUserFor(String impersonateUser, UserGroupInformation baseUgi) {
        UserGroupInformation cachedBase = proxyUgiBase;
        if (cachedBase != baseUgi) {
            synchronized (proxyUgiLock) {
                if (proxyUgiBase != baseUgi) {
                    proxyUgiCache.clear();
                    proxyUgiBase = baseUgi;
                }
            }
        }
        return proxyUgiCache.computeIfAbsent(impersonateUser, user -> {
            logger.debug("Создание Hadoop Proxy User doAs контекста для '{}' (техучетка: '{}')",
                    user, baseUgi.getUserName());
            return UserGroupInformation.createProxyUser(user, baseUgi);
        });
    }

    public String getServicePrincipal() {
        return servicePrincipal;
    }

    public String getKeytabPath() {
        return keytabPath;
    }

    public boolean isKerberosLoggedIn() {
        return kerberosLoggedIn;
    }
}
