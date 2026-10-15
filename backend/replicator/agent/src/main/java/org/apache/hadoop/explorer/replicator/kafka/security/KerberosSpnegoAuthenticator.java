package org.apache.hadoop.explorer.replicator.kafka.security;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Реализация SPNEGO Negotiate аутентификатора для Schema Registry на базе GSS-API и JAAS (Krb5LoginModule).
 */
public class KerberosSpnegoAuthenticator implements SpnegoAuthenticator {

    private static final Logger log = LoggerFactory.getLogger(KerberosSpnegoAuthenticator.class);
    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";

    private final KafkaKerberosConfig config;
    private final String servicePrefix;
    private Subject subject;
    private long loginTimestamp = 0;
    private static final long TICKET_REFRESH_INTERVAL_MS = 6 * 3600 * 1000L; // 6 часов

    public KerberosSpnegoAuthenticator(KafkaKerberosConfig config) {
        this(config, (config != null && config.getSrServiceName() != null) ? config.getSrServiceName() : "HTTP");
    }

    public KerberosSpnegoAuthenticator(KafkaKerberosConfig config, String servicePrefix) {
        this.config = config;
        this.servicePrefix = (servicePrefix != null && !servicePrefix.isBlank()) ? servicePrefix : "HTTP";
    }

    @Override
    public String generateNegotiateHeader(URI targetUri) {
        if (config == null || (!config.isSrKerberosEnabled() && !config.isEnabled())) {
            return null;
        }

        String principal = config.getPrincipal();
        String keytabPath = config.getKeytabPath();
        if (principal == null || principal.isBlank() || keytabPath == null || keytabPath.isBlank()) {
            log.warn("Kerberos SPNEGO включен, но principal или keytabPath не заданы");
            return null;
        }

        File keytabFile = new File(keytabPath);
        if (!keytabFile.exists() || !keytabFile.canRead()) {
            log.warn("Keytab файл не найден или недоступен для чтения: {}", keytabPath);
            return null;
        }

        if (config.getKrb5ConfPath() != null && !config.getKrb5ConfPath().isBlank()) {
            System.setProperty("java.security.krb5.conf", config.getKrb5ConfPath());
        }

        try {
            ensureLoggedIn();
            final String host = (targetUri.getHost() != null && !targetUri.getHost().isBlank())
                    ? targetUri.getHost()
                    : "localhost";

            return Subject.doAs(subject, (PrivilegedExceptionAction<String>) () -> {
                GSSManager manager = GSSManager.getInstance();
                Oid spnegoOid = new Oid(SPNEGO_OID);
                String spn = servicePrefix + "@" + host;
                GSSName serverName = manager.createName(spn, GSSName.NT_HOSTBASED_SERVICE);
                GSSContext context = manager.createContext(
                        serverName,
                        spnegoOid,
                        null,
                        GSSContext.DEFAULT_LIFETIME
                );
                try {
                    context.requestMutualAuth(false);
                    context.requestCredDeleg(false);
                    byte[] token = context.initSecContext(new byte[0], 0, 0);
                    return "Negotiate " + Base64.getEncoder().encodeToString(token);
                } finally {
                    try {
                        context.dispose();
                    } catch (GSSException ignored) {
                    }
                }
            });
        } catch (PrivilegedActionException pae) {
            log.error("Ошибка формирования SPNEGO билета для {}: {}", targetUri, pae.getException().getMessage());
            return null;
        } catch (Exception e) {
            log.error("Ошибка аутентификации Kerberos SPNEGO для {}: {}", targetUri, e.getMessage());
            return null;
        }
    }

    private synchronized void ensureLoggedIn() throws LoginException {
        long now = System.currentTimeMillis();
        if (subject != null && (now - loginTimestamp) < TICKET_REFRESH_INTERVAL_MS) {
            return;
        }

        Configuration jaasConfig = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                Map<String, Object> options = new HashMap<>();
                options.put("useKeyTab", "true");
                options.put("keyTab", config.getKeytabPath());
                options.put("principal", config.getPrincipal());
                options.put("storeKey", "true");
                options.put("doNotPrompt", "true");
                options.put("isInitiator", "true");
                options.put("refreshKrb5Config", "true");
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(
                                "com.sun.security.auth.module.Krb5LoginModule",
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options
                        )
                };
            }
        };

        LoginContext lc = new LoginContext("KafkaReplicatorSpnegoClient", null, null, jaasConfig);
        lc.login();
        this.subject = lc.getSubject();
        this.loginTimestamp = now;
        log.info("Успешный вход JAAS для Kerberos SPNEGO: principal={}", config.getPrincipal());
    }
}
