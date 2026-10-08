package org.apache.hadoop.explorer.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Конфигурационные свойства безопасности для сервисов Hadoop Explorer.
 */
@ConfigurationProperties(prefix = "hadoop.security")
public class CommonSecurityProperties {

    private boolean debug = false;
    private JwtProperties jwt = new JwtProperties();
    private CookieProperties cookie = new CookieProperties();
    private CorsProperties cors = new CorsProperties();
    private AuthProperties auth = new AuthProperties();
    private LdapProperties ldap = new LdapProperties();
    private KerberosProperties kerberos = new KerberosProperties();
    private SessionStoreProperties sessionStore = new SessionStoreProperties();
    private RateLimiterProperties rateLimiter = new RateLimiterProperties();
    private TlsProperties tls = new TlsProperties();
    private List<MockUser> mockUsers = new ArrayList<>();

    public List<MockUser> getMockUsers() {
        return mockUsers;
    }

    public void setMockUsers(List<MockUser> mockUsers) {
        this.mockUsers = mockUsers;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public JwtProperties getJwt() {
        return jwt;
    }

    public void setJwt(JwtProperties jwt) {
        this.jwt = jwt;
    }

    public CookieProperties getCookie() {
        return cookie;
    }

    public void setCookie(CookieProperties cookie) {
        this.cookie = cookie;
    }

    public CorsProperties getCors() {
        return cors;
    }

    public void setCors(CorsProperties cors) {
        this.cors = cors;
    }

    public AuthProperties getAuth() {
        return auth;
    }

    public void setAuth(AuthProperties auth) {
        this.auth = auth;
    }

    public LdapProperties getLdap() {
        return ldap;
    }

    public void setLdap(LdapProperties ldap) {
        this.ldap = ldap;
    }

    public KerberosProperties getKerberos() {
        return kerberos;
    }

    public void setKerberos(KerberosProperties kerberos) {
        this.kerberos = kerberos;
    }

    public SessionStoreProperties getSessionStore() {
        return sessionStore;
    }

    public void setSessionStore(SessionStoreProperties sessionStore) {
        this.sessionStore = sessionStore;
    }

    public RateLimiterProperties getRateLimiter() {
        return rateLimiter;
    }

    public void setRateLimiter(RateLimiterProperties rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    public TlsProperties getTls() {
        return tls;
    }

    public void setTls(TlsProperties tls) {
        this.tls = tls;
    }

    // ==========================================
    // Nested Configuration Classes
    // ==========================================

    public static class JwtProperties {
        private String secretKey = "hadoop-explorer-default-secret-key-32-chars-minimum-safe-value";
        private String algorithm = "HS256";
        private int expirationMinutes = 480;

        public String getSecretKey() {
            return secretKey;
        }

        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }

        public String getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
        }

        public int getExpirationMinutes() {
            return expirationMinutes;
        }

        public void setExpirationMinutes(int expirationMinutes) {
            this.expirationMinutes = expirationMinutes;
        }
    }

    public static class CookieProperties {
        private List<String> names = new ArrayList<>(List.of(
            "access_token",
            "hadoop_explorer_session",
            "session_token",
            "hdfs_explorer_session"
        ));
        private String domain;
        private String path = "/";
        private boolean secure = false;
        private String sameSite = "Lax";
        private boolean httpOnly = true;

        public List<String> getNames() {
            return names;
        }

        public void setNames(List<String> names) {
            this.names = names;
        }

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public boolean isSecure() {
            return secure;
        }

        public void setSecure(boolean secure) {
            this.secure = secure;
        }

        public String getSameSite() {
            return sameSite;
        }

        public void setSameSite(String sameSite) {
            this.sameSite = sameSite;
        }

        public boolean isHttpOnly() {
            return httpOnly;
        }

        public void setHttpOnly(boolean httpOnly) {
            this.httpOnly = httpOnly;
        }
    }

    public static class CorsProperties {
        private List<String> allowedOrigins = new ArrayList<>(List.of(
            "http://localhost:3000",
            "http://localhost:5173",
            "http://localhost:8000",
            "http://localhost:8001",
            "http://localhost:8002",
            "http://localhost:8003",
            "http://localhost:8004",
            "http://localhost:8005"
        ));

        public List<String> getAllowedOrigins() {
            return allowedOrigins;
        }

        public void setAllowedOrigins(List<String> allowedOrigins) {
            this.allowedOrigins = allowedOrigins;
        }
    }

    public static class AuthProperties {
        private String mode = "mock"; // mock, ldap, kerberos
        private Set<String> adminUsers = new HashSet<>();
        private Set<String> adminGroups = new HashSet<>();
        private Set<String> writerUsers = new HashSet<>();
        private Set<String> writerGroups = new HashSet<>();

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public Set<String> getAdminUsers() {
            return adminUsers;
        }

        public void setAdminUsers(Set<String> adminUsers) {
            this.adminUsers = adminUsers;
        }

        public Set<String> getAdminGroups() {
            return adminGroups;
        }

        public void setAdminGroups(Set<String> adminGroups) {
            this.adminGroups = adminGroups;
        }

        public Set<String> getWriterUsers() {
            return writerUsers;
        }

        public void setWriterUsers(Set<String> writerUsers) {
            this.writerUsers = writerUsers;
        }

        public Set<String> getWriterGroups() {
            return writerGroups;
        }

        public void setWriterGroups(Set<String> writerGroups) {
            this.writerGroups = writerGroups;
        }
    }

    public static class LdapProperties {
        private boolean enabled = false;
        private String url = "ldap://localhost:389";
        private String baseDn = "dc=example,dc=com";
        private String userSearchBase = "ou=users";
        private String userSearchFilter = "(&(objectClass=person)(uid={0}))";
        private String groupSearchBase = "ou=groups";
        private String groupSearchFilter = "(&(objectClass=groupOfNames)(member={0}))";
        private String managerDn;
        private String managerPassword;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getBaseDn() {
            return baseDn;
        }

        public void setBaseDn(String baseDn) {
            this.baseDn = baseDn;
        }

        public String getUserSearchBase() {
            return userSearchBase;
        }

        public void setUserSearchBase(String userSearchBase) {
            this.userSearchBase = userSearchBase;
        }

        public String getUserSearchFilter() {
            return userSearchFilter;
        }

        public void setUserSearchFilter(String userSearchFilter) {
            this.userSearchFilter = userSearchFilter;
        }

        public String getGroupSearchBase() {
            return groupSearchBase;
        }

        public void setGroupSearchBase(String groupSearchBase) {
            this.groupSearchBase = groupSearchBase;
        }

        public String getGroupSearchFilter() {
            return groupSearchFilter;
        }

        public void setGroupSearchFilter(String groupSearchFilter) {
            this.groupSearchFilter = groupSearchFilter;
        }

        public String getManagerDn() {
            return managerDn;
        }

        public void setManagerDn(String managerDn) {
            this.managerDn = managerDn;
        }

        public String getManagerPassword() {
            return managerPassword;
        }

        public void setManagerPassword(String managerPassword) {
            this.managerPassword = managerPassword;
        }
    }

    public static class KerberosProperties {
        private boolean enabled = false;
        private String krb5Config = "/etc/krb5.conf";
        private String keytab = "/etc/security/keytabs/spnego.service.keytab";
        private String servicePrincipal = "HTTP/localhost@EXAMPLE.COM";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getKrb5Config() {
            return krb5Config;
        }

        public void setKrb5Config(String krb5Config) {
            this.krb5Config = krb5Config;
        }

        public String getKeytab() {
            return keytab;
        }

        public void setKeytab(String keytab) {
            this.keytab = keytab;
        }

        public String getServicePrincipal() {
            return servicePrincipal;
        }

        public void setServicePrincipal(String servicePrincipal) {
            this.servicePrincipal = servicePrincipal;
        }
    }

    public static class SessionStoreProperties {
        private String type = "memory"; // memory, jdbc, redis
        private boolean failClosed = false;
        private int l1Capacity = 10000;
        private int l1TtlSeconds = 3600;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public boolean isFailClosed() {
            return failClosed;
        }

        public void setFailClosed(boolean failClosed) {
            this.failClosed = failClosed;
        }

        public int getL1Capacity() {
            return l1Capacity;
        }

        public void setL1Capacity(int l1Capacity) {
            this.l1Capacity = l1Capacity;
        }

        public int getL1TtlSeconds() {
            return l1TtlSeconds;
        }

        public void setL1TtlSeconds(int l1TtlSeconds) {
            this.l1TtlSeconds = l1TtlSeconds;
        }
    }

    public static class RateLimiterProperties {
        private boolean enabled = true;
        private int requestsPerMinute = 600;
        private int burstCapacity = 100;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getRequestsPerMinute() {
            return requestsPerMinute;
        }

        public void setRequestsPerMinute(int requestsPerMinute) {
            this.requestsPerMinute = requestsPerMinute;
        }

        public int getBurstCapacity() {
            return burstCapacity;
        }

        public void setBurstCapacity(int burstCapacity) {
            this.burstCapacity = burstCapacity;
        }
    }

    public static class MockUser {
        private String username;
        private String password;
        private String displayName;
        private String email;
        private List<String> groups = new ArrayList<>();

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getDisplayName() { return displayName; }
        public void setDisplayName(String displayName) { this.displayName = displayName; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        public List<String> getGroups() { return groups; }
        public void setGroups(List<String> groups) { this.groups = groups; }
    }

    public static class TlsProperties {
        private boolean enabled = false;
        private String keyStorePath;
        private String keyStorePassword = "changeit";
        private String keyStoreType = "PKCS12";
        private String keyAlias;
        private String trustStorePath;
        private String trustStorePassword;
        private String trustStoreType = "PKCS12";
        private String clientAuth = "none"; // none, want, need
        private boolean insecureSkipVerify = false;
        private boolean autoGenerateSelfSigned = true;
        private List<String> enabledProtocols = new ArrayList<>(List.of("TLSv1.3", "TLSv1.2"));
        private List<String> ciphers = new ArrayList<>();

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public String getKeyStorePath() { return keyStorePath; }
        public void setKeyStorePath(String keyStorePath) { this.keyStorePath = keyStorePath; }

        public String getKeyStorePassword() { return keyStorePassword; }
        public void setKeyStorePassword(String keyStorePassword) { this.keyStorePassword = keyStorePassword; }

        public String getKeyStoreType() { return keyStoreType; }
        public void setKeyStoreType(String keyStoreType) { this.keyStoreType = keyStoreType; }

        public String getKeyAlias() { return keyAlias; }
        public void setKeyAlias(String keyAlias) { this.keyAlias = keyAlias; }

        public String getTrustStorePath() { return trustStorePath; }
        public void setTrustStorePath(String trustStorePath) { this.trustStorePath = trustStorePath; }

        public String getTrustStorePassword() { return trustStorePassword; }
        public void setTrustStorePassword(String trustStorePassword) { this.trustStorePassword = trustStorePassword; }

        public String getTrustStoreType() { return trustStoreType; }
        public void setTrustStoreType(String trustStoreType) { this.trustStoreType = trustStoreType; }

        public String getClientAuth() { return clientAuth; }
        public void setClientAuth(String clientAuth) { this.clientAuth = clientAuth; }

        public boolean isInsecureSkipVerify() { return insecureSkipVerify; }
        public void setInsecureSkipVerify(boolean insecureSkipVerify) { this.insecureSkipVerify = insecureSkipVerify; }

        public boolean isAutoGenerateSelfSigned() { return autoGenerateSelfSigned; }
        public void setAutoGenerateSelfSigned(boolean autoGenerateSelfSigned) { this.autoGenerateSelfSigned = autoGenerateSelfSigned; }

        public List<String> getEnabledProtocols() { return enabledProtocols; }
        public void setEnabledProtocols(List<String> enabledProtocols) { this.enabledProtocols = enabledProtocols; }

        public List<String> getCiphers() { return ciphers; }
        public void setCiphers(List<String> ciphers) { this.ciphers = ciphers; }
    }
}
