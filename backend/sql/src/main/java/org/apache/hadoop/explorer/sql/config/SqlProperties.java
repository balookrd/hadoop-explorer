package org.apache.hadoop.explorer.sql.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "sql")
public class SqlProperties {

    private List<ClusterConfig> clusters = new ArrayList<>();
    private AiConfig ai = new AiConfig();
    private int resultsTtlHours = 24;
    private int maxRowsPerQuery = 10000;
    private int defaultPageSize = 500;

    public List<ClusterConfig> getClusters() {
        return clusters;
    }

    public void setClusters(List<ClusterConfig> clusters) {
        this.clusters = clusters;
    }

    public AiConfig getAi() {
        return ai;
    }

    public void setAi(AiConfig ai) {
        this.ai = ai;
    }

    public int getResultsTtlHours() {
        return resultsTtlHours;
    }

    public void setResultsTtlHours(int resultsTtlHours) {
        this.resultsTtlHours = resultsTtlHours;
    }

    public int getMaxRowsPerQuery() {
        return maxRowsPerQuery;
    }

    public void setMaxRowsPerQuery(int maxRowsPerQuery) {
        this.maxRowsPerQuery = maxRowsPerQuery;
    }

    public int getDefaultPageSize() {
        return defaultPageSize;
    }

    public void setDefaultPageSize(int defaultPageSize) {
        this.defaultPageSize = defaultPageSize;
    }

    public static class ClusterConfig {
        private String id;
        private String name;
        private String type = "trino"; // trino, hive, mock
        private String host = "localhost";
        private int port = 8080;
        private String catalog = "hive";
        private String schema = "default";
        private boolean mockStorage = false;
        private ImpersonationConfig impersonation = new ImpersonationConfig();
        private List<String> allowedGroups = new ArrayList<>();
        private List<String> allowedUsers = new ArrayList<>();
        private JdbcConfig jdbc = new JdbcConfig();
        private KerberosConfig kerberos = new KerberosConfig();

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getCatalog() {
            return catalog;
        }

        public void setCatalog(String catalog) {
            this.catalog = catalog;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public boolean isMockStorage() {
            return mockStorage;
        }

        public void setMockStorage(boolean mockStorage) {
            this.mockStorage = mockStorage;
        }

        public ImpersonationConfig getImpersonation() {
            return impersonation;
        }

        public void setImpersonation(ImpersonationConfig impersonation) {
            this.impersonation = impersonation;
        }

        public List<String> getAllowedGroups() {
            return allowedGroups;
        }

        public void setAllowedGroups(List<String> allowedGroups) {
            this.allowedGroups = allowedGroups;
        }

        public List<String> getAllowedUsers() {
            return allowedUsers;
        }

        public void setAllowedUsers(List<String> allowedUsers) {
            this.allowedUsers = allowedUsers;
        }

        public JdbcConfig getJdbc() {
            return jdbc;
        }

        public void setJdbc(JdbcConfig jdbc) {
            this.jdbc = jdbc;
        }

        public KerberosConfig getKerberos() {
            return kerberos;
        }

        public void setKerberos(KerberosConfig kerberos) {
            this.kerberos = kerberos;
        }
    }

    public static class ImpersonationConfig {
        private boolean enabled = true;
        private String method = "header"; // header, doAs, session

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getMethod() {
            return method;
        }

        public void setMethod(String method) {
            this.method = method;
        }
    }

    /** Connection settings of a real engine (mock-storage: false). */
    public static class JdbcConfig {
        private boolean ssl = false;
        private String sslVerification = "FULL"; // Trino: FULL | CA | NONE
        private String sslTruststorePath;
        private String sslTruststorePassword;
        private String serviceUser;               // default: Kerberos principal short name, else "hadoop-explorer"
        private int queryTimeoutSeconds = 1800;
        private int fetchSize = 1000;
        private int connectTimeoutSeconds = 30;   // Trino: timeout (HTTP connect/read/write); HiveServer2: connectTimeout
        private int socketTimeoutSeconds = 300;   // HiveServer2 only: socketTimeout (every Thrift read), 0 = off

        public boolean isSsl() { return ssl; }
        public void setSsl(boolean ssl) { this.ssl = ssl; }
        public String getSslVerification() { return sslVerification; }
        public void setSslVerification(String sslVerification) { this.sslVerification = sslVerification; }
        public String getSslTruststorePath() { return sslTruststorePath; }
        public void setSslTruststorePath(String sslTruststorePath) { this.sslTruststorePath = sslTruststorePath; }
        public String getSslTruststorePassword() { return sslTruststorePassword; }
        public void setSslTruststorePassword(String sslTruststorePassword) { this.sslTruststorePassword = sslTruststorePassword; }
        public String getServiceUser() { return serviceUser; }
        public void setServiceUser(String serviceUser) { this.serviceUser = serviceUser; }
        public int getQueryTimeoutSeconds() { return queryTimeoutSeconds; }
        public void setQueryTimeoutSeconds(int queryTimeoutSeconds) { this.queryTimeoutSeconds = queryTimeoutSeconds; }
        public int getFetchSize() { return fetchSize; }
        public void setFetchSize(int fetchSize) { this.fetchSize = fetchSize; }
        public int getConnectTimeoutSeconds() { return connectTimeoutSeconds; }
        public void setConnectTimeoutSeconds(int connectTimeoutSeconds) { this.connectTimeoutSeconds = connectTimeoutSeconds; }
        public int getSocketTimeoutSeconds() { return socketTimeoutSeconds; }
        public void setSocketTimeoutSeconds(int socketTimeoutSeconds) { this.socketTimeoutSeconds = socketTimeoutSeconds; }
    }

    /** Keytab login of the service to the engine; end users are impersonated on top of it. */
    public static class KerberosConfig {
        private boolean enabled = false;
        private String principal;
        private String keytab;
        // null: KerberosConfigPath is not passed, the JVM setting applies (java.security.krb5.conf or the JDK default)
        private String krb5Conf;
        private String serviceName = "trino";     // Trino: KerberosRemoteServiceName
        private String serverPrincipal;           // HiveServer2: principal=..., _HOST is replaced with host

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getPrincipal() { return principal; }
        public void setPrincipal(String principal) { this.principal = principal; }
        public String getKeytab() { return keytab; }
        public void setKeytab(String keytab) { this.keytab = keytab; }
        public String getKrb5Conf() { return krb5Conf; }
        public void setKrb5Conf(String krb5Conf) { this.krb5Conf = krb5Conf; }
        public String getServiceName() { return serviceName; }
        public void setServiceName(String serviceName) { this.serviceName = serviceName; }
        public String getServerPrincipal() { return serverPrincipal; }
        public void setServerPrincipal(String serverPrincipal) { this.serverPrincipal = serverPrincipal; }
    }

    public static class AiConfig {
        private boolean enabled = true;
        private String url = "http://localhost:8000/v1";
        private String model = "qwen2.5-coder-32b";
        private String apiKey = "";
        private String provider = "mock-builtin";
        private int timeoutSeconds = 30;
        private int statusTimeoutSeconds = 5;
        private int maxConcurrentRequests = 8;

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

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public String getApiKey() {
            return apiKey;
        }

        public void setApiKey(String apiKey) {
            this.apiKey = apiKey;
        }

        public String getProvider() {
            return provider;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        public int getTimeoutSeconds() {
            return timeoutSeconds;
        }

        public void setTimeoutSeconds(int timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
        }

        public int getStatusTimeoutSeconds() {
            return statusTimeoutSeconds;
        }

        public void setStatusTimeoutSeconds(int statusTimeoutSeconds) {
            this.statusTimeoutSeconds = statusTimeoutSeconds;
        }

        public int getMaxConcurrentRequests() {
            return maxConcurrentRequests;
        }

        public void setMaxConcurrentRequests(int maxConcurrentRequests) {
            this.maxConcurrentRequests = maxConcurrentRequests;
        }
    }
}
