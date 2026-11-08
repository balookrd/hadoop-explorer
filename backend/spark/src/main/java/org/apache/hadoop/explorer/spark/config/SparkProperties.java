package org.apache.hadoop.explorer.spark.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "spark")
public class SparkProperties {

    private List<SparkClusterConfig> clusters = new ArrayList<>();
    private int sessionTtlMinutes = 120;
    private int resultsTtlHours = 24;
    // POST /api/v1/auth/logout deletes all Livy sessions of the user recorded in spark_sessions (every browser)
    private boolean closeSessionsOnLogout = true;
    // Background sync of session state from Livy: delay between runs and the pause of a cluster after 503/504
    private long sessionSyncIntervalMs = 5000;
    private int sessionSyncPauseSeconds = 30;
    // spark_conf keys a user may not set: they would change the master, the queue or the credentials, or make
    // spark-submit resolve Maven dependencies on the Livy host. Matched ignoring case. LivySessionPayloadBuilder
    // denies credential, master, Ivy and local-file keys on top of this list.
    private List<String> deniedSparkConfPrefixes = new ArrayList<>(List.of(
            "spark.master", "spark.submit.deployMode", "spark.yarn.queue",
            "spark.yarn.keytab", "spark.yarn.principal", "spark.kerberos.", "spark.jars.packages"));

    // Schemes accepted in user file lists (spark.files, spark.jars, spark.submit.pyFiles, spark.archives, jars,
    // py_files, custom_python_archive); anything else, local paths included, points to the Livy host
    private List<String> allowedRemoteSchemes = new ArrayList<>(List.of("hdfs", "viewfs", "s3a", "abfs", "abfss"));

    public List<String> getAllowedRemoteSchemes() {
        return allowedRemoteSchemes;
    }

    public void setAllowedRemoteSchemes(List<String> allowedRemoteSchemes) {
        this.allowedRemoteSchemes = allowedRemoteSchemes;
    }

    public List<String> getDeniedSparkConfPrefixes() {
        return deniedSparkConfPrefixes;
    }

    public void setDeniedSparkConfPrefixes(List<String> deniedSparkConfPrefixes) {
        this.deniedSparkConfPrefixes = deniedSparkConfPrefixes;
    }

    public List<SparkClusterConfig> getClusters() {
        return clusters;
    }

    public void setClusters(List<SparkClusterConfig> clusters) {
        this.clusters = clusters;
    }

    public int getSessionTtlMinutes() {
        return sessionTtlMinutes;
    }

    public void setSessionTtlMinutes(int sessionTtlMinutes) {
        this.sessionTtlMinutes = sessionTtlMinutes;
    }

    public int getResultsTtlHours() {
        return resultsTtlHours;
    }

    public void setResultsTtlHours(int resultsTtlHours) {
        this.resultsTtlHours = resultsTtlHours;
    }

    public boolean isCloseSessionsOnLogout() {
        return closeSessionsOnLogout;
    }

    public void setCloseSessionsOnLogout(boolean closeSessionsOnLogout) {
        this.closeSessionsOnLogout = closeSessionsOnLogout;
    }

    public long getSessionSyncIntervalMs() {
        return sessionSyncIntervalMs;
    }

    public void setSessionSyncIntervalMs(long sessionSyncIntervalMs) {
        this.sessionSyncIntervalMs = sessionSyncIntervalMs;
    }

    public int getSessionSyncPauseSeconds() {
        return sessionSyncPauseSeconds;
    }

    public void setSessionSyncPauseSeconds(int sessionSyncPauseSeconds) {
        this.sessionSyncPauseSeconds = sessionSyncPauseSeconds;
    }

    public static class SparkClusterConfig {
        private String id;
        private String name;
        private String description = "";
        private String type = "livy";
        private String livyUrl;
        private String yarnClusterId;
        private boolean mockStorage;
        private List<SparkVersionConfig> sparkVersions = new ArrayList<>();
        private List<MetastoreConfig> metastores = new ArrayList<>();
        private List<String> yarnQueues = new ArrayList<>();
        private String defaultQueue;
        private List<ResourceProfileConfig> resourceProfiles = new ArrayList<>();
        private List<String> defaultRepositories = new ArrayList<>();
        private List<String> allowedUsers = new ArrayList<>();
        private List<String> allowedGroups = new ArrayList<>();
        private LivyConfig livy = new LivyConfig();

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

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getLivyUrl() {
            return livyUrl;
        }

        public void setLivyUrl(String livyUrl) {
            this.livyUrl = livyUrl;
        }

        public String getYarnClusterId() {
            return yarnClusterId;
        }

        public void setYarnClusterId(String yarnClusterId) {
            this.yarnClusterId = yarnClusterId;
        }

        public boolean isMockStorage() {
            return mockStorage;
        }

        public void setMockStorage(boolean mockStorage) {
            this.mockStorage = mockStorage;
        }

        public List<SparkVersionConfig> getSparkVersions() {
            return sparkVersions;
        }

        public void setSparkVersions(List<SparkVersionConfig> sparkVersions) {
            this.sparkVersions = sparkVersions;
        }

        public List<MetastoreConfig> getMetastores() {
            return metastores;
        }

        public void setMetastores(List<MetastoreConfig> metastores) {
            this.metastores = metastores;
        }

        public List<String> getYarnQueues() {
            return yarnQueues;
        }

        public void setYarnQueues(List<String> yarnQueues) {
            this.yarnQueues = yarnQueues;
        }

        public String getDefaultQueue() {
            return defaultQueue;
        }

        public void setDefaultQueue(String defaultQueue) {
            this.defaultQueue = defaultQueue;
        }

        public List<ResourceProfileConfig> getResourceProfiles() {
            return resourceProfiles;
        }

        public void setResourceProfiles(List<ResourceProfileConfig> resourceProfiles) {
            this.resourceProfiles = resourceProfiles;
        }

        public List<String> getDefaultRepositories() {
            return defaultRepositories;
        }

        public void setDefaultRepositories(List<String> defaultRepositories) {
            this.defaultRepositories = defaultRepositories;
        }

        public List<String> getAllowedUsers() {
            return allowedUsers;
        }

        public void setAllowedUsers(List<String> allowedUsers) {
            this.allowedUsers = allowedUsers;
        }

        public List<String> getAllowedGroups() {
            return allowedGroups;
        }

        public void setAllowedGroups(List<String> allowedGroups) {
            this.allowedGroups = allowedGroups;
        }

        public LivyConfig getLivy() {
            return livy;
        }

        public void setLivy(LivyConfig livy) {
            this.livy = livy;
        }
    }

    public static class SparkVersionConfig {
        private String id;
        private String name;
        private boolean defaultVersion = true;
        private List<PythonEnvConfig> pythonVersions = new ArrayList<>();

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

        public boolean isDefaultVersion() {
            return defaultVersion;
        }

        public void setDefaultVersion(boolean defaultVersion) {
            this.defaultVersion = defaultVersion;
        }

        public List<PythonEnvConfig> getPythonVersions() {
            return pythonVersions;
        }

        public void setPythonVersions(List<PythonEnvConfig> pythonVersions) {
            this.pythonVersions = pythonVersions;
        }

        private Map<String, String> sparkConf = new LinkedHashMap<>();

        public Map<String, String> getSparkConf() {
            return sparkConf;
        }

        public void setSparkConf(Map<String, String> sparkConf) {
            this.sparkConf = sparkConf;
        }
    }

    public static class PythonEnvConfig {
        private String id;
        private String name;
        private boolean defaultEnv = true;

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

        public boolean isDefaultEnv() {
            return defaultEnv;
        }

        public void setDefaultEnv(boolean defaultEnv) {
            this.defaultEnv = defaultEnv;
        }

        private Map<String, String> sparkConf = new LinkedHashMap<>();

        public Map<String, String> getSparkConf() {
            return sparkConf;
        }

        public void setSparkConf(Map<String, String> sparkConf) {
            this.sparkConf = sparkConf;
        }
    }

    public static class MetastoreConfig {
        private String id;
        private String name;
        private boolean defaultMetastore = true;

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

        public boolean isDefaultMetastore() {
            return defaultMetastore;
        }

        public void setDefaultMetastore(boolean defaultMetastore) {
            this.defaultMetastore = defaultMetastore;
        }

        private Map<String, String> sparkConf = new LinkedHashMap<>();

        public Map<String, String> getSparkConf() {
            return sparkConf;
        }

        public void setSparkConf(Map<String, String> sparkConf) {
            this.sparkConf = sparkConf;
        }
    }

    public static class ResourceProfileConfig {
        private String id;
        private String name;
        private String driverMemory = "2g";
        private int driverCores = 1;
        private String executorMemory = "4g";
        private int executorCores = 2;
        private int numExecutors = 2;

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

        public String getDriverMemory() {
            return driverMemory;
        }

        public void setDriverMemory(String driverMemory) {
            this.driverMemory = driverMemory;
        }

        public int getDriverCores() {
            return driverCores;
        }

        public void setDriverCores(int driverCores) {
            this.driverCores = driverCores;
        }

        public String getExecutorMemory() {
            return executorMemory;
        }

        public void setExecutorMemory(String executorMemory) {
            this.executorMemory = executorMemory;
        }

        public int getExecutorCores() {
            return executorCores;
        }

        public void setExecutorCores(int executorCores) {
            this.executorCores = executorCores;
        }

        public int getNumExecutors() {
            return numExecutors;
        }

        public void setNumExecutors(int numExecutors) {
            this.numExecutors = numExecutors;
        }
    }

    public static class LivyConfig {
        private String auth = "none";               // none | spnego
        private boolean proxyUser = true;
        private int connectTimeoutSeconds = 10;
        private int readTimeoutSeconds = 60;
        private int sessionStartTimeoutSeconds = 300;
        private int statementTimeoutSeconds = 3600;
        private long pollIntervalMs = 500;
        private String principal;                   // livy.auth=spnego: principal of the service
        private String keytab;                      // livy.auth=spnego: keytab of the service
        private int reloginIntervalMinutes = 60;

        public String getAuth() { return auth; }
        public void setAuth(String auth) { this.auth = auth; }
        public boolean isProxyUser() { return proxyUser; }
        public void setProxyUser(boolean proxyUser) { this.proxyUser = proxyUser; }
        public int getConnectTimeoutSeconds() { return connectTimeoutSeconds; }
        public void setConnectTimeoutSeconds(int connectTimeoutSeconds) { this.connectTimeoutSeconds = connectTimeoutSeconds; }
        public int getReadTimeoutSeconds() { return readTimeoutSeconds; }
        public void setReadTimeoutSeconds(int readTimeoutSeconds) { this.readTimeoutSeconds = readTimeoutSeconds; }
        public int getSessionStartTimeoutSeconds() { return sessionStartTimeoutSeconds; }
        public void setSessionStartTimeoutSeconds(int sessionStartTimeoutSeconds) { this.sessionStartTimeoutSeconds = sessionStartTimeoutSeconds; }
        public int getStatementTimeoutSeconds() { return statementTimeoutSeconds; }
        public void setStatementTimeoutSeconds(int statementTimeoutSeconds) { this.statementTimeoutSeconds = statementTimeoutSeconds; }
        public long getPollIntervalMs() { return pollIntervalMs; }
        public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
        public String getPrincipal() { return principal; }
        public void setPrincipal(String principal) { this.principal = principal; }
        public String getKeytab() { return keytab; }
        public void setKeytab(String keytab) { this.keytab = keytab; }
        public int getReloginIntervalMinutes() { return reloginIntervalMinutes; }
        public void setReloginIntervalMinutes(int reloginIntervalMinutes) { this.reloginIntervalMinutes = reloginIntervalMinutes; }
    }
}
