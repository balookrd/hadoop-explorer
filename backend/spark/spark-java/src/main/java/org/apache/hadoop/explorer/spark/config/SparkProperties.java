package org.apache.hadoop.explorer.spark.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "spark")
public class SparkProperties {

    private List<SparkClusterConfig> clusters = new ArrayList<>();
    private int sessionTtlMinutes = 120;
    private int resultsTtlHours = 24;

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

    public static class SparkClusterConfig {
        private String id;
        private String name;
        private String description = "";
        private String type = "livy";
        private String livyUrl = "http://localhost:8998";
        private String yarnClusterId = "yarn-prod";
        private boolean mockStorage = true;
        private List<SparkVersionConfig> sparkVersions = new ArrayList<>();
        private List<MetastoreConfig> metastores = new ArrayList<>();
        private List<String> yarnQueues = new ArrayList<>(List.of("root.default", "root.analytics"));
        private String defaultQueue = "root.default";
        private List<ResourceProfileConfig> resourceProfiles = new ArrayList<>();
        private List<String> defaultRepositories = new ArrayList<>();
        private List<String> allowedUsers = new ArrayList<>();
        private List<String> allowedGroups = new ArrayList<>();

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
}
