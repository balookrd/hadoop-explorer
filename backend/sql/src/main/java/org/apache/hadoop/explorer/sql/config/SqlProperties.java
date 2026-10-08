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

    public static class AiConfig {
        private boolean enabled = true;
        private String url = "http://localhost:8000/v1";
        private String model = "qwen2.5-coder-32b";
        private String apiKey = "";
        private String provider = "mock-builtin";

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
    }
}
