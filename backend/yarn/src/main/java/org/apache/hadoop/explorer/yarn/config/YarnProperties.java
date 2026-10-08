package org.apache.hadoop.explorer.yarn.config;

import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "yarn")
public class YarnProperties {

    private AclConfig acl = new AclConfig();
    private AwxConfig awx = new AwxConfig();
    private List<ClusterConfig> clusters = new ArrayList<>();

    public YarnProperties() {}

    public static class AclConfig {
        private boolean enforceFourEyes = true;

        public boolean isEnforceFourEyes() { return enforceFourEyes; }
        public void setEnforceFourEyes(boolean enforceFourEyes) { this.enforceFourEyes = enforceFourEyes; }
    }

    public static class AwxConfig {
        private boolean enabled = true;
        private String baseUrl = "https://awx.company.local";
        private String token = "SampleAwxApplicationTokenHere";
        private boolean verifySsl = true;
        private int defaultJobTemplateId = 101;
        private int pollIntervalSeconds = 2;
        private int timeoutSeconds = 180;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
        public String getToken() { return token; }
        public void setToken(String token) { this.token = token; }
        public boolean isVerifySsl() { return verifySsl; }
        public void setVerifySsl(boolean verifySsl) { this.verifySsl = verifySsl; }
        public int getDefaultJobTemplateId() { return defaultJobTemplateId; }
        public void setDefaultJobTemplateId(int defaultJobTemplateId) { this.defaultJobTemplateId = defaultJobTemplateId; }
        public int getPollIntervalSeconds() { return pollIntervalSeconds; }
        public void setPollIntervalSeconds(int pollIntervalSeconds) { this.pollIntervalSeconds = pollIntervalSeconds; }
        public int getTimeoutSeconds() { return timeoutSeconds; }
        public void setTimeoutSeconds(int timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }
    }

    public ClusterConfig findCluster(String clusterId) {
        if (clusterId == null) {
            return null;
        }
        return clusters.stream()
                .filter(c -> clusterId.equals(c.getId()))
                .findFirst()
                .orElse(null);
    }

    public AclConfig getAcl() { return acl; }
    public void setAcl(AclConfig acl) { this.acl = acl; }
    public AwxConfig getAwx() { return awx; }
    public void setAwx(AwxConfig awx) { this.awx = awx; }
    public List<ClusterConfig> getClusters() { return clusters; }
    public void setClusters(List<ClusterConfig> clusters) { this.clusters = clusters; }
}
