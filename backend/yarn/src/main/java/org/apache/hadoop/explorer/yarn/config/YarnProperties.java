package org.apache.hadoop.explorer.yarn.config;

import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "yarn")
public class YarnProperties {

    /**
     * Source of queue data: "native" talks to the ResourceManager REST API, "mock" serves a
     * built-in demo tree. Deliberately independent of the authentication mode.
     */
    private String clientMode = "native";
    private AclConfig acl = new AclConfig();
    private AwxConfig awx = new AwxConfig();
    private KerberosClientConfig kerberos = new KerberosClientConfig();
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
        /** Demo stands only: report a simulated successful job instead of calling AWX. */
        private boolean simulate = false;

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
        public boolean isSimulate() { return simulate; }
        public void setSimulate(boolean simulate) { this.simulate = simulate; }
    }

    /**
     * Identity used for outbound SPNEGO calls to Kerberos-secured ResourceManagers.
     * The principal must be a Hadoop proxy user when impersonation (doAs) is enabled.
     */
    public static class KerberosClientConfig {
        private String principal;
        private String keytab;
        private String krb5Conf;

        public String getKrb5Conf() { return krb5Conf; }
        public void setKrb5Conf(String krb5Conf) { this.krb5Conf = krb5Conf; }
        public String getPrincipal() { return principal; }
        public void setPrincipal(String principal) { this.principal = principal; }
        public String getKeytab() { return keytab; }
        public void setKeytab(String keytab) { this.keytab = keytab; }
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

    public String getClientMode() { return clientMode; }
    public void setClientMode(String clientMode) { this.clientMode = clientMode; }
    public boolean isMockClient() { return "mock".equalsIgnoreCase(clientMode); }
    public KerberosClientConfig getKerberos() { return kerberos; }
    public void setKerberos(KerberosClientConfig kerberos) { this.kerberos = kerberos; }
    public AclConfig getAcl() { return acl; }
    public void setAcl(AclConfig acl) { this.acl = acl; }
    public AwxConfig getAwx() { return awx; }
    public void setAwx(AwxConfig awx) { this.awx = awx; }
    public List<ClusterConfig> getClusters() { return clusters; }
    public void setClusters(List<ClusterConfig> clusters) { this.clusters = clusters; }
}
