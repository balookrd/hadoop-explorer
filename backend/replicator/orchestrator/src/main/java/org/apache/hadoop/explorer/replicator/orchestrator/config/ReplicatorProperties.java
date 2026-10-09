package org.apache.hadoop.explorer.replicator.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "hadoop.replicator")
public class ReplicatorProperties {

    private long globalLimitBytesPerSec = 100L * 1024 * 1024; // 100 MB/s
    private String agentSecret = "replicator-secure-agent-secret-key-12345";
    private boolean enforceClusterWhitelist = false;
    private int agentHeartbeatTimeoutSeconds = 15;
    private int agentOfflineTimeoutSeconds = 45;
    private int maxTaskRetries = 3;
    private int taskTimeoutSeconds = 60;

    private List<DatacenterConfig> datacenters = new ArrayList<>(List.of(
        new DatacenterConfig("dc1", "Дата-Центр 1 (Primary DC)", "zone-a"),
        new DatacenterConfig("dc2", "Дата-Центр 2 (Disaster Recovery)", "zone-b")
    ));

    private List<ClusterConfig> clusters = new ArrayList<>(List.of(
        new ClusterConfig("dc1", "HDFS DC1 Production", "dc1", "hdfs://namenode-dc1:8020", "agent-dc1", 50051),
        new ClusterConfig("dc2", "HDFS DC2 Disaster Recovery", "dc2", "hdfs://namenode-dc2:8020", "agent-dc2", 50051)
    ));

    public long getGlobalLimitBytesPerSec() { return globalLimitBytesPerSec; }
    public void setGlobalLimitBytesPerSec(long globalLimitBytesPerSec) { this.globalLimitBytesPerSec = globalLimitBytesPerSec; }
    public String getAgentSecret() { return agentSecret; }
    public void setAgentSecret(String agentSecret) { this.agentSecret = agentSecret; }
    public boolean isEnforceClusterWhitelist() { return enforceClusterWhitelist; }
    public void setEnforceClusterWhitelist(boolean enforceClusterWhitelist) { this.enforceClusterWhitelist = enforceClusterWhitelist; }
    public int getAgentHeartbeatTimeoutSeconds() { return agentHeartbeatTimeoutSeconds; }
    public void setAgentHeartbeatTimeoutSeconds(int agentHeartbeatTimeoutSeconds) { this.agentHeartbeatTimeoutSeconds = agentHeartbeatTimeoutSeconds; }
    public int getAgentOfflineTimeoutSeconds() { return agentOfflineTimeoutSeconds; }
    public void setAgentOfflineTimeoutSeconds(int agentOfflineTimeoutSeconds) { this.agentOfflineTimeoutSeconds = agentOfflineTimeoutSeconds; }
    public int getMaxTaskRetries() { return maxTaskRetries; }
    public void setMaxTaskRetries(int maxTaskRetries) { this.maxTaskRetries = maxTaskRetries; }
    public int getTaskTimeoutSeconds() { return taskTimeoutSeconds; }
    public void setTaskTimeoutSeconds(int taskTimeoutSeconds) { this.taskTimeoutSeconds = taskTimeoutSeconds; }
    public List<DatacenterConfig> getDatacenters() { return datacenters; }
    public void setDatacenters(List<DatacenterConfig> datacenters) { this.datacenters = datacenters; }
    public List<ClusterConfig> getClusters() { return clusters; }
    public void setClusters(List<ClusterConfig> clusters) { this.clusters = clusters; }

    public static class DatacenterConfig {
        private String id;
        private String name;
        private String networkZone;

        public DatacenterConfig() {}
        public DatacenterConfig(String id, String name, String networkZone) {
            this.id = id;
            this.name = name;
            this.networkZone = networkZone;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getNetworkZone() { return networkZone; }
        public void setNetworkZone(String networkZone) { this.networkZone = networkZone; }
    }

    public static class ClusterConfig {
        private String id;
        private String name;
        private String dcId;
        private String hdfsRpcAddress;
        private String grpcHost;
        private int grpcPort = 50051;

        public ClusterConfig() {}
        public ClusterConfig(String id, String name, String dcId, String hdfsRpcAddress, String grpcHost, int grpcPort) {
            this.id = id;
            this.name = name;
            this.dcId = dcId;
            this.hdfsRpcAddress = hdfsRpcAddress;
            this.grpcHost = grpcHost;
            this.grpcPort = grpcPort;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getDcId() { return dcId; }
        public void setDcId(String dcId) { this.dcId = dcId; }
        public String getHdfsRpcAddress() { return hdfsRpcAddress; }
        public void setHdfsRpcAddress(String hdfsRpcAddress) { this.hdfsRpcAddress = hdfsRpcAddress; }
        public String getGrpcHost() { return grpcHost; }
        public void setGrpcHost(String grpcHost) { this.grpcHost = grpcHost; }
        public int getGrpcPort() { return grpcPort; }
        public void setGrpcPort(int grpcPort) { this.grpcPort = grpcPort; }
    }

    private List<FederationMappingConfig> federationMappings = new ArrayList<>(List.of(
        new FederationMappingConfig("ns-cold", "dc1-ns-cold", "hdfs://ns-cold-dc2:8020", "dc2-ns-cold"),
        new FederationMappingConfig("ns-hot", "dc1-ns-hot", "hdfs://ns-hot-dc2:8020", "dc2-ns-hot"),
        new FederationMappingConfig("namenode-dc1:8020", "dc1", "hdfs://namenode-dc2:8020", "dc2")
    ));

    public List<FederationMappingConfig> getFederationMappings() { return federationMappings; }
    public void setFederationMappings(List<FederationMappingConfig> federationMappings) { this.federationMappings = federationMappings; }

    public static class FederationMappingConfig {
        private String sourceNameservice;
        private String sourceClusterId;
        private String targetNameservice;
        private String targetClusterId;

        public FederationMappingConfig() {}
        public FederationMappingConfig(String sourceNameservice, String sourceClusterId, String targetNameservice, String targetClusterId) {
            this.sourceNameservice = sourceNameservice;
            this.sourceClusterId = sourceClusterId;
            this.targetNameservice = targetNameservice;
            this.targetClusterId = targetClusterId;
        }

        public String getSourceNameservice() { return sourceNameservice; }
        public void setSourceNameservice(String sourceNameservice) { this.sourceNameservice = sourceNameservice; }
        public String getSourceClusterId() { return sourceClusterId; }
        public void setSourceClusterId(String sourceClusterId) { this.sourceClusterId = sourceClusterId; }
        public String getTargetNameservice() { return targetNameservice; }
        public void setTargetNameservice(String targetNameservice) { this.targetNameservice = targetNameservice; }
        public String getTargetClusterId() { return targetClusterId; }
        public void setTargetClusterId(String targetClusterId) { this.targetClusterId = targetClusterId; }
    }
}
