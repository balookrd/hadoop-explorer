package org.apache.hadoop.explorer.replicator.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "hadoop.replicator")
public class ReplicatorProperties {

    /**
     * Секрет, который раньше был зашит как значение по умолчанию.
     *
     * <p>Оставлен только для распознавания унаследованных конфигураций: установка, где он
     * задан явно, использует публично известный из репозитория ключ, и запуск прерывается.
     */
    public static final String LEGACY_DEFAULT_AGENT_SECRET = "replicator-secure-agent-secret-key-12345";

    private long globalLimitBytesPerSec = 100L * 1024 * 1024; // 100 MB/s
    /** Секрета по умолчанию нет: он задается конфигурацией или REPLICATOR_AGENT_SECRET. */
    private String agentSecret;
    private boolean enforceAgentSecret = true;
    private boolean enforceClusterWhitelist = true;
    private boolean allowLoopbackAgents = true;
    private int agentHeartbeatTimeoutSeconds = 15;
    private int agentOfflineTimeoutSeconds = 45;
    private int taskRetentionDays = 7;
    /** Размер пакета удаления при очистке истории задач. */
    private int taskRetentionBatchSize = 500;
    /** Сколько пакетов удаляется за один проход очистки. */
    private int taskRetentionMaxBatchesPerRun = 20;
    private int maxTaskRetries = 3;
    private int taskTimeoutSeconds = 60;
    private long hmsPollIntervalMs = 5000L;
    private int hmsBootstrapConcurrency = 8;
    private int hmsPartitionBatchSize = 1000;
    private int hmsLeaseTimeoutSeconds = 30;
    private long smallFileThresholdBytes = 1024 * 1024L; // 1 MB
    private long bundleTargetSizeBytes = 16 * 1024 * 1024L; // 16 MB
    private int maxBundleFiles = 500;
    private int bundleCommitConcurrency = 8;
    private StreamingConfig streaming = new StreamingConfig();
    private GrpcControlPlaneConfig grpc = new GrpcControlPlaneConfig();

    private List<DatacenterConfig> datacenters = new ArrayList<>();
    private List<ClusterConfig> clusters = new ArrayList<>();

    public long getGlobalLimitBytesPerSec() { return globalLimitBytesPerSec; }
    public void setGlobalLimitBytesPerSec(long globalLimitBytesPerSec) { this.globalLimitBytesPerSec = globalLimitBytesPerSec; }
    public String getAgentSecret() {
        String envSecret = System.getenv("REPLICATOR_AGENT_SECRET");
        if (envSecret != null && !envSecret.isBlank()) {
            return envSecret;
        }
        return agentSecret;
    }
    public void setAgentSecret(String agentSecret) { this.agentSecret = agentSecret; }
    public boolean isEnforceAgentSecret() { return enforceAgentSecret; }
    public void setEnforceAgentSecret(boolean enforceAgentSecret) { this.enforceAgentSecret = enforceAgentSecret; }
    public boolean isEnforceClusterWhitelist() { return enforceClusterWhitelist; }
    public void setEnforceClusterWhitelist(boolean enforceClusterWhitelist) { this.enforceClusterWhitelist = enforceClusterWhitelist; }
    public boolean isAllowLoopbackAgents() { return allowLoopbackAgents; }
    public void setAllowLoopbackAgents(boolean allowLoopbackAgents) { this.allowLoopbackAgents = allowLoopbackAgents; }
    public int getAgentHeartbeatTimeoutSeconds() { return agentHeartbeatTimeoutSeconds; }
    public void setAgentHeartbeatTimeoutSeconds(int agentHeartbeatTimeoutSeconds) { this.agentHeartbeatTimeoutSeconds = agentHeartbeatTimeoutSeconds; }
    public int getAgentOfflineTimeoutSeconds() { return agentOfflineTimeoutSeconds; }
    public void setAgentOfflineTimeoutSeconds(int agentOfflineTimeoutSeconds) { this.agentOfflineTimeoutSeconds = agentOfflineTimeoutSeconds; }
    public int getTaskRetentionDays() { return taskRetentionDays; }
    public void setTaskRetentionDays(int taskRetentionDays) { this.taskRetentionDays = taskRetentionDays; }
    public int getTaskRetentionBatchSize() { return taskRetentionBatchSize; }
    public void setTaskRetentionBatchSize(int taskRetentionBatchSize) { this.taskRetentionBatchSize = taskRetentionBatchSize; }
    public int getTaskRetentionMaxBatchesPerRun() { return taskRetentionMaxBatchesPerRun; }
    public void setTaskRetentionMaxBatchesPerRun(int taskRetentionMaxBatchesPerRun) { this.taskRetentionMaxBatchesPerRun = taskRetentionMaxBatchesPerRun; }
    public int getMaxTaskRetries() { return maxTaskRetries; }
    public void setMaxTaskRetries(int maxTaskRetries) { this.maxTaskRetries = maxTaskRetries; }
    public int getTaskTimeoutSeconds() { return taskTimeoutSeconds; }
    public void setTaskTimeoutSeconds(int taskTimeoutSeconds) { this.taskTimeoutSeconds = taskTimeoutSeconds; }
    public long getHmsPollIntervalMs() { return hmsPollIntervalMs; }
    public void setHmsPollIntervalMs(long hmsPollIntervalMs) { this.hmsPollIntervalMs = hmsPollIntervalMs; }
    public int getHmsBootstrapConcurrency() { return hmsBootstrapConcurrency; }
    public void setHmsBootstrapConcurrency(int hmsBootstrapConcurrency) { this.hmsBootstrapConcurrency = hmsBootstrapConcurrency; }
    public int getHmsPartitionBatchSize() { return hmsPartitionBatchSize; }
    public void setHmsPartitionBatchSize(int hmsPartitionBatchSize) { this.hmsPartitionBatchSize = hmsPartitionBatchSize; }
    public int getHmsLeaseTimeoutSeconds() { return hmsLeaseTimeoutSeconds; }
    public void setHmsLeaseTimeoutSeconds(int hmsLeaseTimeoutSeconds) { this.hmsLeaseTimeoutSeconds = hmsLeaseTimeoutSeconds; }
    public long getSmallFileThresholdBytes() { return smallFileThresholdBytes; }
    public void setSmallFileThresholdBytes(long smallFileThresholdBytes) { this.smallFileThresholdBytes = smallFileThresholdBytes; }
    public long getBundleTargetSizeBytes() { return bundleTargetSizeBytes; }
    public void setBundleTargetSizeBytes(long bundleTargetSizeBytes) { this.bundleTargetSizeBytes = bundleTargetSizeBytes; }
    public int getMaxBundleFiles() { return maxBundleFiles; }
    public void setMaxBundleFiles(int maxBundleFiles) { this.maxBundleFiles = maxBundleFiles; }
    public int getBundleCommitConcurrency() { return bundleCommitConcurrency; }
    public void setBundleCommitConcurrency(int bundleCommitConcurrency) { this.bundleCommitConcurrency = bundleCommitConcurrency; }
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
        private String kafkaBootstrapServers;
        private String schemaRegistryUrl;
        private String schemaRegistryType = "CONFLUENT";
        private boolean kerberosEnabled = false;
        private String principal;
        private String keytabPath;

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
        public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
        public void setKafkaBootstrapServers(String kafkaBootstrapServers) { this.kafkaBootstrapServers = kafkaBootstrapServers; }
        public String getSchemaRegistryUrl() { return schemaRegistryUrl; }
        public void setSchemaRegistryUrl(String schemaRegistryUrl) { this.schemaRegistryUrl = schemaRegistryUrl; }
        public String getSchemaRegistryType() { return schemaRegistryType; }
        public void setSchemaRegistryType(String schemaRegistryType) { this.schemaRegistryType = schemaRegistryType; }
        public boolean isKerberosEnabled() { return kerberosEnabled; }
        public void setKerberosEnabled(boolean kerberosEnabled) { this.kerberosEnabled = kerberosEnabled; }
        public String getPrincipal() { return principal; }
        public void setPrincipal(String principal) { this.principal = principal; }
        public String getKeytabPath() { return keytabPath; }
        public void setKeytabPath(String keytabPath) { this.keytabPath = keytabPath; }
    }

    private List<FederationMappingConfig> federationMappings = new ArrayList<>();

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

    public StreamingConfig getStreaming() { return streaming; }
    public void setStreaming(StreamingConfig streaming) { this.streaming = streaming; }

    public static class StreamingConfig {
        private boolean enabled = false;
        private long pollIntervalMs = 1000L;
        private int maxEventsPerBatch = 100;
        private long debounceDelayMs = 1500L;
        private boolean autoReconciliationOnGap = true;
        private long maxTxidLagAlert = 50000L;
        private int maxPendingTasksThreshold = 5000;
        private int inotifyQueueCapacity = 10000;
        private double inotifyHighWatermark = 0.8;
        private double inotifyLowWatermark = 0.3;
        private int inotifyMaxBatchSize = 500;
        private long inotifyFlushIntervalMs = 200L;
        private long inotifyBackpressureMaxDelayMs = 5000L;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getPollIntervalMs() { return pollIntervalMs; }
        public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
        public int getMaxEventsPerBatch() { return maxEventsPerBatch; }
        public void setMaxEventsPerBatch(int maxEventsPerBatch) { this.maxEventsPerBatch = maxEventsPerBatch; }
        public long getDebounceDelayMs() { return debounceDelayMs; }
        public void setDebounceDelayMs(long debounceDelayMs) { this.debounceDelayMs = debounceDelayMs; }
        public boolean isAutoReconciliationOnGap() { return autoReconciliationOnGap; }
        public void setAutoReconciliationOnGap(boolean autoReconciliationOnGap) { this.autoReconciliationOnGap = autoReconciliationOnGap; }
        public long getMaxTxidLagAlert() { return maxTxidLagAlert; }
        public void setMaxTxidLagAlert(long maxTxidLagAlert) { this.maxTxidLagAlert = maxTxidLagAlert; }
        public int getMaxPendingTasksThreshold() { return maxPendingTasksThreshold; }
        public void setMaxPendingTasksThreshold(int maxPendingTasksThreshold) { this.maxPendingTasksThreshold = maxPendingTasksThreshold; }
        public int getInotifyQueueCapacity() { return inotifyQueueCapacity; }
        public void setInotifyQueueCapacity(int inotifyQueueCapacity) { this.inotifyQueueCapacity = inotifyQueueCapacity; }
        public double getInotifyHighWatermark() { return inotifyHighWatermark; }
        public void setInotifyHighWatermark(double inotifyHighWatermark) { this.inotifyHighWatermark = inotifyHighWatermark; }
        public double getInotifyLowWatermark() { return inotifyLowWatermark; }
        public void setInotifyLowWatermark(double inotifyLowWatermark) { this.inotifyLowWatermark = inotifyLowWatermark; }
        public int getInotifyMaxBatchSize() { return inotifyMaxBatchSize; }
        public void setInotifyMaxBatchSize(int inotifyMaxBatchSize) { this.inotifyMaxBatchSize = inotifyMaxBatchSize; }
        public long getInotifyFlushIntervalMs() { return inotifyFlushIntervalMs; }
        public void setInotifyFlushIntervalMs(long inotifyFlushIntervalMs) { this.inotifyFlushIntervalMs = inotifyFlushIntervalMs; }
        public long getInotifyBackpressureMaxDelayMs() { return inotifyBackpressureMaxDelayMs; }
        public void setInotifyBackpressureMaxDelayMs(long inotifyBackpressureMaxDelayMs) { this.inotifyBackpressureMaxDelayMs = inotifyBackpressureMaxDelayMs; }
    }

    public GrpcControlPlaneConfig getGrpc() { return grpc; }
    public void setGrpc(GrpcControlPlaneConfig grpc) { this.grpc = grpc; }

    public static class GrpcControlPlaneConfig {
        private boolean enabled = true;
        private int port = 50050;
        private int maxInboundMessageSize = 16 * 1024 * 1024;
        private boolean tlsEnabled = false;
        private String certChainPath;
        private String privateKeyPath;
        private String trustCertCollectionPath;
        private String clientAuth = "NONE";

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public int getMaxInboundMessageSize() { return maxInboundMessageSize; }
        public void setMaxInboundMessageSize(int maxInboundMessageSize) { this.maxInboundMessageSize = maxInboundMessageSize; }
        public boolean isTlsEnabled() { return tlsEnabled; }
        public void setTlsEnabled(boolean tlsEnabled) { this.tlsEnabled = tlsEnabled; }
        public String getCertChainPath() { return certChainPath; }
        public void setCertChainPath(String certChainPath) { this.certChainPath = certChainPath; }
        public String getPrivateKeyPath() { return privateKeyPath; }
        public void setPrivateKeyPath(String privateKeyPath) { this.privateKeyPath = privateKeyPath; }
        public String getTrustCertCollectionPath() { return trustCertCollectionPath; }
        public void setTrustCertCollectionPath(String trustCertCollectionPath) { this.trustCertCollectionPath = trustCertCollectionPath; }
        public String getClientAuth() { return clientAuth; }
        public void setClientAuth(String clientAuth) { this.clientAuth = clientAuth; }
    }
}

