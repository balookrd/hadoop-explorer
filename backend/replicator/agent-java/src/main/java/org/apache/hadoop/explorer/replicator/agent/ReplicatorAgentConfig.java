package org.apache.hadoop.explorer.replicator.agent;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Конфигурация Replicator Agent на Java.
 * Загружает параметры из переменных окружения с разумными значениями по умолчанию.
 */
public class ReplicatorAgentConfig {

    private String agentId;
    private String clusterId;
    private String mode;
    private String orchestratorUrl;
    private String receiverHost;
    private int receiverPort;
    private String stagingDir;
    private String defaultFsUri;
    private String fallbackTargetAddress;
    private String advertisedGrpcAddress;
    private String agentSecret;
    private Double maxBandwidthMbS;
    private double pollIntervalSec;
    private boolean enableDynamicRegistration;
    private String keytabPath;
    private String principal;
    private int chunkSize;

    public static ReplicatorAgentConfig fromEnv() {
        ReplicatorAgentConfig config = new ReplicatorAgentConfig();

        String envId = getEnv("AGENT_ID", getEnv("WORKER_ID", null));
        config.agentId = (envId != null && !envId.isBlank()) ? envId : "agent-java-" + UUID.randomUUID().toString().substring(0, 8);

        config.clusterId = getEnv("AGENT_CLUSTER_ID", getEnv("CLUSTER_ID", null));
        config.mode = getEnv("AGENT_MODE", "all").toLowerCase();
        config.orchestratorUrl = getEnv("ORCHESTRATOR_URL", "http://localhost:8005");
        config.receiverHost = getEnv("RECEIVER_HOST", "0.0.0.0");
        config.receiverPort = Integer.parseInt(getEnv("RECEIVER_PORT", "50051"));
        config.stagingDir = getEnv("REPLICATOR_STAGING_DIR", "/tmp/staging");

        String hdfsNamenode = getEnv("REPLICATOR_HDFS_NAMENODE", null);
        String hdfsPort = getEnv("REPLICATOR_HDFS_PORT", "8020");
        String defaultFs = getEnv("HDFS_DEFAULT_FS", null);
        if (defaultFs != null && !defaultFs.isBlank()) {
            config.defaultFsUri = defaultFs;
        } else if (hdfsNamenode != null && !hdfsNamenode.isBlank()) {
            config.defaultFsUri = "hdfs://" + hdfsNamenode + ":" + hdfsPort;
        }

        config.fallbackTargetAddress = getEnv("FALLBACK_TARGET_ADDRESS", getEnv("RECEIVER_ADDRESS", "localhost:50051"));

        String adv = getEnv("AGENT_ADVERTISED_ADDRESS", getEnv("RECEIVER_ADVERTISED_ADDRESS", null));
        if (adv != null && !adv.isBlank()) {
            config.advertisedGrpcAddress = adv;
        } else {
            String host = "localhost";
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (Exception ignored) {}
            config.advertisedGrpcAddress = host + ":" + config.receiverPort;
        }

        config.agentSecret = getEnv("REPLICATOR_AGENT_SECRET", getEnv("AGENT_SECRET", null));

        String bw = getEnv("AGENT_MAX_BANDWIDTH_MB_S", null);
        if (bw != null && !bw.isBlank()) {
            try {
                config.maxBandwidthMbS = Double.parseDouble(bw);
            } catch (NumberFormatException ignored) {}
        }

        config.pollIntervalSec = Double.parseDouble(getEnv("POLL_INTERVAL_SEC", "3.0"));
        config.enableDynamicRegistration = Boolean.parseBoolean(getEnv("AGENT_ENABLE_REGISTRATION", "true"));
        config.keytabPath = getEnv("KRB5_KEYTAB", getEnv("REPLICATOR_KEYTAB_PATH", null));
        config.principal = getEnv("KRB5_PRINCIPAL", getEnv("REPLICATOR_EXECUTION_PRINCIPAL", null));
        config.chunkSize = Integer.parseInt(getEnv("CHUNK_SIZE_BYTES", String.valueOf(64 * 1024)));

        return config;
    }

    private static String getEnv(String name, String def) {
        String val = System.getenv(name);
        return (val != null && !val.isBlank()) ? val : def;
    }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

    public String getOrchestratorUrl() { return orchestratorUrl; }
    public void setOrchestratorUrl(String orchestratorUrl) { this.orchestratorUrl = orchestratorUrl; }

    public String getReceiverHost() { return receiverHost; }
    public void setReceiverHost(String receiverHost) { this.receiverHost = receiverHost; }

    public int getReceiverPort() { return receiverPort; }
    public void setReceiverPort(int receiverPort) { this.receiverPort = receiverPort; }

    public String getStagingDir() { return stagingDir; }
    public void setStagingDir(String stagingDir) { this.stagingDir = stagingDir; }

    public String getDefaultFsUri() { return defaultFsUri; }
    public void setDefaultFsUri(String defaultFsUri) { this.defaultFsUri = defaultFsUri; }

    public String getFallbackTargetAddress() { return fallbackTargetAddress; }
    public void setFallbackTargetAddress(String fallbackTargetAddress) { this.fallbackTargetAddress = fallbackTargetAddress; }

    public String getAdvertisedGrpcAddress() { return advertisedGrpcAddress; }
    public void setAdvertisedGrpcAddress(String advertisedGrpcAddress) { this.advertisedGrpcAddress = advertisedGrpcAddress; }

    public String getAgentSecret() { return agentSecret; }
    public void setAgentSecret(String agentSecret) { this.agentSecret = agentSecret; }

    public Double getMaxBandwidthMbS() { return maxBandwidthMbS; }
    public void setMaxBandwidthMbS(Double maxBandwidthMbS) { this.maxBandwidthMbS = maxBandwidthMbS; }

    public double getPollIntervalSec() { return pollIntervalSec; }
    public void setPollIntervalSec(double pollIntervalSec) { this.pollIntervalSec = pollIntervalSec; }

    public boolean isEnableDynamicRegistration() { return enableDynamicRegistration; }
    public void setEnableDynamicRegistration(boolean enableDynamicRegistration) { this.enableDynamicRegistration = enableDynamicRegistration; }

    public String getKeytabPath() { return keytabPath; }
    public void setKeytabPath(String keytabPath) { this.keytabPath = keytabPath; }

    public String getPrincipal() { return principal; }
    public void setPrincipal(String principal) { this.principal = principal; }

    public int getChunkSize() { return chunkSize; }
    public void setChunkSize(int chunkSize) { this.chunkSize = chunkSize; }
}
