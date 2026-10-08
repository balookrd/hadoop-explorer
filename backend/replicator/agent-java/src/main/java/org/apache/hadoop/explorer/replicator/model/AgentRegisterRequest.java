package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Запрос первичной регистрации агента в Оркестраторе (Service Discovery).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AgentRegisterRequest {

    @JsonProperty("agent_id")
    private String agentId;

    @JsonProperty("cluster_id")
    private String clusterId;

    @JsonProperty("dc_id")
    private String dcId;

    @JsonProperty("mode")
    private String mode = "all";

    @JsonProperty("grpc_address")
    private String grpcAddress;

    @JsonProperty("hostname")
    private String hostname;

    @JsonProperty("version")
    private String version = "1.0.0-java";

    @JsonProperty("max_bandwidth_mb_s")
    private Double maxBandwidthMbS;

    public AgentRegisterRequest() {}

    public AgentRegisterRequest(String agentId, String clusterId, String mode, String grpcAddress, String hostname, Double maxBandwidthMbS) {
        this.agentId = agentId;
        this.clusterId = clusterId;
        this.mode = mode != null ? mode : "all";
        this.grpcAddress = grpcAddress;
        this.hostname = hostname;
        this.maxBandwidthMbS = maxBandwidthMbS;
    }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }

    public String getDcId() { return dcId; }
    public void setDcId(String dcId) { this.dcId = dcId; }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

    public String getGrpcAddress() { return grpcAddress; }
    public void setGrpcAddress(String grpcAddress) { this.grpcAddress = grpcAddress; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public Double getMaxBandwidthMbS() { return maxBandwidthMbS; }
    public void setMaxBandwidthMbS(Double maxBandwidthMbS) { this.maxBandwidthMbS = maxBandwidthMbS; }
}
