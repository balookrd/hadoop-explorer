package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO с детальной информацией об узле агента Kafka репликации
 * (включая локальный лимит полосы пропускания, заданный инженером при старте демона).
 */
public class KafkaAgentEndpointDto {

    @JsonProperty("agent_id")
    private String agentId;

    @JsonProperty("grpc_address")
    private String grpcAddress;

    @JsonProperty("cluster_id")
    private String clusterId;

    @JsonProperty("mode")
    private String mode;

    @JsonProperty("status")
    private String status;

    @JsonProperty("max_bandwidth_mb_s")
    private Double maxBandwidthMbS;

    public KafkaAgentEndpointDto() {}

    public KafkaAgentEndpointDto(String agentId, String grpcAddress, String clusterId,
                                String mode, String status, Double maxBandwidthMbS) {
        this.agentId = agentId;
        this.grpcAddress = grpcAddress;
        this.clusterId = clusterId;
        this.mode = mode;
        this.status = status;
        this.maxBandwidthMbS = maxBandwidthMbS;
    }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public String getGrpcAddress() { return grpcAddress; }
    public void setGrpcAddress(String grpcAddress) { this.grpcAddress = grpcAddress; }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Double getMaxBandwidthMbS() { return maxBandwidthMbS; }
    public void setMaxBandwidthMbS(Double maxBandwidthMbS) { this.maxBandwidthMbS = maxBandwidthMbS; }
}
