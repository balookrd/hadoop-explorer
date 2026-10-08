package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Периодический запрос keepalive / heartbeat от агента к Оркестратору.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AgentHeartbeatRequest {

    @JsonProperty("agent_id")
    private String agentId;

    @JsonProperty("cluster_id")
    private String clusterId;

    @JsonProperty("grpc_address")
    private String grpcAddress;

    @JsonProperty("status")
    private String status = "online";

    @JsonProperty("active_transfers")
    private int activeTransfers = 0;

    public AgentHeartbeatRequest() {}

    public AgentHeartbeatRequest(String agentId, String clusterId, String grpcAddress, int activeTransfers) {
        this.agentId = agentId;
        this.clusterId = clusterId;
        this.grpcAddress = grpcAddress;
        this.activeTransfers = activeTransfers;
        this.status = "online";
    }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }

    public String getGrpcAddress() { return grpcAddress; }
    public void setGrpcAddress(String grpcAddress) { this.grpcAddress = grpcAddress; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public int getActiveTransfers() { return activeTransfers; }
    public void setActiveTransfers(int activeTransfers) { this.activeTransfers = activeTransfers; }
}
