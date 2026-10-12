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

    @JsonProperty("mode")
    private String mode;

    @JsonProperty("transferred_bytes")
    private long transferredBytes = 0;

    @JsonProperty("transfer_errors")
    private long transferErrors = 0;

    @JsonProperty("bandwidth_limit_mb_s")
    private Double bandwidthLimitMbS;

    public AgentHeartbeatRequest() {}

    public AgentHeartbeatRequest(String agentId, String clusterId, String grpcAddress, int activeTransfers) {
        this(agentId, clusterId, grpcAddress, activeTransfers, null);
    }

    public AgentHeartbeatRequest(String agentId, String clusterId, String grpcAddress, int activeTransfers, String mode) {
        this.agentId = agentId;
        this.clusterId = clusterId;
        this.grpcAddress = grpcAddress;
        this.activeTransfers = activeTransfers;
        this.status = "online";
        this.mode = mode;
    }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

    public long getTransferredBytes() { return transferredBytes; }
    public void setTransferredBytes(long transferredBytes) { this.transferredBytes = transferredBytes; }

    public long getTransferErrors() { return transferErrors; }
    public void setTransferErrors(long transferErrors) { this.transferErrors = transferErrors; }

    public Double getBandwidthLimitMbS() { return bandwidthLimitMbS; }
    public void setBandwidthLimitMbS(Double bandwidthLimitMbS) { this.bandwidthLimitMbS = bandwidthLimitMbS; }

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
