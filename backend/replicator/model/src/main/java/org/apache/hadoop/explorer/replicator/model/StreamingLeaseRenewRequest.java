package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamingLeaseRenewRequest {
    @JsonProperty("cluster_id")
    private String clusterId;
    @JsonProperty("agent_id")
    private String agentId;

    public StreamingLeaseRenewRequest() {}

    public StreamingLeaseRenewRequest(String clusterId, String agentId) {
        this.clusterId = clusterId;
        this.agentId = agentId;
    }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }
}
