package org.apache.hadoop.explorer.replicator.model;

public class StreamingLeaseRenewRequest {
    private String clusterId;
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
