package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamingLeaseRenewResponse {
    private String status;
    private long epoch;
    private String activeAgentId;
    private int registeredStreamersCount;
    private boolean redundancyWarning;
    private long lastCommittedTxid;

    public StreamingLeaseRenewResponse() {}

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public long getEpoch() { return epoch; }
    public void setEpoch(long epoch) { this.epoch = epoch; }

    public String getActiveAgentId() { return activeAgentId; }
    public void setActiveAgentId(String activeAgentId) { this.activeAgentId = activeAgentId; }

    public int getRegisteredStreamersCount() { return registeredStreamersCount; }
    public void setRegisteredStreamersCount(int registeredStreamersCount) { this.registeredStreamersCount = registeredStreamersCount; }

    public boolean isRedundancyWarning() { return redundancyWarning; }
    public void setRedundancyWarning(boolean redundancyWarning) { this.redundancyWarning = redundancyWarning; }

    public long getLastCommittedTxid() { return lastCommittedTxid; }
    public void setLastCommittedTxid(long lastCommittedTxid) { this.lastCommittedTxid = lastCommittedTxid; }
}
