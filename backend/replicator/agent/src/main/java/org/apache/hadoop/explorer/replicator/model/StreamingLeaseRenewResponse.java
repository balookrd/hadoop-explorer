package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamingLeaseRenewResponse {
    @JsonProperty("status")
    private String status;
    @JsonProperty("epoch")
    private long epoch;
    @JsonProperty("active_agent_id")
    private String activeAgentId;
    @JsonProperty("registered_streamers_count")
    private int registeredStreamersCount;
    @JsonProperty("redundancy_warning")
    private boolean redundancyWarning;
    @JsonProperty("last_committed_txid")
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
