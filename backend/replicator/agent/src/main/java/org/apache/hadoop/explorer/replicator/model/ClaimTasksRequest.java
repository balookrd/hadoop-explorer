package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Запрос воркера на взятие пофайловых задач из распределенного пула Оркестратора.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClaimTasksRequest {

    @JsonProperty("agent_id")
    private String agentId;

    @JsonProperty("cluster_id")
    private String clusterId;

    @JsonProperty("limit")
    private int limit = 5;

    public ClaimTasksRequest() {}

    public ClaimTasksRequest(String agentId, String clusterId, int limit) {
        this.agentId = agentId;
        this.clusterId = clusterId;
        this.limit = limit > 0 ? limit : 5;
    }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }

    public int getLimit() { return limit; }
    public void setLimit(int limit) { this.limit = limit; }
}
