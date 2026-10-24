package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Запрос воркера на выделение полосы пропускания (Hierarchical Token Bucket).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TokenRequest {

    @JsonProperty("worker_id")
    private String workerId;

    @JsonProperty("requested_bytes")
    private long requestedBytes;

    @JsonProperty("source_cluster_id")
    private String sourceClusterId;

    @JsonProperty("target_cluster_id")
    private String targetClusterId;

    public TokenRequest() {}

    public TokenRequest(String workerId, long requestedBytes, String sourceClusterId, String targetClusterId) {
        this.workerId = workerId;
        this.requestedBytes = requestedBytes;
        this.sourceClusterId = sourceClusterId;
        this.targetClusterId = targetClusterId;
    }

    public String getWorkerId() { return workerId; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }

    public long getRequestedBytes() { return requestedBytes; }
    public void setRequestedBytes(long requestedBytes) { this.requestedBytes = requestedBytes; }

    public String getSourceClusterId() { return sourceClusterId; }
    public void setSourceClusterId(String sourceClusterId) { this.sourceClusterId = sourceClusterId; }

    public String getTargetClusterId() { return targetClusterId; }
    public void setTargetClusterId(String targetClusterId) { this.targetClusterId = targetClusterId; }
}
