package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Запрос воркера на фиксацию успешного завершения пофайловой задачи.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CompleteTaskRequest {

    @JsonProperty("task_id")
    private String taskId;

    @JsonProperty("agent_id")
    private String agentId;

    @JsonProperty("bytes_transferred")
    private long bytesTransferred;

    @JsonProperty("checksum")
    private String checksum;

    public CompleteTaskRequest() {}

    public CompleteTaskRequest(String taskId, String agentId, long bytesTransferred, String checksum) {
        this.taskId = taskId;
        this.agentId = agentId;
        this.bytesTransferred = bytesTransferred;
        this.checksum = checksum;
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public long getBytesTransferred() { return bytesTransferred; }
    public void setBytesTransferred(long bytesTransferred) { this.bytesTransferred = bytesTransferred; }

    public String getChecksum() { return checksum; }
    public void setChecksum(String checksum) { this.checksum = checksum; }
}
