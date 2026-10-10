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

    @JsonProperty("files_count")
    private int filesCount = 1;

    public CompleteTaskRequest() {}

    public CompleteTaskRequest(String taskId, String agentId, long bytesTransferred, String checksum) {
        this(taskId, agentId, bytesTransferred, checksum, 1);
    }

    public CompleteTaskRequest(String taskId, String agentId, long bytesTransferred, String checksum, int filesCount) {
        this.taskId = taskId;
        this.agentId = agentId;
        this.bytesTransferred = bytesTransferred;
        this.checksum = checksum;
        this.filesCount = filesCount > 0 ? filesCount : 1;
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public long getBytesTransferred() { return bytesTransferred; }
    public void setBytesTransferred(long bytesTransferred) { this.bytesTransferred = bytesTransferred; }

    public String getChecksum() { return checksum; }
    public void setChecksum(String checksum) { this.checksum = checksum; }

    public int getFilesCount() { return filesCount; }
    public void setFilesCount(int filesCount) { this.filesCount = filesCount; }
}
