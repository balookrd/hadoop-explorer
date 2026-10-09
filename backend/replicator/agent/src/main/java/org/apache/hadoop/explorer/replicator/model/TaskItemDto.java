package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Описание пофайловой подзадачи репликации (Task DTO).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskItemDto {

    @JsonProperty("id")
    private String id;

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("run_id")
    private String runId;

    @JsonProperty("source_path")
    private String sourcePath;

    @JsonProperty("target_path")
    private String targetPath;

    @JsonProperty("file_size")
    private long fileSize;

    @JsonProperty("status")
    private String status;

    @JsonProperty("assigned_agent_id")
    private String assignedAgentId;

    @JsonProperty("checksum")
    private String checksum;

    @JsonProperty("target_address")
    private String targetAddress;

    @JsonProperty("execution_principal")
    private String executionPrincipal;

    @JsonProperty("run_as_service_account")
    private Boolean runAsServiceAccount;

    public TaskItemDto() {}

    public TaskItemDto(String id, String jobId, String runId, String sourcePath, String targetPath,
                       long fileSize, String status, String assignedAgentId, String checksum,
                       String targetAddress, String executionPrincipal, Boolean runAsServiceAccount) {
        this.id = id;
        this.jobId = jobId;
        this.runId = runId;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.fileSize = fileSize;
        this.status = status;
        this.assignedAgentId = assignedAgentId;
        this.checksum = checksum;
        this.targetAddress = targetAddress;
        this.executionPrincipal = executionPrincipal;
        this.runAsServiceAccount = runAsServiceAccount;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }

    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }

    public String getSourcePath() { return sourcePath; }
    public void setSourcePath(String sourcePath) { this.sourcePath = sourcePath; }

    public String getTargetPath() { return targetPath; }
    public void setTargetPath(String targetPath) { this.targetPath = targetPath; }

    public long getFileSize() { return fileSize; }
    public void setFileSize(long fileSize) { this.fileSize = fileSize; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getAssignedAgentId() { return assignedAgentId; }
    public void setAssignedAgentId(String assignedAgentId) { this.assignedAgentId = assignedAgentId; }

    public String getChecksum() { return checksum; }
    public void setChecksum(String checksum) { this.checksum = checksum; }

    public String getTargetAddress() { return targetAddress; }
    public void setTargetAddress(String targetAddress) { this.targetAddress = targetAddress; }

    public String getExecutionPrincipal() { return executionPrincipal; }
    public void setExecutionPrincipal(String executionPrincipal) { this.executionPrincipal = executionPrincipal; }

    public Boolean getRunAsServiceAccount() { return runAsServiceAccount; }
    public void setRunAsServiceAccount(Boolean runAsServiceAccount) { this.runAsServiceAccount = runAsServiceAccount; }
}
