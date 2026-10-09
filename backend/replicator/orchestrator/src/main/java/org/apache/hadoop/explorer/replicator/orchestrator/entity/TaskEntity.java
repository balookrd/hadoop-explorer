package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.persistence.*;

@Entity
@Table(
    name = "replication_tasks",
    indexes = {
        @Index(name = "idx_task_job_id", columnList = "job_id"),
        @Index(name = "idx_task_status", columnList = "status"),
        @Index(name = "idx_task_job_status", columnList = "job_id, status")
    }
)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TaskEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "job_id", nullable = false, length = 64)
    private String jobId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", insertable = false, updatable = false)
    @JsonIgnore
    private JobEntity job;

    @Column(name = "run_id", length = 64)
    private String runId;

    @Column(name = "source_path", nullable = false)
    private String sourcePath;

    @Column(name = "target_path", nullable = false)
    private String targetPath;

    @Column(name = "file_size", nullable = false)
    private long fileSize = 0;

    @Column(name = "status", nullable = false, length = 32)
    private String status = "QUEUED";

    @Column(name = "assigned_agent_id", length = 64)
    private String assignedAgentId;

    @Column(name = "checksum")
    private String checksum;

    @Column(name = "error_message")
    private String errorMessage;

    public TaskEntity() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public JobEntity getJob() { return job; }
    public void setJob(JobEntity job) { this.job = job; }
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
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
