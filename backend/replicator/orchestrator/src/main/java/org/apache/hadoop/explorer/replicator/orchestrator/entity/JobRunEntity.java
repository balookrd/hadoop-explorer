package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.persistence.*;
import java.time.Duration;
import java.time.Instant;

@Entity
@Table(name = "job_runs")
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class JobRunEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "job_id", nullable = false, length = 64)
    private String jobId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", insertable = false, updatable = false)
    @JsonIgnore
    private JobEntity job;

    @Column(name = "run_number", nullable = false)
    private int runNumber = 1;

    @Column(name = "trigger_type", nullable = false, length = 32)
    private String triggerType = "MANUAL"; // MANUAL, SCHEDULED

    @Column(name = "status", nullable = false, length = 32)
    private String status = "QUEUED";

    @Column(name = "total_bytes", nullable = false)
    private long totalBytes = 0;

    @Column(name = "copied_bytes", nullable = false)
    private long copiedBytes = 0;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "duration_seconds")
    private Double durationSeconds;

    @Column(name = "average_speed_mb_s")
    private Double averageSpeedMbS;

    @Column(name = "message")
    private String message;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "error_details")
    private String errorDetails;

    @Column(name = "triggered_by")
    private String triggeredBy = "system";

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "total_objects")
    private int totalObjects = 0;

    @Column(name = "transferred_objects")
    private int transferredObjects = 0;

    @Column(name = "skipped_objects")
    private int skippedObjects = 0;

    @Column(name = "failed_objects")
    private int failedObjects = 0;

    public JobRunEntity() {}

    public void calculateMetrics() {
        if (startedAt != null && completedAt != null) {
            long ms = Duration.between(startedAt, completedAt).toMillis();
            this.durationSeconds = Math.max(0.1, ms / 1000.0);
            if (this.copiedBytes > 0 && this.durationSeconds > 0) {
                double mb = (double) this.copiedBytes / (1024.0 * 1024.0);
                this.averageSpeedMbS = Math.round((mb / this.durationSeconds) * 100.0) / 100.0;
            }
        }
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public JobEntity getJob() { return job; }
    public void setJob(JobEntity job) { this.job = job; }
    public int getRunNumber() { return runNumber; }
    public void setRunNumber(int runNumber) { this.runNumber = runNumber; }
    public String getTriggerType() { return triggerType; }
    public void setTriggerType(String triggerType) { this.triggerType = triggerType; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public long getTotalBytes() { return totalBytes; }
    public void setTotalBytes(long totalBytes) { this.totalBytes = totalBytes; }
    public long getCopiedBytes() { return copiedBytes; }
    public void setCopiedBytes(long copiedBytes) { this.copiedBytes = copiedBytes; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
    public Double getDurationSeconds() { return durationSeconds; }
    public void setDurationSeconds(Double durationSeconds) { this.durationSeconds = durationSeconds; }
    public Double getAverageSpeedMbS() { return averageSpeedMbS; }
    public void setAverageSpeedMbS(Double averageSpeedMbS) { this.averageSpeedMbS = averageSpeedMbS; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public String getErrorDetails() { return errorDetails; }
    public void setErrorDetails(String errorDetails) { this.errorDetails = errorDetails; }
    public String getTriggeredBy() { return triggeredBy; }
    public void setTriggeredBy(String triggeredBy) { this.triggeredBy = triggeredBy; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public int getTotalObjects() { return totalObjects; }
    public void setTotalObjects(int totalObjects) { this.totalObjects = totalObjects; }
    public int getTransferredObjects() { return transferredObjects; }
    public void setTransferredObjects(int transferredObjects) { this.transferredObjects = transferredObjects; }
    public int getSkippedObjects() { return skippedObjects; }
    public void setSkippedObjects(int skippedObjects) { this.skippedObjects = skippedObjects; }
    public int getFailedObjects() { return failedObjects; }
    public void setFailedObjects(int failedObjects) { this.failedObjects = failedObjects; }
}
