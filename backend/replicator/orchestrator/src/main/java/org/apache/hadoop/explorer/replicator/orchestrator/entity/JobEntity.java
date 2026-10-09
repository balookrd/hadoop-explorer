package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "replication_jobs")
public class JobEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "source_path", nullable = false)
    private String sourcePath;

    @Column(name = "target_path", nullable = false)
    private String targetPath;

    @Column(name = "source_cluster_id", nullable = false, length = 64)
    private String sourceClusterId = "dc1";

    @Column(name = "target_cluster_id", nullable = false, length = 64)
    private String targetClusterId = "dc2";

    @Column(name = "status", nullable = false, length = 32)
    private String status = "QUEUED";

    @Column(name = "total_bytes", nullable = false)
    private long totalBytes = 0;

    @Column(name = "copied_bytes", nullable = false)
    private long copiedBytes = 0;

    @Column(name = "run_as_service_account", nullable = false)
    private boolean runAsServiceAccount = true;

    @Column(name = "execution_principal", nullable = false)
    private String executionPrincipal = "hdfs@EXAMPLE.COM";

    @Column(name = "created_by", nullable = false)
    private String createdBy = "system_operator";

    @Column(name = "is_scheduled", nullable = false)
    private boolean isScheduled = false;

    @Column(name = "cron_expression")
    private String cronExpression;

    @Column(name = "next_run_at")
    private Instant nextRunAt;

    @Column(name = "last_run_at")
    private Instant lastRunAt;

    @Column(name = "message")
    private String message;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "history_retention_runs", nullable = false)
    private int historyRetentionRuns = 20;

    @Column(name = "active_run_id", length = 64)
    private String activeRunId;

    @Column(name = "total_objects", nullable = false)
    private int totalObjects = 0;

    @Column(name = "transferred_objects", nullable = false)
    private int transferredObjects = 0;

    @Column(name = "skipped_objects", nullable = false)
    private int skippedObjects = 0;

    @Column(name = "failed_objects", nullable = false)
    private int failedObjects = 0;

    @OneToMany(mappedBy = "job", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private List<TaskEntity> tasks = new ArrayList<>();

    @OneToMany(mappedBy = "job", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @OrderBy("runNumber DESC")
    private List<JobRunEntity> runs = new ArrayList<>();

    public JobEntity() {}

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = Instant.now();
    }

    public double getProgressPercent() {
        if (totalBytes > 0) {
            return Math.round(Math.min(100.0, ((double) copiedBytes / totalBytes) * 100.0) * 10.0) / 10.0;
        }
        return "COMPLETED".equalsIgnoreCase(status) ? 100.0 : 0.0;
    }

    public Double getAverageSpeedMbS() {
        Instant st = startedAt != null ? startedAt : createdAt;
        if (st == null || copiedBytes <= 0) {
            return null;
        }
        Instant end = completedAt != null ? completedAt : Instant.now();
        long elapsedSec = DurationBetween(st, end);
        if (elapsedSec > 0) {
            double mbS = ((double) copiedBytes / (1024.0 * 1024.0)) / elapsedSec;
            return Math.round(mbS * 100.0) / 100.0;
        }
        return null;
    }

    private long DurationBetween(Instant start, Instant end) {
        return Math.max(1, end.getEpochSecond() - start.getEpochSecond());
    }

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getSourcePath() { return sourcePath; }
    public void setSourcePath(String sourcePath) { this.sourcePath = sourcePath; }
    public String getTargetPath() { return targetPath; }
    public void setTargetPath(String targetPath) { this.targetPath = targetPath; }
    public String getSourceClusterId() { return sourceClusterId; }
    public void setSourceClusterId(String sourceClusterId) { this.sourceClusterId = sourceClusterId; }
    public String getTargetClusterId() { return targetClusterId; }
    public void setTargetClusterId(String targetClusterId) { this.targetClusterId = targetClusterId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public long getTotalBytes() { return totalBytes; }
    public void setTotalBytes(long totalBytes) { this.totalBytes = totalBytes; }
    public long getCopiedBytes() { return copiedBytes; }
    public void setCopiedBytes(long copiedBytes) { this.copiedBytes = copiedBytes; }
    public boolean isRunAsServiceAccount() { return runAsServiceAccount; }
    public void setRunAsServiceAccount(boolean runAsServiceAccount) { this.runAsServiceAccount = runAsServiceAccount; }
    public String getExecutionPrincipal() { return executionPrincipal; }
    public void setExecutionPrincipal(String executionPrincipal) { this.executionPrincipal = executionPrincipal; }
    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public boolean isScheduled() { return isScheduled; }
    public void setScheduled(boolean scheduled) { isScheduled = scheduled; }
    public String getCronExpression() { return cronExpression; }
    public void setCronExpression(String cronExpression) { this.cronExpression = cronExpression; }
    public Instant getNextRunAt() { return nextRunAt; }
    public void setNextRunAt(Instant nextRunAt) { this.nextRunAt = nextRunAt; }
    public Instant getLastRunAt() { return lastRunAt; }
    public void setLastRunAt(Instant lastRunAt) { this.lastRunAt = lastRunAt; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
    public int getHistoryRetentionRuns() { return historyRetentionRuns; }
    public void setHistoryRetentionRuns(int historyRetentionRuns) { this.historyRetentionRuns = historyRetentionRuns; }
    public String getActiveRunId() { return activeRunId; }
    public void setActiveRunId(String activeRunId) { this.activeRunId = activeRunId; }
    public List<TaskEntity> getTasks() { return tasks; }
    public void setTasks(List<TaskEntity> tasks) { this.tasks = tasks; }
    public List<JobRunEntity> getRuns() { return runs; }
    public void setRuns(List<JobRunEntity> runs) { this.runs = runs; }
    public int getTotalObjects() { return totalObjects; }
    public void setTotalObjects(int totalObjects) { this.totalObjects = totalObjects; }
    public int getTransferredObjects() { return transferredObjects; }
    public void setTransferredObjects(int transferredObjects) { this.transferredObjects = transferredObjects; }
    public int getSkippedObjects() { return skippedObjects; }
    public void setSkippedObjects(int skippedObjects) { this.skippedObjects = skippedObjects; }
    public int getFailedObjects() { return failedObjects; }
    public void setFailedObjects(int failedObjects) { this.failedObjects = failedObjects; }
}
