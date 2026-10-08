package org.apache.hadoop.explorer.sql.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "query_history")
public class QueryHistory {

    @Id
    @Column(name = "id", length = 64, nullable = false)
    private String id;

    @Column(name = "username", length = 128, nullable = false)
    private String username;

    @Column(name = "cluster_id", length = 64, nullable = false)
    private String clusterId;

    @Column(name = "cluster_name", length = 128, nullable = false)
    private String clusterName;

    @Column(name = "engine_type", length = 32, nullable = false)
    private String engineType;

    @Lob
    @Column(name = "query_text", nullable = false)
    private String queryText;

    @Column(name = "status", length = 32, nullable = false)
    private String status = "QUEUED"; // QUEUED, RUNNING, FINISHED, FAILED, CANCELLED

    @Column(name = "rows_count")
    private long rowsCount = 0;

    @Column(name = "bytes_processed")
    private long bytesProcessed = 0;

    @Column(name = "execution_time_ms")
    private double executionTimeMs = 0.0;

    @Column(name = "progress_percent")
    private double progressPercent = 0.0;

    @Lob
    @Column(name = "error_message")
    private String errorMessage;

    @Lob
    @Column(name = "columns_json")
    private String columnsJson;

    @Column(name = "is_in_queue")
    private boolean inQueue = true;

    @Column(name = "has_cached_result")
    private boolean hasCachedResult = false;

    @Column(name = "created_at")
    private Instant createdAt = Instant.now();

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    public QueryHistory() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public String getQueryText() {
        return queryText;
    }

    public void setQueryText(String queryText) {
        this.queryText = queryText;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getRowsCount() {
        return rowsCount;
    }

    public void setRowsCount(long rowsCount) {
        this.rowsCount = rowsCount;
    }

    public long getBytesProcessed() {
        return bytesProcessed;
    }

    public void setBytesProcessed(long bytesProcessed) {
        this.bytesProcessed = bytesProcessed;
    }

    public double getExecutionTimeMs() {
        return executionTimeMs;
    }

    public void setExecutionTimeMs(double executionTimeMs) {
        this.executionTimeMs = executionTimeMs;
    }

    public double getProgressPercent() {
        return progressPercent;
    }

    public void setProgressPercent(double progressPercent) {
        this.progressPercent = progressPercent;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getColumnsJson() {
        return columnsJson;
    }

    public void setColumnsJson(String columnsJson) {
        this.columnsJson = columnsJson;
    }

    public boolean isInQueue() {
        return inQueue;
    }

    public void setInQueue(boolean inQueue) {
        this.inQueue = inQueue;
    }

    public boolean isHasCachedResult() {
        return hasCachedResult;
    }

    public void setHasCachedResult(boolean hasCachedResult) {
        this.hasCachedResult = hasCachedResult;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }
}
