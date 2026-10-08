package org.apache.hadoop.explorer.spark.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "spark_execution_history")
public class SparkExecutionHistory {

    @Id
    @Column(name = "id", length = 64, nullable = false)
    private String id;

    @Column(name = "session_id", length = 64, nullable = false)
    private String sessionId;

    @Column(name = "username", length = 128, nullable = false)
    private String username;

    @Column(name = "cluster_id", length = 64, nullable = false)
    private String clusterId;

    @Column(name = "language", length = 32, nullable = false)
    private String language = "pyspark"; // pyspark, scalaspark, sql

    @Lob
    @Column(name = "code", nullable = false)
    private String code;

    @Column(name = "status", length = 32, nullable = false)
    private String status = "QUEUED"; // QUEUED, RUNNING, FINISHED, FAILED, CANCELLED

    @Column(name = "rows_count")
    private long rowsCount = 0;

    @Column(name = "execution_time_ms")
    private double executionTimeMs = 0.0;

    @Lob
    @Column(name = "error_message")
    private String errorMessage;

    @Lob
    @Column(name = "columns_json")
    private String columnsJson;

    @Lob
    @Column(name = "logs")
    private String logs;

    @Column(name = "has_cached_result")
    private boolean hasCachedResult = false;

    @Column(name = "created_at")
    private Instant createdAt = Instant.now();

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    public SparkExecutionHistory() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
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

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
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

    public double getExecutionTimeMs() {
        return executionTimeMs;
    }

    public void setExecutionTimeMs(double executionTimeMs) {
        this.executionTimeMs = executionTimeMs;
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

    public String getLogs() {
        return logs;
    }

    public void setLogs(String logs) {
        this.logs = logs;
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
