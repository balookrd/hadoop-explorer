package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(
    name = "hms_replication_jobs",
    indexes = {
        @Index(name = "idx_hms_jobs_status", columnList = "status"),
        @Index(name = "idx_hms_jobs_assigned_agent", columnList = "assigned_agent_id"),
        @Index(name = "idx_hms_jobs_src_cluster", columnList = "source_cluster_id"),
        @Index(name = "idx_hms_jobs_lease_exp", columnList = "lease_expires_at")
    }
)
public class HmsReplicationJobEntity {

    /**
     * Версия для оптимистичной блокировки.
     *
     * <p>Статус и счетчики задачи обновляются read-modify-write одновременно из gRPC-вызовов
     * агентов, планировщиков и REST, в том числе с разных узлов active-active. Без версии
     * последняя запись молча затирала предыдущую: параллельные отказы терялись в счетчике,
     * а финализация задачи перетирала статус failover-а.
     */
    @Version
    @Column(name = "version")
    private Long version;

    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "source_cluster_id", nullable = false, length = 64)
    private String sourceClusterId;

    @Column(name = "target_cluster_id", nullable = false, length = 64)
    private String targetClusterId;

    @Column(name = "source_db_name", nullable = false, length = 128)
    private String sourceDbName;

    @Column(name = "target_db_name", nullable = false, length = 128)
    private String targetDbName;

    @Column(name = "table_include_pattern")
    private String tableIncludePattern = "*";

    @Column(name = "table_exclude_pattern")
    private String tableExcludePattern;

    @Column(name = "drop_extraneous_tables", nullable = false)
    private boolean dropExtraneousTables = false;

    @Column(name = "drop_extraneous_partitions", nullable = false)
    private boolean dropExtraneousPartitions = false;

    @Column(name = "enable_hdfs_streaming", nullable = false)
    @JsonProperty("enable_hdfs_streaming")
    private boolean enableHdfsStreaming = true;

    @Column(name = "companion_hdfs_job_id", length = 64)
    @JsonProperty("companion_hdfs_job_id")
    private String companionHdfsJobId;

    @Column(name = "db_warehouse_location", length = 512)
    @JsonProperty("db_warehouse_location")
    private String dbWarehouseLocation;

    @Column(name = "status", nullable = false, length = 32)
    private String status = "BOOTSTRAPPING"; // BOOTSTRAPPING, ACTIVE, PAUSED, ERROR

    @Column(name = "bootstrap_event_id")
    private Long bootstrapEventId;

    @Column(name = "last_processed_event_id")
    private Long lastProcessedEventId = 0L;

    @Column(name = "event_lag")
    private Long eventLag = 0L;

    @Column(name = "total_tables", nullable = false)
    private int totalTables = 0;

    @Column(name = "replicated_tables", nullable = false)
    private int replicatedTables = 0;

    @Column(name = "total_partitions", nullable = false)
    private int totalPartitions = 0;

    @Column(name = "replicated_partitions", nullable = false)
    private int replicatedPartitions = 0;

    @Column(name = "message")
    private String message;

    @Column(name = "created_by", nullable = false)
    private String createdBy = "system";

    @Column(name = "execution_principal", length = 128)
    private String executionPrincipal = "hdfs@EXAMPLE.COM";

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @Column(name = "last_sync_at")
    private Instant lastSyncAt;

    @Column(name = "assigned_agent_id", length = 64)
    private String assignedAgentId;

    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    @Column(name = "last_failed_agent_id", length = 64)
    private String lastFailedAgentId;

    /** Статус до паузы: по нему возобновление решает, вернуться в CDC или заново в bootstrap. */
    @Column(name = "paused_from_status", length = 32)
    private String pausedFromStatus;

    @Transient
    @JsonProperty("started_at")
    private Instant startedAt;

    @Transient
    @JsonProperty("completed_at")
    private Instant completedAt;

    @Transient
    @JsonProperty("estimated_completion_at")
    private Instant estimatedCompletionAt;

    @Transient
    @JsonProperty("average_speed_mb_s")
    private Double averageSpeedMbS;

    @Transient
    @JsonProperty("total_bytes")
    private Long totalBytes;

    @Transient
    @JsonProperty("copied_bytes")
    private Long copiedBytes;

    @Transient
    @JsonProperty("progress_percent")
    private Double progressPercent;

    @Transient
    @JsonProperty("subtasks_count")
    private Integer subtasksCount;

    @Transient
    @JsonProperty("active_subtasks_count")
    private Integer activeSubtasksCount;

    @Transient
    @JsonProperty("companion_streamer_online")
    private Boolean companionStreamerOnline;

    public HmsReplicationJobEntity() {}

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = Instant.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getSourceClusterId() { return sourceClusterId; }
    public void setSourceClusterId(String sourceClusterId) { this.sourceClusterId = sourceClusterId; }
    public String getTargetClusterId() { return targetClusterId; }
    public void setTargetClusterId(String targetClusterId) { this.targetClusterId = targetClusterId; }
    public String getSourceDbName() { return sourceDbName; }
    public void setSourceDbName(String sourceDbName) { this.sourceDbName = sourceDbName; }
    public String getTargetDbName() { return targetDbName; }
    public void setTargetDbName(String targetDbName) { this.targetDbName = targetDbName; }
    public String getTableIncludePattern() { return tableIncludePattern; }
    public void setTableIncludePattern(String tableIncludePattern) { this.tableIncludePattern = tableIncludePattern; }
    public String getTableExcludePattern() { return tableExcludePattern; }
    public void setTableExcludePattern(String tableExcludePattern) { this.tableExcludePattern = tableExcludePattern; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public org.apache.hadoop.explorer.replicator.model.HmsJobStatus getJobStatus() {
        return org.apache.hadoop.explorer.replicator.model.HmsJobStatus.fromString(this.status);
    }
    public void setJobStatus(org.apache.hadoop.explorer.replicator.model.HmsJobStatus jobStatus) {
        this.status = jobStatus != null ? jobStatus.name() : org.apache.hadoop.explorer.replicator.model.HmsJobStatus.QUEUED.name();
    }
    public void transitionTo(org.apache.hadoop.explorer.replicator.model.HmsJobStatus targetStatus) {
        getJobStatus().validateTransition(targetStatus);
        setJobStatus(targetStatus);
    }
    /**
     * Пауза с запоминанием прежнего статуса.
     *
     * <p>Аренда снимается: агент, работавший над задачей, получит отказ на следующем отчете и
     * остановится, а после возобновления задачу заново захватит опросом любой агент.
     *
     * @return {@code false}, если из текущего статуса пауза невозможна (задача остановлена,
     *         завершена с ошибкой и т.п.)
     */
    public boolean pause() {
        org.apache.hadoop.explorer.replicator.model.HmsJobStatus current = getJobStatus();
        if (current == org.apache.hadoop.explorer.replicator.model.HmsJobStatus.PAUSED) {
            return true;
        }
        if (!current.canTransitionTo(org.apache.hadoop.explorer.replicator.model.HmsJobStatus.PAUSED)) {
            return false;
        }
        this.pausedFromStatus = current.name();
        setJobStatus(org.apache.hadoop.explorer.replicator.model.HmsJobStatus.PAUSED);
        this.assignedAgentId = null;
        this.leaseExpiresAt = null;
        return true;
    }

    /**
     * Возобновление: CDC — с сохраненной позиции, все остальное — с bootstrap.
     *
     * <p>Пауза посреди bootstrap оставляет схему перенесенной частично; переход сразу в ACTIVE
     * пропустил бы остаток. Bootstrap идемпотентен (существующие объекты обновляются), поэтому
     * повтор с начала безопасен.
     *
     * @return {@code false}, если задача не на паузе
     */
    public boolean resume() {
        if (getJobStatus() != org.apache.hadoop.explorer.replicator.model.HmsJobStatus.PAUSED) {
            return false;
        }
        boolean wasStreaming = org.apache.hadoop.explorer.replicator.model.HmsJobStatus.ACTIVE.name()
                .equalsIgnoreCase(pausedFromStatus);
        setJobStatus(wasStreaming
                ? org.apache.hadoop.explorer.replicator.model.HmsJobStatus.ACTIVE
                : org.apache.hadoop.explorer.replicator.model.HmsJobStatus.QUEUED);
        this.pausedFromStatus = null;
        return true;
    }

    public String getPausedFromStatus() { return pausedFromStatus; }
    public void setPausedFromStatus(String pausedFromStatus) { this.pausedFromStatus = pausedFromStatus; }
    public Long getBootstrapEventId() { return bootstrapEventId; }
    public void setBootstrapEventId(Long bootstrapEventId) { this.bootstrapEventId = bootstrapEventId; }
    public Long getLastProcessedEventId() { return lastProcessedEventId; }
    public void setLastProcessedEventId(Long lastProcessedEventId) { this.lastProcessedEventId = lastProcessedEventId; }
    public Long getEventLag() { return eventLag; }
    public void setEventLag(Long eventLag) { this.eventLag = eventLag; }
    public int getTotalTables() { return totalTables; }
    public void setTotalTables(int totalTables) { this.totalTables = totalTables; }
    public int getReplicatedTables() { return replicatedTables; }
    public void setReplicatedTables(int replicatedTables) { this.replicatedTables = replicatedTables; }
    public int getTotalPartitions() { return totalPartitions; }
    public void setTotalPartitions(int totalPartitions) { this.totalPartitions = totalPartitions; }
    public int getReplicatedPartitions() { return replicatedPartitions; }
    public void setReplicatedPartitions(int replicatedPartitions) { this.replicatedPartitions = replicatedPartitions; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public String getExecutionPrincipal() { return executionPrincipal; }
    public void setExecutionPrincipal(String executionPrincipal) { this.executionPrincipal = executionPrincipal; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public Instant getLastSyncAt() { return lastSyncAt; }
    public void setLastSyncAt(Instant lastSyncAt) { this.lastSyncAt = lastSyncAt; }
    public boolean isDropExtraneousTables() { return dropExtraneousTables; }
    public void setDropExtraneousTables(boolean dropExtraneousTables) { this.dropExtraneousTables = dropExtraneousTables; }
    public boolean isDropExtraneousPartitions() { return dropExtraneousPartitions; }
    public void setDropExtraneousPartitions(boolean dropExtraneousPartitions) { this.dropExtraneousPartitions = dropExtraneousPartitions; }
    public boolean isEnableHdfsStreaming() { return enableHdfsStreaming; }
    public void setEnableHdfsStreaming(boolean enableHdfsStreaming) { this.enableHdfsStreaming = enableHdfsStreaming; }
    public String getCompanionHdfsJobId() { return companionHdfsJobId; }
    public void setCompanionHdfsJobId(String companionHdfsJobId) { this.companionHdfsJobId = companionHdfsJobId; }
    public String getDbWarehouseLocation() { return dbWarehouseLocation; }
    public void setDbWarehouseLocation(String dbWarehouseLocation) { this.dbWarehouseLocation = dbWarehouseLocation; }
    public String getAssignedAgentId() { return assignedAgentId; }
    public void setAssignedAgentId(String assignedAgentId) { this.assignedAgentId = assignedAgentId; }
    public Instant getLeaseExpiresAt() { return leaseExpiresAt; }
    public void setLeaseExpiresAt(Instant leaseExpiresAt) { this.leaseExpiresAt = leaseExpiresAt; }
    public String getLastFailedAgentId() { return lastFailedAgentId; }
    public void setLastFailedAgentId(String lastFailedAgentId) { this.lastFailedAgentId = lastFailedAgentId; }

    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
    public Instant getEstimatedCompletionAt() { return estimatedCompletionAt; }
    public void setEstimatedCompletionAt(Instant estimatedCompletionAt) { this.estimatedCompletionAt = estimatedCompletionAt; }
    public Double getAverageSpeedMbS() { return averageSpeedMbS; }
    public void setAverageSpeedMbS(Double averageSpeedMbS) { this.averageSpeedMbS = averageSpeedMbS; }
    public Long getTotalBytes() { return totalBytes; }
    public void setTotalBytes(Long totalBytes) { this.totalBytes = totalBytes; }
    public Long getCopiedBytes() { return copiedBytes; }
    public void setCopiedBytes(Long copiedBytes) { this.copiedBytes = copiedBytes; }
    public Double getProgressPercent() { return progressPercent; }
    public void setProgressPercent(Double progressPercent) { this.progressPercent = progressPercent; }
    public Integer getSubtasksCount() { return subtasksCount; }
    public void setSubtasksCount(Integer subtasksCount) { this.subtasksCount = subtasksCount; }
    public Integer getActiveSubtasksCount() { return activeSubtasksCount; }
    public void setActiveSubtasksCount(Integer activeSubtasksCount) { this.activeSubtasksCount = activeSubtasksCount; }
    public Boolean getCompanionStreamerOnline() { return companionStreamerOnline; }
    public void setCompanionStreamerOnline(Boolean companionStreamerOnline) { this.companionStreamerOnline = companionStreamerOnline; }
}
