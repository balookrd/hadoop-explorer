package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "hms_replication_jobs")
public class HmsReplicationJobEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "source_cluster_id", nullable = false, length = 64)
    private String sourceClusterId = "dc1";

    @Column(name = "target_cluster_id", nullable = false, length = 64)
    private String targetClusterId = "dc2";

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
    private String createdBy = "system_operator";

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
    public String getAssignedAgentId() { return assignedAgentId; }
    public void setAssignedAgentId(String assignedAgentId) { this.assignedAgentId = assignedAgentId; }
    public Instant getLeaseExpiresAt() { return leaseExpiresAt; }
    public void setLeaseExpiresAt(Instant leaseExpiresAt) { this.leaseExpiresAt = leaseExpiresAt; }
    public String getLastFailedAgentId() { return lastFailedAgentId; }
    public void setLastFailedAgentId(String lastFailedAgentId) { this.lastFailedAgentId = lastFailedAgentId; }
}
