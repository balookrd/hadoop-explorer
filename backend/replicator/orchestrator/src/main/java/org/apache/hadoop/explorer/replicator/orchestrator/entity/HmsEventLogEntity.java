package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(
    name = "hms_event_logs",
    indexes = {
        @Index(name = "idx_hms_event_job_id", columnList = "hms_job_id"),
        @Index(name = "idx_hms_event_status", columnList = "status"),
        @Index(name = "idx_hms_event_id_order", columnList = "hms_job_id, event_id")
    }
)
public class HmsEventLogEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "hms_job_id", nullable = false, length = 64)
    private String hmsJobId;

    @Column(name = "event_id")
    private Long eventId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "table_name", nullable = false, length = 128)
    private String tableName;

    @Column(name = "partition_name", length = 256)
    private String partitionName;

    @Column(name = "source_uri", length = 1024)
    private String sourceUri;

    @Column(name = "target_uri", length = 1024)
    private String targetUri;

    @Column(name = "subjob_id", length = 64)
    private String subjobId;

    @Column(name = "status", nullable = false, length = 32)
    private String status = "PENDING_DATA"; // PENDING_DATA, DATA_COPIED, APPLIED, SKIPPED_ACID, FAILED

    @Column(name = "error_message", length = 2048)
    private String errorMessage;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    public HmsEventLogEntity() {}

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = Instant.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getHmsJobId() { return hmsJobId; }
    public void setHmsJobId(String hmsJobId) { this.hmsJobId = hmsJobId; }
    public Long getEventId() { return eventId; }
    public void setEventId(Long eventId) { this.eventId = eventId; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getPartitionName() { return partitionName; }
    public void setPartitionName(String partitionName) { this.partitionName = partitionName; }
    public String getSourceUri() { return sourceUri; }
    public void setSourceUri(String sourceUri) { this.sourceUri = sourceUri; }
    public String getTargetUri() { return targetUri; }
    public void setTargetUri(String targetUri) { this.targetUri = targetUri; }
    public String getSubjobId() { return subjobId; }
    public void setSubjobId(String subjobId) { this.subjobId = subjobId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
