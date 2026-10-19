package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * Сущность для хранения ядовитых (poison pill) CDC-событий Hive Metastore в Dead-Letter Queue (DLQ).
 * Позволяет изолировать сбойные DDL-события и предотвратить зависание репликации схемы.
 */
@Entity
@Table(
    name = "hms_dead_letter_events",
    indexes = {
        @Index(name = "idx_hms_dlq_job_id", columnList = "hms_job_id"),
        @Index(name = "idx_hms_dlq_status", columnList = "status"),
        @Index(name = "idx_hms_dlq_event_id", columnList = "hms_job_id, event_id")
    }
)
public class HmsDeadLetterEventEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "hms_job_id", nullable = false, length = 64)
    private String hmsJobId;

    @Column(name = "event_id", nullable = false)
    private Long eventId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "table_name", length = 128)
    private String tableName;

    @Column(name = "partition_name", length = 256)
    private String partitionName;

    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "failed_at", nullable = false)
    private Instant failedAt = Instant.now();

    @Column(name = "retry_count", nullable = false)
    private int retryCount = 1;

    @Column(name = "status", nullable = false, length = 32)
    private String status = "NEW"; // NEW, IGNORED, REPLAYED

    public HmsDeadLetterEventEntity() {}

    public HmsDeadLetterEventEntity(
            String id,
            String hmsJobId,
            Long eventId,
            String eventType,
            String tableName,
            String partitionName,
            String payload,
            String errorMessage
    ) {
        this.id = id;
        this.hmsJobId = hmsJobId;
        this.eventId = eventId;
        this.eventType = eventType;
        this.tableName = tableName;
        this.partitionName = partitionName;
        this.payload = payload;
        this.errorMessage = errorMessage;
        this.failedAt = Instant.now();
        this.retryCount = 1;
        this.status = "NEW";
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

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public Instant getFailedAt() { return failedAt; }
    public void setFailedAt(Instant failedAt) { this.failedAt = failedAt; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}
