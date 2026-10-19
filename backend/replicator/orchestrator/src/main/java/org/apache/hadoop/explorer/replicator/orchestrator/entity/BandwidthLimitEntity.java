package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Сущность персистентного хранения динамических лимитов полосы пропускания
 * (глобального, DC-DC и HDFS-HDFS) для межсервисной синхронизации в Active-Active пуле.
 */
@Entity
@Table(name = "replication_bandwidth_limits")
public class BandwidthLimitEntity {

    @Id
    @Column(name = "limit_key", length = 128, nullable = false)
    private String limitKey;

    @Column(name = "limit_bytes_per_sec", nullable = false)
    private Long limitBytesPerSec;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    public BandwidthLimitEntity() {
    }

    public BandwidthLimitEntity(String limitKey, Long limitBytesPerSec, Instant updatedAt) {
        this.limitKey = limitKey;
        this.limitBytesPerSec = limitBytesPerSec;
        this.updatedAt = updatedAt != null ? updatedAt : Instant.now();
    }

    public String getLimitKey() {
        return limitKey;
    }

    public void setLimitKey(String limitKey) {
        this.limitKey = limitKey;
    }

    public Long getLimitBytesPerSec() {
        return limitBytesPerSec;
    }

    public void setLimitBytesPerSec(Long limitBytesPerSec) {
        this.limitBytesPerSec = limitBytesPerSec;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
