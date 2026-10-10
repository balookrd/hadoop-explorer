package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Сущность распределенного лизинга Active-Standby для агентов HDFS Inotify стриминга.
 * Обеспечивает строгий эксклюзивный доступ (ровно 1 активный стример на кластер)
 * и автоматический failover со сменой эпох (Epoch Fencing).
 */
@Entity
@Table(name = "replicator_streaming_leases")
public class StreamingLeaseEntity {

    @Id
    @Column(name = "cluster_id", nullable = false, length = 64)
    private String clusterId;

    @Column(name = "active_agent_id", nullable = false, length = 128)
    private String activeAgentId;

    @Column(name = "epoch", nullable = false)
    private long epoch = 1;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    @Column(name = "last_renewed_at", nullable = false)
    private Instant lastRenewedAt;

    public StreamingLeaseEntity() {
    }

    public StreamingLeaseEntity(String clusterId, String activeAgentId, long epoch, Instant expiresAt, Instant lastRenewedAt) {
        this.clusterId = clusterId;
        this.activeAgentId = activeAgentId;
        this.epoch = epoch;
        this.expiresAt = expiresAt;
        this.lastRenewedAt = lastRenewedAt;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getActiveAgentId() {
        return activeAgentId;
    }

    public void setActiveAgentId(String activeAgentId) {
        this.activeAgentId = activeAgentId;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }

    public Instant getLastRenewedAt() {
        return lastRenewedAt;
    }

    public void setLastRenewedAt(Instant lastRenewedAt) {
        this.lastRenewedAt = lastRenewedAt;
    }
}
