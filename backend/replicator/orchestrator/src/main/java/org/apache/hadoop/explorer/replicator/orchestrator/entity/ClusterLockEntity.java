package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Сущность распределенной блокировки для координации фоновых шедулеров и процессов между инстансами Оркестратора.
 */
@Entity
@Table(name = "cluster_locks")
public class ClusterLockEntity {

    @Id
    @Column(name = "lock_name", nullable = false, length = 128)
    private String lockName;

    @Column(name = "locked_by", nullable = false, length = 128)
    private String lockedBy;

    @Column(name = "locked_until", nullable = false)
    private Instant lockedUntil;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    public ClusterLockEntity() {}

    public ClusterLockEntity(String lockName, String lockedBy, Instant lockedUntil) {
        this.lockName = lockName;
        this.lockedBy = lockedBy;
        this.lockedUntil = lockedUntil;
        this.updatedAt = Instant.now();
    }

    public String getLockName() { return lockName; }
    public void setLockName(String lockName) { this.lockName = lockName; }
    public String getLockedBy() { return lockedBy; }
    public void setLockedBy(String lockedBy) { this.lockedBy = lockedBy; }
    public Instant getLockedUntil() { return lockedUntil; }
    public void setLockedUntil(Instant lockedUntil) { this.lockedUntil = lockedUntil; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
