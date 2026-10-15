package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "replicator_dr_snapshots")
public class DrSnapshotEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "cluster_id", nullable = false, length = 64)
    private String clusterId;

    @Column(name = "operator", length = 128)
    private String operator;

    @Column(name = "reason", length = 1024)
    private String reason;

    @Column(name = "timestamp", nullable = false)
    private Instant timestamp;

    @Column(name = "network_fenced", nullable = false)
    private boolean networkFenced;

    @Column(name = "snapshot_json", columnDefinition = "TEXT")
    private String snapshotJson;

    public DrSnapshotEntity() {}

    public DrSnapshotEntity(String id, String clusterId, String operator, String reason,
                            Instant timestamp, boolean networkFenced, String snapshotJson) {
        this.id = id;
        this.clusterId = clusterId;
        this.operator = operator;
        this.reason = reason;
        this.timestamp = timestamp;
        this.networkFenced = networkFenced;
        this.snapshotJson = snapshotJson;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }

    public String getOperator() { return operator; }
    public void setOperator(String operator) { this.operator = operator; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public boolean isNetworkFenced() { return networkFenced; }
    public void setNetworkFenced(boolean networkFenced) { this.networkFenced = networkFenced; }

    public String getSnapshotJson() { return snapshotJson; }
    public void setSnapshotJson(String snapshotJson) { this.snapshotJson = snapshotJson; }
}
