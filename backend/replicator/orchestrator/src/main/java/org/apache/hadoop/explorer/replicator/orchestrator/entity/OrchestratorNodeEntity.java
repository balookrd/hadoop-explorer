package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Сущность узла Orchestrator для распределенного Active-Active кластера.
 * Обеспечивает отслеживание живых инстансов и координацию разделения квот трафика.
 */
@Entity
@Table(name = "replication_orchestrator_nodes")
public class OrchestratorNodeEntity {

    @Id
    @Column(name = "node_id", length = 64, nullable = false)
    private String nodeId;

    @Column(name = "host_name", length = 128, nullable = false)
    private String hostName;

    @Column(name = "last_heartbeat", nullable = false)
    private Instant lastHeartbeat = Instant.now();

    @Column(name = "status", length = 32, nullable = false)
    private String status = "ACTIVE";

    public OrchestratorNodeEntity() {
    }

    public OrchestratorNodeEntity(String nodeId, String hostName, Instant lastHeartbeat, String status) {
        this.nodeId = nodeId;
        this.hostName = hostName;
        this.lastHeartbeat = lastHeartbeat;
        this.status = status;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public Instant getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(Instant lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
