package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Сущность агента репликации в БД Оркестратора.
 * Обеспечивает персистентное хранение состояния воркеров и полнофункциональный
 * stateless Active-Active режим нескольких инстансов Оркестратора.
 */
@Entity
@Table(name = "replication_agents")
public class AgentEntity {

    @Id
    @Column(name = "agent_id", length = 64, nullable = false)
    private String agentId;

    @Column(name = "cluster_id", length = 64, nullable = false)
    private String clusterId;

    @Column(name = "host", length = 128, nullable = false)
    private String host;

    @Column(name = "grpc_port", nullable = false)
    private int grpcPort;

    @Column(name = "grpc_address", length = 160, nullable = false)
    private String grpcAddress;

    @Column(name = "mode", length = 32, nullable = false)
    private String mode = "all";

    @Column(name = "active_transfers", nullable = false)
    private int activeTransfers = 0;

    @Column(name = "max_bandwidth_mb_s")
    private Double maxBandwidthMbS;

    @Column(name = "status", length = 32, nullable = false)
    private String status = "ONLINE";

    @Column(name = "last_heartbeat", nullable = false)
    private Instant lastHeartbeat = Instant.now();

    @Column(name = "transferred_bytes", nullable = false)
    private long transferredBytes = 0L;

    @Column(name = "transfer_errors", nullable = false)
    private long transferErrors = 0L;

    @Column(name = "compression_raw_bytes", nullable = false)
    private long compressionRawBytes = 0L;

    @Column(name = "compression_compressed_bytes", nullable = false)
    private long compressionCompressedBytes = 0L;

    @Column(name = "compression_ratio", nullable = false)
    private double compressionRatio = 1.0;

    @Column(name = "chunk_latency_avg_ms", nullable = false)
    private double chunkLatencyAvgMs = 0.0;

    @Column(name = "chunk_latency_max_ms", nullable = false)
    private double chunkLatencyMaxMs = 0.0;

    @Column(name = "chunk_transfer_count", nullable = false)
    private long chunkTransferCount = 0L;

    @Column(name = "staging_active_files", nullable = false)
    private int stagingActiveFiles = 0;

    @Column(name = "staging_active_bytes", nullable = false)
    private long stagingActiveBytes = 0L;

    @Column(name = "staging_orphans_cleaned", nullable = false)
    private long stagingOrphansCleaned = 0L;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    public AgentEntity() {}

    public AgentEntity(String agentId, String clusterId, String host, int grpcPort, String grpcAddress, String mode, Double maxBandwidthMbS) {
        this.agentId = agentId;
        this.clusterId = clusterId;
        this.host = host;
        this.grpcPort = grpcPort;
        this.grpcAddress = grpcAddress;
        this.mode = mode != null ? mode : "all";
        this.maxBandwidthMbS = maxBandwidthMbS;
        this.status = "ONLINE";
        this.lastHeartbeat = Instant.now();
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getGrpcPort() { return grpcPort; }
    public void setGrpcPort(int grpcPort) { this.grpcPort = grpcPort; }

    public String getGrpcAddress() { return grpcAddress; }
    public void setGrpcAddress(String grpcAddress) { this.grpcAddress = grpcAddress; }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

    public int getActiveTransfers() { return activeTransfers; }
    public void setActiveTransfers(int activeTransfers) { this.activeTransfers = activeTransfers; }

    public Double getMaxBandwidthMbS() { return maxBandwidthMbS; }
    public void setMaxBandwidthMbS(Double maxBandwidthMbS) { this.maxBandwidthMbS = maxBandwidthMbS; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Instant getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(Instant lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }

    public long getTransferredBytes() { return transferredBytes; }
    public void setTransferredBytes(long transferredBytes) { this.transferredBytes = transferredBytes; }

    public long getTransferErrors() { return transferErrors; }
    public void setTransferErrors(long transferErrors) { this.transferErrors = transferErrors; }

    public long getCompressionRawBytes() { return compressionRawBytes; }
    public void setCompressionRawBytes(long compressionRawBytes) { this.compressionRawBytes = compressionRawBytes; }

    public long getCompressionCompressedBytes() { return compressionCompressedBytes; }
    public void setCompressionCompressedBytes(long compressionCompressedBytes) { this.compressionCompressedBytes = compressionCompressedBytes; }

    public double getCompressionRatio() { return compressionRatio; }
    public void setCompressionRatio(double compressionRatio) { this.compressionRatio = compressionRatio; }

    public double getChunkLatencyAvgMs() { return chunkLatencyAvgMs; }
    public void setChunkLatencyAvgMs(double chunkLatencyAvgMs) { this.chunkLatencyAvgMs = chunkLatencyAvgMs; }

    public double getChunkLatencyMaxMs() { return chunkLatencyMaxMs; }
    public void setChunkLatencyMaxMs(double chunkLatencyMaxMs) { this.chunkLatencyMaxMs = chunkLatencyMaxMs; }

    public long getChunkTransferCount() { return chunkTransferCount; }
    public void setChunkTransferCount(long chunkTransferCount) { this.chunkTransferCount = chunkTransferCount; }

    public int getStagingActiveFiles() { return stagingActiveFiles; }
    public void setStagingActiveFiles(int stagingActiveFiles) { this.stagingActiveFiles = stagingActiveFiles; }

    public long getStagingActiveBytes() { return stagingActiveBytes; }
    public void setStagingActiveBytes(long stagingActiveBytes) { this.stagingActiveBytes = stagingActiveBytes; }

    public long getStagingOrphansCleaned() { return stagingOrphansCleaned; }
    public void setStagingOrphansCleaned(long stagingOrphansCleaned) { this.stagingOrphansCleaned = stagingOrphansCleaned; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
