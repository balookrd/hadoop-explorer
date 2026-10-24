package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Периодический запрос keepalive / heartbeat от агента к Оркестратору.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AgentHeartbeatRequest {

    @JsonProperty("agent_id")
    private String agentId;

    @JsonProperty("cluster_id")
    private String clusterId;

    @JsonProperty("grpc_address")
    private String grpcAddress;

    @JsonProperty("status")
    private String status = "online";

    @JsonProperty("active_transfers")
    private int activeTransfers = 0;

    @JsonProperty("mode")
    private String mode;

    @JsonProperty("transferred_bytes")
    private long transferredBytes = 0;

    @JsonProperty("transfer_errors")
    private long transferErrors = 0;

    @JsonProperty("bandwidth_limit_mb_s")
    private Double bandwidthLimitMbS;

    @JsonProperty("compression_raw_bytes")
    private long compressionRawBytes = 0;

    @JsonProperty("compression_compressed_bytes")
    private long compressionCompressedBytes = 0;

    @JsonProperty("compression_ratio")
    private Double compressionRatio = 1.0;

    @JsonProperty("chunk_latency_avg_ms")
    private Double chunkLatencyAvgMs = 0.0;

    @JsonProperty("chunk_latency_max_ms")
    private Double chunkLatencyMaxMs = 0.0;

    @JsonProperty("chunk_transfer_count")
    private long chunkTransferCount = 0;

    @JsonProperty("staging_active_files")
    private int stagingActiveFiles = 0;

    @JsonProperty("staging_active_bytes")
    private long stagingActiveBytes = 0;

    @JsonProperty("staging_orphans_cleaned")
    private long stagingOrphansCleaned = 0;

    @JsonProperty("kafka_messages_replicated")
    private long kafkaMessagesReplicated = 0;

    @JsonProperty("kafka_bytes_replicated")
    private long kafkaBytesReplicated = 0;

    @JsonProperty("kafka_current_lag_ms")
    private long kafkaCurrentLagMs = 0;

    public AgentHeartbeatRequest() {}

    public AgentHeartbeatRequest(String agentId, String clusterId, String grpcAddress, int activeTransfers) {
        this(agentId, clusterId, grpcAddress, activeTransfers, null);
    }

    public AgentHeartbeatRequest(String agentId, String clusterId, String grpcAddress, int activeTransfers, String mode) {
        this.agentId = agentId;
        this.clusterId = clusterId;
        this.grpcAddress = grpcAddress;
        this.activeTransfers = activeTransfers;
        this.status = "online";
        this.mode = mode;
    }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

    public long getTransferredBytes() { return transferredBytes; }
    public void setTransferredBytes(long transferredBytes) { this.transferredBytes = transferredBytes; }

    public long getTransferErrors() { return transferErrors; }
    public void setTransferErrors(long transferErrors) { this.transferErrors = transferErrors; }

    public Double getBandwidthLimitMbS() { return bandwidthLimitMbS; }
    public void setBandwidthLimitMbS(Double bandwidthLimitMbS) { this.bandwidthLimitMbS = bandwidthLimitMbS; }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }

    public String getGrpcAddress() { return grpcAddress; }
    public void setGrpcAddress(String grpcAddress) { this.grpcAddress = grpcAddress; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public int getActiveTransfers() { return activeTransfers; }
    public void setActiveTransfers(int activeTransfers) { this.activeTransfers = activeTransfers; }

    public long getCompressionRawBytes() { return compressionRawBytes; }
    public void setCompressionRawBytes(long compressionRawBytes) { this.compressionRawBytes = compressionRawBytes; }

    public long getCompressionCompressedBytes() { return compressionCompressedBytes; }
    public void setCompressionCompressedBytes(long compressionCompressedBytes) { this.compressionCompressedBytes = compressionCompressedBytes; }

    public Double getCompressionRatio() { return compressionRatio; }
    public void setCompressionRatio(Double compressionRatio) { this.compressionRatio = compressionRatio; }

    public Double getChunkLatencyAvgMs() { return chunkLatencyAvgMs; }
    public void setChunkLatencyAvgMs(Double chunkLatencyAvgMs) { this.chunkLatencyAvgMs = chunkLatencyAvgMs; }

    public Double getChunkLatencyMaxMs() { return chunkLatencyMaxMs; }
    public void setChunkLatencyMaxMs(Double chunkLatencyMaxMs) { this.chunkLatencyMaxMs = chunkLatencyMaxMs; }

    public long getChunkTransferCount() { return chunkTransferCount; }
    public void setChunkTransferCount(long chunkTransferCount) { this.chunkTransferCount = chunkTransferCount; }

    public int getStagingActiveFiles() { return stagingActiveFiles; }
    public void setStagingActiveFiles(int stagingActiveFiles) { this.stagingActiveFiles = stagingActiveFiles; }

    public long getStagingActiveBytes() { return stagingActiveBytes; }
    public void setStagingActiveBytes(long stagingActiveBytes) { this.stagingActiveBytes = stagingActiveBytes; }

    public long getStagingOrphansCleaned() { return stagingOrphansCleaned; }
    public void setStagingOrphansCleaned(long stagingOrphansCleaned) { this.stagingOrphansCleaned = stagingOrphansCleaned; }

    public long getKafkaMessagesReplicated() { return kafkaMessagesReplicated; }
    public void setKafkaMessagesReplicated(long kafkaMessagesReplicated) { this.kafkaMessagesReplicated = kafkaMessagesReplicated; }

    public long getKafkaBytesReplicated() { return kafkaBytesReplicated; }
    public void setKafkaBytesReplicated(long kafkaBytesReplicated) { this.kafkaBytesReplicated = kafkaBytesReplicated; }

    public long getKafkaCurrentLagMs() { return kafkaCurrentLagMs; }
    public void setKafkaCurrentLagMs(long kafkaCurrentLagMs) { this.kafkaCurrentLagMs = kafkaCurrentLagMs; }
}
