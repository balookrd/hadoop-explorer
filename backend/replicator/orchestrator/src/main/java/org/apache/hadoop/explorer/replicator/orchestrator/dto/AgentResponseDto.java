package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AgentResponseDto(
    @JsonProperty("agent_id") String agentId,
    @JsonProperty("cluster_id") String clusterId,
    @JsonProperty("grpc_address") String grpcAddress,
    @JsonProperty("status") String status, // "online", "stale", "offline" (строго нижний регистр для UI)
    @JsonProperty("mode") String mode, // "all", "sender", "receiver"
    @JsonProperty("active_transfers") int activeTransfers,
    @JsonProperty("max_bandwidth_mb_s") Double maxBandwidthMbS,
    @JsonProperty("heartbeat_age_seconds") long heartbeatAgeSeconds,
    @JsonProperty("last_heartbeat") String lastHeartbeat,
    @JsonProperty("transferred_bytes") long transferredBytes,
    @JsonProperty("transfer_errors") long transferErrors,
    @JsonProperty("compression_raw_bytes") long compressionRawBytes,
    @JsonProperty("compression_compressed_bytes") long compressionCompressedBytes,
    @JsonProperty("compression_ratio") double compressionRatio,
    @JsonProperty("chunk_latency_avg_ms") double chunkLatencyAvgMs,
    @JsonProperty("chunk_latency_max_ms") double chunkLatencyMaxMs,
    @JsonProperty("chunk_transfer_count") long chunkTransferCount,
    @JsonProperty("staging_active_files") int stagingActiveFiles,
    @JsonProperty("staging_active_bytes") long stagingActiveBytes,
    @JsonProperty("staging_orphans_cleaned") long stagingOrphansCleaned
) {
    public AgentResponseDto(String agentId, String clusterId, String grpcAddress, String status,
                            String mode, int activeTransfers, Double maxBandwidthMbS,
                            long heartbeatAgeSeconds, String lastHeartbeat,
                            long transferredBytes, long transferErrors) {
        this(agentId, clusterId, grpcAddress, status, mode, activeTransfers, maxBandwidthMbS,
             heartbeatAgeSeconds, lastHeartbeat, transferredBytes, transferErrors,
             0L, 0L, 1.0, 0.0, 0.0, 0L, 0, 0L, 0L);
    }

    public AgentResponseDto(String agentId, String clusterId, String grpcAddress, String status,
                            String mode, int activeTransfers, Double maxBandwidthMbS,
                            long heartbeatAgeSeconds, String lastHeartbeat) {
        this(agentId, clusterId, grpcAddress, status, mode, activeTransfers, maxBandwidthMbS,
             heartbeatAgeSeconds, lastHeartbeat, 0L, 0L);
    }
}
