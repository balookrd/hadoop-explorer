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
    @JsonProperty("transfer_errors") long transferErrors
) {
    public AgentResponseDto(String agentId, String clusterId, String grpcAddress, String status,
                            String mode, int activeTransfers, Double maxBandwidthMbS,
                            long heartbeatAgeSeconds, String lastHeartbeat) {
        this(agentId, clusterId, grpcAddress, status, mode, activeTransfers, maxBandwidthMbS, heartbeatAgeSeconds, lastHeartbeat, 0L, 0L);
    }
}
