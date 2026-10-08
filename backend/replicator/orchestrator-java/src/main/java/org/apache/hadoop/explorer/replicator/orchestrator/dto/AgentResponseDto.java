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
    @JsonProperty("last_heartbeat") String lastHeartbeat
) {}
