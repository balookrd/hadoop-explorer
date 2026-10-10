package org.apache.hadoop.explorer.replicator.orchestrator.dto;

public record StreamingLeaseRenewRequest(
    String clusterId,
    String agentId
) {}
