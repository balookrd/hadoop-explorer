package io.github.balookrd.pristan.replicator.orchestrator.dto;

public record StreamingLeaseRenewRequest(
    String clusterId,
    String agentId
) {}
