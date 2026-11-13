package io.github.balookrd.pristan.parom.orchestrator.dto;

public record StreamingLeaseRenewRequest(
    String clusterId,
    String agentId
) {}
