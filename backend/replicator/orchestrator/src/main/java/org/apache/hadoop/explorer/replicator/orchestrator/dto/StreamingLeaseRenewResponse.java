package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import java.time.Instant;

/**
 * Ответ оркестратора на запрос продления аренды Active-Standby стримера.
 */
public record StreamingLeaseRenewResponse(
    String status,              // "ACTIVE", "STANDBY", "DISABLED"
    long epoch,
    String activeAgentId,
    Instant leaseExpiresAt,
    int registeredStreamersCount,
    boolean redundancyWarning,  // true, если активных стримеров в кластере < 2
    long lastCommittedTxid
) {}
