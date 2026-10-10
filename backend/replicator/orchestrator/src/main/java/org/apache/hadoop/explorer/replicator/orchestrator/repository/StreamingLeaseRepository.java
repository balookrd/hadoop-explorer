package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.StreamingLeaseEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
public interface StreamingLeaseRepository extends JpaRepository<StreamingLeaseEntity, String> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE StreamingLeaseEntity l SET " +
           "l.activeAgentId = :agentId, " +
           "l.expiresAt = :expiresAt, " +
           "l.lastRenewedAt = :now, " +
           "l.epoch = CASE WHEN l.activeAgentId = :agentId THEN l.epoch ELSE (l.epoch + 1) END " +
           "WHERE l.clusterId = :clusterId AND (l.expiresAt < :now OR l.activeAgentId = :agentId)")
    int tryAcquireOrRenewLease(
        @Param("clusterId") String clusterId,
        @Param("agentId") String agentId,
        @Param("expiresAt") Instant expiresAt,
        @Param("now") Instant now
    );
}
