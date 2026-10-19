package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.OrchestratorNodeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface OrchestratorNodeRepository extends JpaRepository<OrchestratorNodeEntity, String> {

    @Query("SELECT COUNT(n) FROM OrchestratorNodeEntity n WHERE n.lastHeartbeat >= :threshold AND n.status = 'ACTIVE'")
    long countActiveNodes(@Param("threshold") Instant threshold);

    List<OrchestratorNodeEntity> findByLastHeartbeatGreaterThanEqualAndStatus(Instant threshold, String status);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("DELETE FROM OrchestratorNodeEntity n WHERE n.lastHeartbeat < :staleThreshold")
    int cleanStaleNodes(@Param("staleThreshold") Instant staleThreshold);
}
