package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
public interface HmsReplicationJobRepository extends JpaRepository<HmsReplicationJobEntity, String> {
    List<HmsReplicationJobEntity> findByStatus(String status);
    List<HmsReplicationJobEntity> findByStatusIn(Collection<String> statuses);
    List<HmsReplicationJobEntity> findByAssignedAgentIdIsNotNullAndStatusIn(Collection<String> statuses);
    long countByStatus(String status);
    Optional<HmsReplicationJobEntity> findBySourceClusterIdAndSourceDbName(String sourceClusterId, String sourceDbName);

    @Query("""
        SELECT h FROM HmsReplicationJobEntity h
        WHERE LOWER(REPLACE(REPLACE(h.sourceClusterId, '-', ''), '_', '')) = LOWER(REPLACE(REPLACE(:clusterId, '-', ''), '_', ''))
    """)
    List<HmsReplicationJobEntity> findByNormalizedSourceClusterId(@Param("clusterId") String clusterId);
}
