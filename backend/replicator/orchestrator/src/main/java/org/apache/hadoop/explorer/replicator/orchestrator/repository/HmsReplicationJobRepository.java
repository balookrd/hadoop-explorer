package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface HmsReplicationJobRepository extends JpaRepository<HmsReplicationJobEntity, String> {
    List<HmsReplicationJobEntity> findByStatus(String status);
    Optional<HmsReplicationJobEntity> findBySourceClusterIdAndSourceDbName(String sourceClusterId, String sourceDbName);
}
