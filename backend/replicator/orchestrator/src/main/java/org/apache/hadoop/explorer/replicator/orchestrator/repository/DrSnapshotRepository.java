package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.DrSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DrSnapshotRepository extends JpaRepository<DrSnapshotEntity, String> {
    Optional<DrSnapshotEntity> findByClusterIdIgnoreCase(String clusterId);
}
