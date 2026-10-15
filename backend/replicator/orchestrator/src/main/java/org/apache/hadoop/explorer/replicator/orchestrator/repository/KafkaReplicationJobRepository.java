package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface KafkaReplicationJobRepository extends JpaRepository<KafkaReplicationJobEntity, String> {

    List<KafkaReplicationJobEntity> findByStatus(String status);

    List<KafkaReplicationJobEntity> findBySourceDcAndTargetDc(String sourceDc, String targetDc);
}
