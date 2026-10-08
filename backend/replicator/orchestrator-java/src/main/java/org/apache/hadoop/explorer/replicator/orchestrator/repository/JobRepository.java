package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface JobRepository extends JpaRepository<JobEntity, String> {
    List<JobEntity> findByStatus(String status);
    List<JobEntity> findByIsScheduledTrueAndNextRunAtLessThanEqual(Instant now);
}
