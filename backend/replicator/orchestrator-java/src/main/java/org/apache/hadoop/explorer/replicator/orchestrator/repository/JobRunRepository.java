package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobRunEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRunRepository extends JpaRepository<JobRunEntity, String> {
    List<JobRunEntity> findByJobIdOrderByRunNumberDesc(String jobId);
    int countByJobId(String jobId);
    void deleteByJobId(String jobId);
}
