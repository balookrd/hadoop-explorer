package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TaskRepository extends JpaRepository<TaskEntity, String> {
    List<TaskEntity> findByJobId(String jobId);
    List<TaskEntity> findByJobIdAndStatus(String jobId, String status);
}
