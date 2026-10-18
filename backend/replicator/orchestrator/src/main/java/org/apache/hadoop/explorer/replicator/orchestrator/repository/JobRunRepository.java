package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobRunEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;

@Repository
public interface JobRunRepository extends JpaRepository<JobRunEntity, String> {
    List<JobRunEntity> findByJobIdOrderByRunNumberDesc(String jobId);
    int countByJobId(String jobId);
    void deleteByJobId(String jobId);

    @Query("SELECT COALESCE(MAX(r.runNumber), 0) FROM JobRunEntity r WHERE r.jobId = :jobId")
    int findMaxRunNumberByJobId(@Param("jobId") String jobId);

    interface JobRunCount {
        String getJobId();
        long getRunCount();
    }

    @Query("SELECT r.jobId AS jobId, COUNT(r) AS runCount FROM JobRunEntity r WHERE r.jobId IN :jobIds GROUP BY r.jobId")
    List<JobRunCount> countByJobIdIn(@Param("jobIds") Collection<String> jobIds);
}
