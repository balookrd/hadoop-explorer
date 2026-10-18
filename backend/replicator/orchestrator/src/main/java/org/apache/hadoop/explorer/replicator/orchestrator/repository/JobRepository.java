package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface JobRepository extends JpaRepository<JobEntity, String> {
    List<JobEntity> findByStatus(String status);
    List<JobEntity> findByJobType(String jobType);
    List<JobEntity> findByJobTypeAndStatus(String jobType, String status);
    List<JobEntity> findByParentJobId(String parentJobId);
    List<JobEntity> findByParentJobIdIn(java.util.Collection<String> parentJobIds);
    List<JobEntity> findByIsScheduledTrueAndNextRunAtLessThanEqual(Instant now);
    long countByStatus(String status);
    long countByStatusAndParentJobIdIsNull(String status);

    @Query("""
        SELECT COALESCE(MAX(j.lastProcessedTxid), 0) FROM JobEntity j
        WHERE j.syncMode = 'STREAMING_INOTIFY'
          AND LOWER(j.sourceClusterId) = LOWER(:clusterId)
    """)
    long findMaxProcessedTxidByClusterId(@Param("clusterId") String clusterId);

    @org.springframework.data.jpa.repository.Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
        UPDATE JobEntity j
        SET j.copiedBytes = j.copiedBytes + :bytes,
            j.transferredObjects = j.transferredObjects + :files,
            j.updatedAt = :now
        WHERE j.id = :jobId
    """)
    int incrementProgress(@Param("jobId") String jobId, @Param("bytes") long bytes, @Param("files") int files, @Param("now") Instant now);
}
