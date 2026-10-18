package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

@Repository
public interface TaskRepository extends JpaRepository<TaskEntity, String> {
    List<TaskEntity> findByJobId(String jobId);
    org.springframework.data.domain.Page<TaskEntity> findByJobId(String jobId, Pageable pageable);
    List<TaskEntity> findByJobIdAndStatus(String jobId, String status);
    long countByJobId(String jobId);
    long countByJobIdAndStatus(String jobId, String status);
    long countByJobIdAndStatusIn(String jobId, Collection<String> statuses);
    boolean existsByJobIdAndStatusIn(String jobId, Collection<String> statuses);
    List<TaskEntity> findByStatus(String status);
    List<TaskEntity> findByStatus(String status, Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")})
    @Query("SELECT t FROM TaskEntity t WHERE t.status = :status ORDER BY t.updatedAt ASC")
    List<TaskEntity> findByStatusForUpdateSkipLocked(@Param("status") String status, Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")})
    @Query("""
        SELECT t FROM TaskEntity t
        JOIN t.job j
        WHERE t.status = :status
          AND j.status = 'RUNNING'
          AND (:clusterId IS NULL OR LOWER(j.sourceClusterId) = LOWER(:clusterId))
        ORDER BY t.updatedAt ASC
    """)
    List<TaskEntity> findPendingTasksForClusterSkipLocked(
        @Param("status") String status,
        @Param("clusterId") String clusterId,
        Pageable pageable
    );

    List<TaskEntity> findByStatusAndUpdatedAtBefore(String status, Instant cutoff, Pageable pageable);
    List<TaskEntity> findByStatusAndAssignedAgentIdIsNull(String status, Pageable pageable);

    long countByStatus(String status);
    List<TaskEntity> findByAssignedAgentIdAndStatus(String assignedAgentId, String status);
    void deleteByJobId(String jobId);

    @Query("""
        SELECT COUNT(t) FROM TaskEntity t
        WHERE t.status = :status
          AND t.jobId IN (
              SELECT j.id FROM JobEntity j
              WHERE LOWER(j.sourceClusterId) = LOWER(:clusterId)
                 OR LOWER(j.targetClusterId) = LOWER(:clusterId)
          )
    """)
    long countPendingTasksByClusterId(@Param("clusterId") String clusterId, @Param("status") String status);

    @Modifying
    @Query("DELETE FROM TaskEntity t WHERE t.status IN :statuses AND t.updatedAt < :before")
    int deleteByStatusInAndUpdatedAtBefore(@Param("statuses") Collection<String> statuses, @Param("before") Instant before);
}
