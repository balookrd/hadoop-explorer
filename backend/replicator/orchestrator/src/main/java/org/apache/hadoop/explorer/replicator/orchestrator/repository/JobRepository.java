package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface JobRepository extends JpaRepository<JobEntity, String>,
        org.springframework.data.jpa.repository.JpaSpecificationExecutor<JobEntity> {
    List<JobEntity> findByStatus(String status);
    List<JobEntity> findByJobType(String jobType);
    List<JobEntity> findByJobTypeAndStatus(String jobType, String status);
    List<JobEntity> findByParentJobId(String parentJobId);
    List<JobEntity> findByParentJobIdIn(java.util.Collection<String> parentJobIds);
    List<JobEntity> findByParentJobIdAndSourcePathAndTargetPath(String parentJobId, String sourcePath, String targetPath);

    @Query("""
        SELECT j FROM JobEntity j
        WHERE j.isScheduled = true
          AND j.nextRunAt IS NOT NULL
          AND j.nextRunAt <= :now
    """)
    List<JobEntity> findByIsScheduledTrueAndNextRunAtLessThanEqual(@Param("now") Instant now);
    long countByStatus(String status);
    long countByStatusAndParentJobIdIsNull(String status);

    @Query("""
        SELECT COALESCE(MAX(j.lastProcessedTxid), 0) FROM JobEntity j
        WHERE j.syncMode = 'STREAMING_INOTIFY'
          AND LOWER(j.sourceClusterId) = LOWER(:clusterId)
    """)
    long findMaxProcessedTxidByClusterId(@Param("clusterId") String clusterId);

    @Query("""
        SELECT j FROM JobEntity j
        WHERE LOWER(REPLACE(REPLACE(j.sourceClusterId, '-', ''), '_', '')) = LOWER(REPLACE(REPLACE(:clusterId, '-', ''), '_', ''))
    """)
    List<JobEntity> findByNormalizedSourceClusterId(@Param("clusterId") String clusterId);


    /**
     * Идентификаторы заданий без истории запусков — порция для дозаполнения при старте.
     *
     * <p>Прежний проход читал все задания и для каждого спрашивал количество запусков
     * отдельным запросом: на большой таблице это полная выборка плюс N+1.
     */
    @Query("SELECT j.id FROM JobEntity j WHERE NOT EXISTS "
            + "(SELECT 1 FROM JobRunEntity r WHERE r.jobId = j.id)")
    List<String> findIdsWithoutRuns(org.springframework.data.domain.Pageable pageable);

    /**
     * Сумма байт по статусу для корневых заданий — одним агрегатом.
     *
     * <p>Рендер метрик вычитывал таблицу заданий целиком трижды за скрейп (по статусу),
     * и так на каждом интервале Prometheus.
     */
    @Query("SELECT coalesce(sum(j.totalBytes), 0) FROM JobEntity j "
            + "WHERE j.parentJobId IS NULL AND lower(j.status) = lower(:status)")
    long sumTotalBytesByStatus(@Param("status") String status);

    @Query("SELECT coalesce(sum(j.copiedBytes), 0) FROM JobEntity j "
            + "WHERE j.parentJobId IS NULL AND lower(j.status) = lower(:status)")
    long sumCopiedBytesByStatus(@Param("status") String status);

    @Query("SELECT count(j) FROM JobEntity j WHERE j.parentJobId IS NULL AND j.isScheduled = :scheduled")
    long countByScheduledFlag(@Param("scheduled") boolean scheduled);

    /**
     * Количество заданий обратной репликации по статусу: обратные отличаются префиксом пути.
     */
    @Query("SELECT count(j) FROM JobEntity j "
            + "WHERE lower(j.status) = lower(:status) AND j.sourcePath LIKE concat(:pathPrefix, '%')")
    long countByStatusAndSourcePathPrefix(@Param("status") String status,
                                          @Param("pathPrefix") String pathPrefix);

    @org.springframework.data.jpa.repository.Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
        UPDATE JobEntity j
        SET j.copiedBytes = j.copiedBytes + :bytes,
            j.transferredObjects = j.transferredObjects + :files,
            j.updatedAt = :now
        WHERE j.id = :jobId
    """)
    int incrementProgress(@Param("jobId") String jobId, @Param("bytes") long bytes, @Param("files") int files, @Param("now") Instant now);

    @org.springframework.data.jpa.repository.Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
        UPDATE JobEntity j
        SET j.status = 'ANALYZING',
            j.updatedAt = :now,
            j.message = :message
        WHERE j.id = :jobId
          AND j.status = 'QUEUED'
    """)
    int claimJobForAnalysis(@Param("jobId") String jobId, @Param("message") String message, @Param("now") Instant now);
}
