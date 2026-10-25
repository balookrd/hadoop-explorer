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

    /**
     * Распределение размеров файлов одним агрегатом: количество, сумма и границы гистограммы.
     *
     * <p>Рендер метрик вычитывал таблицу подзадач целиком — на миллионах пофайловых записей
     * один скрейп Prometheus означал полную выборку в heap.
     */
    @Query("SELECT count(t), coalesce(sum(t.fileSize), 0), "
            + "sum(case when t.fileSize <= 1048576 then 1 else 0 end), "
            + "sum(case when t.fileSize <= 10485760 then 1 else 0 end), "
            + "sum(case when t.fileSize <= 104857600 then 1 else 0 end), "
            + "sum(case when t.fileSize <= 1073741824 then 1 else 0 end), "
            + "sum(case when t.fileSize <= 10737418240 then 1 else 0 end) "
            + "FROM TaskEntity t")
    List<Object[]> aggregateFileSizeHistogram();
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
        WHERE t.status = :status
          AND t.jobId IN (
              SELECT j.id FROM JobEntity j
              WHERE (j.status = 'RUNNING' OR j.status = 'STREAMING')
                AND (:clusterId IS NULL OR LOWER(j.sourceClusterId) = LOWER(:clusterId))
          )
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

    @Modifying
    @Query("DELETE FROM TaskEntity t WHERE t.jobId = :jobId")
    void deleteByJobId(@Param("jobId") String jobId);

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

    /**
     * Идентификаторы завершенных задач старше указанного момента — порция для очистки истории.
     *
     * <p>Возвращаются только идентификаторы: в записи задачи лежит манифест бандла (TEXT), и
     * выбирать сущности целиком ради удаления означало бы тянуть его в heap пачками по сотням.
     * Порядок по времени обновления делает очистку предсказуемой: сначала самое старое.
     */
    @Query("SELECT t.id FROM TaskEntity t WHERE t.status IN :statuses AND t.updatedAt < :before ORDER BY t.updatedAt ASC")
    List<String> findExpiredTaskIds(@Param("statuses") Collection<String> statuses,
                                    @Param("before") Instant before,
                                    Pageable pageable);
}
