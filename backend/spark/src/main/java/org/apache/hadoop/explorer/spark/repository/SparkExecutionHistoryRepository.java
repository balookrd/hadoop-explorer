package org.apache.hadoop.explorer.spark.repository;

import org.apache.hadoop.explorer.spark.model.SparkExecutionHistory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface SparkExecutionHistoryRepository extends JpaRepository<SparkExecutionHistory, String> {

    List<SparkExecutionHistory> findByUsernameOrderByCreatedAtDesc(String username, Pageable pageable);

    Optional<SparkExecutionHistory> findByIdAndUsername(String id, String username);

    /** Cancels only a QUEUED or RUNNING execution; a terminal status written meanwhile is kept. Returns rows changed. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("UPDATE SparkExecutionHistory h SET h.status = 'CANCELLED', h.finishedAt = :finishedAt "
            + "WHERE h.id = :id AND h.status IN ('QUEUED', 'RUNNING')")
    int markCancelledIfActive(@Param("id") String id, @Param("finishedAt") Instant finishedAt);

    /** Moves a QUEUED execution to RUNNING; a cancel written meanwhile is kept. Returns rows changed. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("UPDATE SparkExecutionHistory h SET h.status = 'RUNNING', h.startedAt = :startedAt "
            + "WHERE h.id = :id AND h.status = 'QUEUED'")
    int markRunningIfQueued(@Param("id") String id, @Param("startedAt") Instant startedAt);

    /**
     * Writes the terminal outcome only while the execution is still QUEUED or RUNNING, so a concurrent cancel
     * is never overwritten (last write does not win). Returns rows changed: 0 means another terminal status won.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("UPDATE SparkExecutionHistory h SET h.status = :status, h.rowsCount = :rowsCount, "
            + "h.executionTimeMs = :executionTimeMs, h.columnsJson = :columnsJson, h.logs = :logs, "
            + "h.errorMessage = :errorMessage, h.hasCachedResult = :hasCachedResult, h.finishedAt = :finishedAt "
            + "WHERE h.id = :id AND h.status IN ('QUEUED', 'RUNNING')")
    int completeIfActive(@Param("id") String id,
                         @Param("status") String status,
                         @Param("rowsCount") long rowsCount,
                         @Param("executionTimeMs") double executionTimeMs,
                         @Param("columnsJson") String columnsJson,
                         @Param("logs") String logs,
                         @Param("errorMessage") String errorMessage,
                         @Param("hasCachedResult") boolean hasCachedResult,
                         @Param("finishedAt") Instant finishedAt);
}
