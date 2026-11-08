package org.apache.hadoop.explorer.sql.repository;

import org.apache.hadoop.explorer.sql.model.QueryHistory;
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
public interface QueryHistoryRepository extends JpaRepository<QueryHistory, String> {

    List<QueryHistory> findByUsernameOrderByCreatedAtDesc(String username, Pageable pageable);

    List<QueryHistory> findByUsernameAndInQueueTrueOrderByCreatedAtDesc(String username, Pageable pageable);

    Optional<QueryHistory> findByIdAndUsername(String id, String username);

    /**
     * Marks the query CANCELLED only while it is still QUEUED or RUNNING, so a concurrent
     * FINISHED/FAILED write by the query thread is never overwritten. Returns the number of updated rows.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("UPDATE QueryHistory q SET q.status = 'CANCELLED', q.inQueue = false, q.finishedAt = :finishedAt "
            + "WHERE q.id = :id AND q.status IN ('QUEUED', 'RUNNING')")
    int markCancelledIfActive(@Param("id") String id, @Param("finishedAt") Instant finishedAt);
}
