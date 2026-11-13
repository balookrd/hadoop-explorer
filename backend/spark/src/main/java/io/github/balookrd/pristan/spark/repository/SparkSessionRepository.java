package io.github.balookrd.pristan.spark.repository;

import io.github.balookrd.pristan.spark.model.SparkSessionRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
public interface SparkSessionRepository extends JpaRepository<SparkSessionRecord, String> {

    List<SparkSessionRecord> findByUsernameAndStatusNotInOrderByCreatedAtDesc(String username, List<String> statuses);

    List<SparkSessionRecord> findByUsernameOrderByCreatedAtDesc(String username);

    Optional<SparkSessionRecord> findByIdAndUsername(String id, String username);

    Optional<SparkSessionRecord> findFirstByUsernameAndClusterIdAndStatusInOrderByLastActivityAtDesc(
            String username, String clusterId, Collection<String> statuses);

    List<SparkSessionRecord> findByClusterIdAndStatusNotIn(String clusterId, Collection<String> statuses);

    List<SparkSessionRecord> findByStatusAndLastActivityAtBefore(String status, Instant threshold);

    /** Stop: claims the session unless it is already dead or killed. Returns rows changed. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("UPDATE SparkSessionRecord s SET s.status = 'killed', s.stoppedAt = :now "
            + "WHERE s.id = :id AND s.status NOT IN ('dead', 'killed')")
    int markKilledIfActive(@Param("id") String id, @Param("now") Instant now);

    /**
     * Idle TTL: claims the session only if it is still idle and inactive since before the threshold, so a
     * statement started meanwhile (busy, newer lastActivityAt) keeps its session. Returns rows changed.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("UPDATE SparkSessionRecord s SET s.status = 'killed', s.stoppedAt = :now "
            + "WHERE s.id = :id AND s.status = 'idle' AND s.lastActivityAt < :threshold")
    int markKilledIfIdleBefore(@Param("id") String id, @Param("threshold") Instant threshold, @Param("now") Instant now);

    /**
     * Returns a session to idle after a statement, unless it was stopped or found dead meanwhile:
     * a stop (killed) or a dead Livy session must not come back to life. Returns rows changed.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("UPDATE SparkSessionRecord s SET s.status = 'idle', s.lastActivityAt = :now "
            + "WHERE s.id = :id AND s.status NOT IN ('dead', 'killed')")
    int markIdleIfActive(@Param("id") String id, @Param("now") Instant now);

    /**
     * Writes the state and YARN application id read from Livy, unless the session was stopped or found dead
     * meanwhile. stoppedAt is set for a terminal state. Returns rows changed.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("UPDATE SparkSessionRecord s SET s.status = :status, "
            + "s.yarnApplicationId = :appId, s.stoppedAt = :stoppedAt "
            + "WHERE s.id = :id AND s.status NOT IN ('dead', 'killed')")
    int updateStateIfActive(@Param("id") String id, @Param("status") String status, @Param("appId") String appId,
                            @Param("stoppedAt") Instant stoppedAt);

    /** Number of sessions per cluster and kind, for the spark_sessions_active gauge. */
    interface ActiveSessionCount {
        String getClusterId();

        String getKind();

        Long getSessions();
    }

    @Query("select s.clusterId as clusterId, s.kind as kind, count(s) as sessions "
            + "from SparkSessionRecord s where s.status not in :statuses group by s.clusterId, s.kind")
    List<ActiveSessionCount> countByClusterAndKindExcludingStatuses(@Param("statuses") List<String> statuses);
}
