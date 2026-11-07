package org.apache.hadoop.explorer.spark.repository;

import org.apache.hadoop.explorer.spark.model.SparkSessionRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SparkSessionRepository extends JpaRepository<SparkSessionRecord, String> {

    List<SparkSessionRecord> findByUsernameAndStatusNotInOrderByCreatedAtDesc(String username, List<String> statuses);

    List<SparkSessionRecord> findByUsernameOrderByCreatedAtDesc(String username);

    Optional<SparkSessionRecord> findByIdAndUsername(String id, String username);

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
