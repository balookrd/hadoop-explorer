package org.apache.hadoop.explorer.spark.repository;

import org.apache.hadoop.explorer.spark.model.SparkSessionRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SparkSessionRepository extends JpaRepository<SparkSessionRecord, String> {

    List<SparkSessionRecord> findByUsernameAndStatusNotInOrderByCreatedAtDesc(String username, List<String> statuses);

    List<SparkSessionRecord> findByUsernameOrderByCreatedAtDesc(String username);

    Optional<SparkSessionRecord> findByIdAndUsername(String id, String username);
}
