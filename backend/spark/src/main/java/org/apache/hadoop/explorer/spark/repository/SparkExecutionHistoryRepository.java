package org.apache.hadoop.explorer.spark.repository;

import org.apache.hadoop.explorer.spark.model.SparkExecutionHistory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SparkExecutionHistoryRepository extends JpaRepository<SparkExecutionHistory, String> {

    List<SparkExecutionHistory> findByUsernameOrderByCreatedAtDesc(String username, Pageable pageable);

    Optional<SparkExecutionHistory> findByIdAndUsername(String id, String username);
}
