package org.apache.hadoop.explorer.sql.repository;

import org.apache.hadoop.explorer.sql.model.QueryHistory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface QueryHistoryRepository extends JpaRepository<QueryHistory, String> {

    List<QueryHistory> findByUsernameOrderByCreatedAtDesc(String username, Pageable pageable);

    List<QueryHistory> findByUsernameAndInQueueTrueOrderByCreatedAtDesc(String username, Pageable pageable);

    Optional<QueryHistory> findByIdAndUsername(String id, String username);
}
