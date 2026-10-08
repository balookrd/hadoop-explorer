package org.apache.hadoop.explorer.sql.repository;

import org.apache.hadoop.explorer.sql.model.SavedQuery;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SavedQueryRepository extends JpaRepository<SavedQuery, String> {

    @Query("SELECT s FROM SavedQuery s WHERE s.username = :username OR s.shared = true ORDER BY s.updatedAt DESC")
    List<SavedQuery> findVisibleQueries(@Param("username") String username);
}
