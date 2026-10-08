package org.apache.hadoop.explorer.sql.repository;

import org.apache.hadoop.explorer.sql.model.SqlUserWorkspace;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SqlUserWorkspaceRepository extends JpaRepository<SqlUserWorkspace, String> {
}
