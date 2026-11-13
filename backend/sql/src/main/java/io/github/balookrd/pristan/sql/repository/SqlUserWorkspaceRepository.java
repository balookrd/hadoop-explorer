package io.github.balookrd.pristan.sql.repository;

import io.github.balookrd.pristan.sql.model.SqlUserWorkspace;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SqlUserWorkspaceRepository extends JpaRepository<SqlUserWorkspace, String> {
}
