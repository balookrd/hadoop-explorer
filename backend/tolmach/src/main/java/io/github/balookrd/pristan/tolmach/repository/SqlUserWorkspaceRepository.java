package io.github.balookrd.pristan.tolmach.repository;

import io.github.balookrd.pristan.tolmach.model.SqlUserWorkspace;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SqlUserWorkspaceRepository extends JpaRepository<SqlUserWorkspace, String> {
}
