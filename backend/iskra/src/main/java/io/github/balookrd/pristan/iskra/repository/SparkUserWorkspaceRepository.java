package io.github.balookrd.pristan.iskra.repository;

import io.github.balookrd.pristan.iskra.model.SparkUserWorkspace;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SparkUserWorkspaceRepository extends JpaRepository<SparkUserWorkspace, String> {
}
