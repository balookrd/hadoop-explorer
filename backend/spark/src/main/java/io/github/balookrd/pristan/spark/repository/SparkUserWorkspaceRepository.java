package io.github.balookrd.pristan.spark.repository;

import io.github.balookrd.pristan.spark.model.SparkUserWorkspace;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SparkUserWorkspaceRepository extends JpaRepository<SparkUserWorkspace, String> {
}
