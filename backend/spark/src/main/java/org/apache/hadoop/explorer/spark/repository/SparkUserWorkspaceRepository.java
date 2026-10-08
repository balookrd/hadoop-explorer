package org.apache.hadoop.explorer.spark.repository;

import org.apache.hadoop.explorer.spark.model.SparkUserWorkspace;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SparkUserWorkspaceRepository extends JpaRepository<SparkUserWorkspace, String> {
}
