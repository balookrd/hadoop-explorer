package org.apache.hadoop.explorer.yarn.repository;

import org.apache.hadoop.explorer.yarn.entity.ChangeRequestEntity;
import org.apache.hadoop.explorer.yarn.entity.ChangeRequestStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ChangeRequestRepository extends JpaRepository<ChangeRequestEntity, Long> {

    List<ChangeRequestEntity> findAllByOrderByCreatedAtDesc();

    List<ChangeRequestEntity> findByClusterIdOrderByCreatedAtDesc(String clusterId);

    List<ChangeRequestEntity> findByStatusOrderByCreatedAtDesc(ChangeRequestStatus status);

    List<ChangeRequestEntity> findByClusterIdAndStatusOrderByCreatedAtDesc(String clusterId, ChangeRequestStatus status);

    long countByStatus(ChangeRequestStatus status);

    long countByClusterIdAndStatus(String clusterId, ChangeRequestStatus status);
}
