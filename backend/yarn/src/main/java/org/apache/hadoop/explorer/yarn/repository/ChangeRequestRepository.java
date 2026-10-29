package org.apache.hadoop.explorer.yarn.repository;

import org.apache.hadoop.explorer.yarn.entity.ChangeRequestEntity;
import org.apache.hadoop.explorer.yarn.entity.ChangeRequestStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface ChangeRequestRepository extends JpaRepository<ChangeRequestEntity, Long> {

    List<ChangeRequestEntity> findAllByOrderByCreatedAtDesc();

    List<ChangeRequestEntity> findByClusterIdOrderByCreatedAtDesc(String clusterId);

    List<ChangeRequestEntity> findByStatusOrderByCreatedAtDesc(ChangeRequestStatus status);

    List<ChangeRequestEntity> findByClusterIdAndStatusOrderByCreatedAtDesc(String clusterId, ChangeRequestStatus status);

    long countByStatus(ChangeRequestStatus status);

    long countByClusterIdAndStatus(String clusterId, ChangeRequestStatus status);

    /**
     * Atomically marks an approved request as being deployed. Returns 0 when it is not approved,
     * is already being deployed or was deployed successfully, so two clicks never run two jobs.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("update ChangeRequestEntity e set e.deploymentStatus = 'DEPLOYING', e.deploymentError = null, e.updatedAt = :now "
            + "where e.id = :id and e.status = :approved "
            + "and (e.deploymentStatus is null or e.deploymentStatus = 'FAILED')")
    int claimForDeployment(@Param("id") Long id, @Param("approved") ChangeRequestStatus approved, @Param("now") Instant now);
}
