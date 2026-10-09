package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface HmsEventLogRepository extends JpaRepository<HmsEventLogEntity, String> {
    List<HmsEventLogEntity> findByHmsJobIdOrderByCreatedAtDesc(String hmsJobId, Pageable pageable);
    List<HmsEventLogEntity> findByHmsJobIdAndStatus(String hmsJobId, String status);
    long countByHmsJobId(String hmsJobId);
    void deleteByHmsJobId(String hmsJobId);
}
