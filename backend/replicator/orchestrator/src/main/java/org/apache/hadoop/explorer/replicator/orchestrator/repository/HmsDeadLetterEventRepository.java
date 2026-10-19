package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsDeadLetterEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface HmsDeadLetterEventRepository extends JpaRepository<HmsDeadLetterEventEntity, String> {

    List<HmsDeadLetterEventEntity> findByHmsJobIdOrderByFailedAtDesc(String hmsJobId, Pageable pageable);

    List<HmsDeadLetterEventEntity> findByHmsJobIdAndStatusOrderByFailedAtDesc(String hmsJobId, String status, Pageable pageable);

    Optional<HmsDeadLetterEventEntity> findByHmsJobIdAndEventId(String hmsJobId, Long eventId);

    long countByHmsJobIdAndStatus(String hmsJobId, String status);

    void deleteByHmsJobId(String hmsJobId);
}
