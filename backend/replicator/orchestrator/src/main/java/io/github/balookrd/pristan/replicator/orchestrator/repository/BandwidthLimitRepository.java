package io.github.balookrd.pristan.replicator.orchestrator.repository;

import io.github.balookrd.pristan.replicator.orchestrator.entity.BandwidthLimitEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BandwidthLimitRepository extends JpaRepository<BandwidthLimitEntity, String> {
}
