package io.github.balookrd.pristan.replicator.orchestrator.repository;

import io.github.balookrd.pristan.replicator.orchestrator.entity.AgentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AgentRepository extends JpaRepository<AgentEntity, String> {
    List<AgentEntity> findByClusterId(String clusterId);
    List<AgentEntity> findByStatus(String status);
}
