package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface HmsEventLogRepository extends JpaRepository<HmsEventLogEntity, String> {
    List<HmsEventLogEntity> findByHmsJobId(String hmsJobId);
    List<HmsEventLogEntity> findByHmsJobIdOrderByCreatedAtDesc(String hmsJobId, Pageable pageable);
    List<HmsEventLogEntity> findByHmsJobIdAndStatus(String hmsJobId, String status);
    long countByHmsJobId(String hmsJobId);
    // Пакетный DELETE: производный deleteBy грузит в память каждую запись журнала (их сотни
    // тысяч у задачи с большой таблицей) и удаляет по одной
    @org.springframework.transaction.annotation.Transactional
    @org.springframework.data.jpa.repository.Modifying
    @org.springframework.data.jpa.repository.Query("DELETE FROM HmsEventLogEntity e WHERE e.hmsJobId = :hmsJobId")
    void deleteByHmsJobId(@org.springframework.data.repository.query.Param("hmsJobId") String hmsJobId);
}
