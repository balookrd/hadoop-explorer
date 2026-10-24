package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface KafkaReplicationJobRepository extends JpaRepository<KafkaReplicationJobEntity, String> {

    List<KafkaReplicationJobEntity> findByStatus(String status);

    List<KafkaReplicationJobEntity> findBySourceDcAndTargetDc(String sourceDc, String targetDc);

    /**
     * Активные задачи, по которым отчитывается агент указанного ЦОД.
     *
     * <p>Агент источника отчитывается по своим задачам; для BIDIRECTIONAL отчитывается и агент
     * целевого ЦОД, поскольку обратное направление обслуживает он. Выборка точечная: heartbeat
     * приходит от каждого агента каждые несколько секунд, и чтение всей таблицы на каждом вызове
     * давало лишние UPDATE по одним и тем же строкам.
     */
    @Query("SELECT j FROM KafkaReplicationJobEntity j WHERE UPPER(j.status) = 'ACTIVE' "
            + "AND (LOWER(j.sourceDc) = LOWER(:dc) "
            + "OR (UPPER(j.direction) = 'BIDIRECTIONAL' AND LOWER(j.targetDc) = LOWER(:dc)))")
    List<KafkaReplicationJobEntity> findActiveJobsReportedByDc(@Param("dc") String dc);
}
