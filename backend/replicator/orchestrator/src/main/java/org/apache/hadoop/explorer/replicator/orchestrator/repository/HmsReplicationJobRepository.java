package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
public interface HmsReplicationJobRepository extends JpaRepository<HmsReplicationJobEntity, String> {
    List<HmsReplicationJobEntity> findByStatus(String status);
    List<HmsReplicationJobEntity> findByStatusIn(Collection<String> statuses);
    List<HmsReplicationJobEntity> findByAssignedAgentIdIsNotNullAndStatusIn(Collection<String> statuses);
    long countByStatus(String status);
    Optional<HmsReplicationJobEntity> findBySourceClusterIdAndSourceDbName(String sourceClusterId, String sourceDbName);

    @Query("""
        SELECT h FROM HmsReplicationJobEntity h
        WHERE LOWER(REPLACE(REPLACE(h.sourceClusterId, '-', ''), '_', '')) = LOWER(REPLACE(REPLACE(:clusterId, '-', ''), '_', ''))
    """)
    List<HmsReplicationJobEntity> findByNormalizedSourceClusterId(@Param("clusterId") String clusterId);

    /**
     * Задачи, назначенные конкретному агенту — для эвакуации при его отключении.
     *
     * <p>Эвакуация читала таблицу задач целиком и сравнивала идентификатор агента в памяти.
     */
    List<HmsReplicationJobEntity> findByAssignedAgentId(String assignedAgentId);

    /**
     * Задачи одного маршрута: кластеры сравниваются без регистра, дефисов и подчеркиваний.
     */
    @Query("SELECT h FROM HmsReplicationJobEntity h "
            + "WHERE LOWER(REPLACE(REPLACE(h.sourceClusterId, '-', ''), '_', '')) "
            + "        = LOWER(REPLACE(REPLACE(:sourceClusterId, '-', ''), '_', '')) "
            + "  AND LOWER(REPLACE(REPLACE(h.targetClusterId, '-', ''), '_', '')) "
            + "        = LOWER(REPLACE(REPLACE(:targetClusterId, '-', ''), '_', ''))")
    List<HmsReplicationJobEntity> findByNormalizedRoute(@Param("sourceClusterId") String sourceClusterId,
                                                        @Param("targetClusterId") String targetClusterId);

    /**
     * Таблицы и партиции всех задач одним агрегатом: всего и перенесено.
     *
     * <p>Рендер метрик вычитывал таблицу задач HMS целиком четырежды за скрейп — по разу на
     * каждый гейдж.
     */
    @Query("SELECT coalesce(sum(h.totalTables), 0), coalesce(sum(h.replicatedTables), 0), "
            + "coalesce(sum(h.totalPartitions), 0), coalesce(sum(h.replicatedPartitions), 0) "
            + "FROM HmsReplicationJobEntity h")
    List<Object[]> aggregateTableAndPartitionCounts();
}
