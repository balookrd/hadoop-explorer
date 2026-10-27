package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
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

    /**
     * Запись телеметрии агентов ЦОД в его активные задачи одним UPDATE, без чтения сущностей.
     *
     * <p>Heartbeat раньше читал задачу и сохранял ее целиком. Если пауза фиксировалась между
     * чтением и записью heartbeat, сохранение возвращало статус ACTIVE, и пауза «не держалась».
     * Точечный UPDATE не трогает статус и не пишет в строку, ставшую неактивной. Счетчики
     * только растут: после рестарта оркестратора сумма по агентам набирается заново и не
     * должна откатывать уже показанное значение.
     */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("UPDATE KafkaReplicationJobEntity j SET "
            + "j.messagesReplicated = CASE WHEN j.messagesReplicated IS NULL OR j.messagesReplicated < :messages "
            + "THEN :messages ELSE j.messagesReplicated END, "
            + "j.bytesReplicated = CASE WHEN j.bytesReplicated IS NULL OR j.bytesReplicated < :bytes "
            + "THEN :bytes ELSE j.bytesReplicated END, "
            + "j.currentLagMs = :lagMs, j.updatedAt = :now "
            + "WHERE UPPER(j.status) = 'ACTIVE' "
            + "AND (LOWER(j.sourceDc) = LOWER(:dc) "
            + "OR (UPPER(j.direction) = 'BIDIRECTIONAL' AND LOWER(j.targetDc) = LOWER(:dc)))")
    int applyDcTelemetry(@Param("dc") String dc, @Param("messages") long messages,
                         @Param("bytes") long bytes, @Param("lagMs") long lagMs,
                         @Param("now") Instant now);

    /** Телеметрия одной задачи: те же правила, что у {@link #applyDcTelemetry}, но по id. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("UPDATE KafkaReplicationJobEntity j SET "
            + "j.messagesReplicated = CASE WHEN j.messagesReplicated IS NULL OR j.messagesReplicated < :messages "
            + "THEN :messages ELSE j.messagesReplicated END, "
            + "j.bytesReplicated = CASE WHEN j.bytesReplicated IS NULL OR j.bytesReplicated < :bytes "
            + "THEN :bytes ELSE j.bytesReplicated END, "
            + "j.currentLagMs = :lagMs, j.updatedAt = :now "
            + "WHERE j.id = :id AND UPPER(j.status) = 'ACTIVE'")
    int applyJobTelemetry(@Param("id") String id, @Param("messages") long messages,
                          @Param("bytes") long bytes, @Param("lagMs") long lagMs,
                          @Param("now") Instant now);

    /** Смена статуса задачи без перезаписи остальных полей строки. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("UPDATE KafkaReplicationJobEntity j SET j.status = :status, j.updatedAt = :now WHERE j.id = :id")
    int updateStatus(@Param("id") String id, @Param("status") String status, @Param("now") Instant now);

    /**
     * Итоги по всем задачам Kafka одной строкой: всего, активных, на паузе, сообщения, байты,
     * максимальный лаг.
     */
    @Query("SELECT count(j), "
            + "coalesce(sum(case when upper(j.status) = 'ACTIVE' then 1 else 0 end), 0), "
            + "coalesce(sum(case when upper(j.status) = 'PAUSED' then 1 else 0 end), 0), "
            + "coalesce(sum(j.messagesReplicated), 0), coalesce(sum(j.bytesReplicated), 0), "
            + "coalesce(max(j.currentLagMs), 0) "
            + "FROM KafkaReplicationJobEntity j")
    List<Object[]> aggregateReplicationTotals();

    /**
     * Строки для построчных метрик задач Kafka: только нужные поля и с ограничением.
     *
     * <p>Сущность тянет за собой TEXT-поля (список топиков, пул эндпоинтов, сводка
     * провижининга), и загружать их на каждом скрейпе Prometheus незачем. Итоговые счетчики
     * берутся агрегатом, поэтому ограничение выборки не искажает сумму — обрезаются только
     * построчные метрики, которые при таком количестве задач и так непригодны по кардинальности.
     */
    @Query("SELECT j.id, j.name, j.sourceDc, j.targetDc, j.status, "
            + "coalesce(j.messagesReplicated, 0), coalesce(j.bytesReplicated, 0), "
            + "coalesce(j.currentLagMs, 0), coalesce(j.bandwidthLimitMb, 0) "
            + "FROM KafkaReplicationJobEntity j ORDER BY j.name ASC")
    List<Object[]> findJobMetricRows(org.springframework.data.domain.Pageable pageable);
}
