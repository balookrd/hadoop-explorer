package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;

/**
 * Суммы по отобранным заданиям метастора одним запросом.
 *
 * <p>Складывать перенесенные таблицы и партиции перебором значит вычитать весь отобранный набор
 * в память — ровно то, от чего уводит страничная выборка. База считает это сама.
 */
@Component
public class HmsJobListingAggregates {

    @PersistenceContext
    private EntityManager entityManager;

    /** @return перенесенные таблицы и партиции по тому же условию, что у страницы */
    public Totals sumReplicated(Specification<HmsReplicationJobEntity> spec) {
        if (entityManager == null) {
            return new Totals(0L, 0L);
        }
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Object[]> query = cb.createQuery(Object[].class);
        Root<HmsReplicationJobEntity> root = query.from(HmsReplicationJobEntity.class);
        query.multiselect(
                cb.coalesce(cb.sum(root.get("replicatedTables")), 0),
                cb.coalesce(cb.sum(root.get("replicatedPartitions")), 0));
        if (spec != null) {
            var predicate = spec.toPredicate(root, query, cb);
            if (predicate != null) {
                query.where(predicate);
            }
        }
        Object[] row = entityManager.createQuery(query).getSingleResult();
        return new Totals(((Number) row[0]).longValue(), ((Number) row[1]).longValue());
    }

    public record Totals(long replicatedTables, long replicatedPartitions) {
    }
}
