package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.springframework.data.jpa.domain.Specification;

import java.util.ArrayList;
import java.util.List;

/**
 * Условия отбора заданий метастора — единственное место, где они описаны.
 *
 * <p>Страница, счетчики статусов и список авторов обязаны отбирать одно и то же: иначе чип
 * показывает одно число, а страница — другой набор. Поэтому условие собирается здесь и
 * переиспользуется всеми запросами списка.
 */
public final class HmsJobListingSpecifications {

    private HmsJobListingSpecifications() {
    }

    /**
     * Условие списка: статус, поиск, автор и владелец.
     *
     * @param status статус в верхнем регистре или {@code null}
     * @param search подстрока поиска в нижнем регистре или {@code null}
     * @param author автор в нижнем регистре или {@code null}
     * @param owner  имя пользователя в нижнем регистре или {@code null} для полного доступа
     */
    public static Specification<HmsReplicationJobEntity> forListing(String status, String search,
                                                                    String author, String owner) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (status != null && !status.isBlank()) {
                predicates.add(cb.equal(cb.upper(root.get("status")), status));
            }
            if (author != null && !author.isBlank()) {
                predicates.add(cb.equal(cb.lower(root.get("createdBy")), author));
            }
            if (search != null && !search.isBlank()) {
                String pattern = "%" + search + "%";
                predicates.add(cb.or(
                        like(cb, root.get("id"), pattern),
                        like(cb, root.get("sourceDbName"), pattern),
                        like(cb, root.get("targetDbName"), pattern),
                        like(cb, root.get("sourceClusterId"), pattern),
                        like(cb, root.get("targetClusterId"), pattern),
                        like(cb, root.get("createdBy"), pattern)));
            }
            if (owner != null && !owner.isBlank()) {
                // Бесхозные и системные задания видны всем: их завела не конкретная учетка
                predicates.add(cb.or(
                        cb.isNull(root.get("createdBy")),
                        cb.equal(cb.lower(root.get("createdBy")), owner)));
            }
            return predicates.isEmpty() ? cb.conjunction() : cb.and(predicates.toArray(new Predicate[0]));
        };
    }

    /** Добавка условия по статусу к готовому условию списка — для счетчиков сводки. */
    public static Specification<HmsReplicationJobEntity> withStatus(String status) {
        return (root, query, cb) -> status == null || status.isBlank()
                ? cb.conjunction()
                : cb.equal(cb.upper(root.get("status")), status);
    }

    private static Predicate like(CriteriaBuilder cb, Path<String> path, String pattern) {
        return cb.like(cb.lower(path), pattern);
    }
}
