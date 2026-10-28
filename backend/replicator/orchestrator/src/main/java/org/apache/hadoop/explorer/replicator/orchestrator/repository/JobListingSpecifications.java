package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.springframework.data.jpa.domain.Specification;

import java.util.ArrayList;
import java.util.List;

/**
 * Условия отбора заданий для списка — единственное место, где они описаны.
 *
 * <p>Страница, счетчики по статусам и список авторов должны отбирать одно и то же: иначе чипы
 * в интерфейсе показывают одно число, а страница — другой набор. Поэтому условие собирается
 * здесь и переиспользуется всеми тремя запросами, а не повторяется тремя текстами JPQL.
 */
public final class JobListingSpecifications {

    /** Статус потоковой репликации и режим синхронизации, который к нему относится. */
    private static final String STREAMING_STATUS = "STREAMING";
    private static final String STREAMING_SYNC_MODE = "STREAMING_INOTIFY";

    /** Служебные подзадачи метастора: в регламентном списке HDFS их не показывают. */
    private static final List<String> SUBJOB_TYPES = List.of("HMS_SUBJOB", "HMS_COMPANION_STREAMING");

    private JobListingSpecifications() {
    }

    /**
     * Условие списка: статус, кластер-источник, автор, поиск, служебные подзадачи и владелец.
     *
     * @param status         статус в верхнем регистре или {@code null}
     * @param clusterId      кластер-источник в нижнем регистре или {@code null}
     * @param author         автор в нижнем регистре или {@code null}
     * @param search         подстрока поиска в нижнем регистре или {@code null}
     * @param includeSubjobs показывать ли служебные подзадачи метастора
     * @param owner          локальная часть имени пользователя или {@code null} для полного доступа
     */
    public static Specification<JobEntity> forListing(String status, String clusterId, String author,
                                                      String search, boolean includeSubjobs, String owner) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            Predicate byStatus = statusPredicate(status, root, cb);
            if (byStatus != null) {
                predicates.add(byStatus);
            }
            if (clusterId != null) {
                predicates.add(cb.equal(cb.lower(root.get("sourceClusterId")), clusterId));
            }
            if (author != null) {
                predicates.add(cb.equal(cb.lower(root.get("createdBy")), author));
            }
            if (search != null) {
                String pattern = "%" + search + "%";
                predicates.add(cb.or(
                        like(cb, root.get("id"), pattern),
                        like(cb, root.get("sourcePath"), pattern),
                        like(cb, root.get("targetPath"), pattern),
                        like(cb, root.get("sourceClusterId"), pattern),
                        like(cb, root.get("targetClusterId"), pattern),
                        like(cb, root.get("executionPrincipal"), pattern),
                        like(cb, root.get("createdBy"), pattern)));
            }
            if (!includeSubjobs) {
                predicates.add(cb.or(
                        cb.isNull(root.get("jobType")),
                        cb.not(cb.upper(root.get("jobType")).in(SUBJOB_TYPES))));
            }
            if (owner != null) {
                // Правило владельца: сравниваются локальные части имен без realm-суффикса,
                // системные и бесхозные задания видны всем, совпадение по принципалу тоже считается
                predicates.add(cb.or(
                        cb.isNull(root.get("createdBy")),
                        cb.equal(cb.trim(root.get("createdBy")), ""),
                        localPart(cb, root.get("createdBy")).in("system", owner),
                        cb.and(cb.isNotNull(root.get("executionPrincipal")),
                                cb.equal(localPart(cb, root.get("executionPrincipal")), owner))));
            }
            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }

    /**
     * Добавка условия по статусу к готовому условию списка — для счетчиков сводки.
     */
    public static Specification<JobEntity> withStatus(String status) {
        return (root, query, cb) -> {
            Predicate byStatus = statusPredicate(status, root, cb);
            return byStatus != null ? byStatus : cb.conjunction();
        };
    }

    private static Predicate statusPredicate(String status, Path<JobEntity> root, CriteriaBuilder cb) {
        if (status == null || status.isBlank()) {
            return null;
        }
        // Равенство по самой колонке, а не upper(status): функция над колонкой исключала индекс
        // idx_jobs_status, и каждый опрос анализатора каждого агента вместе с count страницы читал
        // всю таблицу заданий — при подзадаче на партицию это миллионы строк. Статусы пишутся
        // в верхнем регистре; нижний оставлен для строк, записанных до этого правила
        Path<String> column = root.get("status");
        Predicate byStatus = cb.or(cb.equal(column, status), cb.equal(column, status.toLowerCase()));
        if (STREAMING_STATUS.equals(status)) {
            // Потоковое задание может стоять в статусе RUNNING: в интерфейсе это один фильтр
            return cb.or(byStatus, cb.equal(cb.upper(root.get("syncMode")), STREAMING_SYNC_MODE));
        }
        return byStatus;
    }

    private static Predicate like(CriteriaBuilder cb, Path<String> path, String pattern) {
        return cb.like(cb.lower(path), pattern);
    }

    /**
     * Локальная часть имени: часть до {@code @}, в нижнем регистре.
     */
    private static Expression<String> localPart(CriteriaBuilder cb, Path<String> path) {
        Expression<Integer> at = cb.locate(path, "@");
        return cb.lower(cb.<String>selectCase()
                .when(cb.greaterThan(at, 0), cb.substring(path, cb.literal(1), cb.diff(at, 1)))
                .otherwise(path));
    }
}
