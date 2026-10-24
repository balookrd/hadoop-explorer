package org.apache.hadoop.explorer.replicator.kafka.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Фильтр черного списка (blacklist) консьюмер-групп для репликации Kafka.
 * <p>
 * Исключает из сканирования и репликации:
 * <ul>
 *   <li>Собственные группы Репликатора (агенты-отправители, time-resolver, фоновые воркеры)</li>
 *   <li>Внутренние брокерные группы Kafka (__*)</li>
 *   <li>Инфраструктурные сервисы экосистемы (Schema Registry, Kafka Connect, MirrorMaker)</li>
 *   <li>Служебные и временные консольные группы (console-consumer-*, kafka-exporter-*)</li>
 * </ul>
 * Это предотвращает загрязнение метаданных, бесконечные циклы смещений в Active-Active и
 * повреждение локальных оффсетов репликатора.
 */
public class ConsumerGroupBlacklistFilter {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupBlacklistFilter.class);

    /**
     * Дефолтный регулярный шаблон для исключения служебных и инфраструктурных групп.
     */
    public static final String DEFAULT_EXCLUDE_GROUPS_REGEX =
            "^(kafka-replicator-.*|replicator-.*|hadoop-replicator-.*|connect-.*|schema-registry-.*|console-consumer-.*|kafka-exporter-.*|__.*)$";

    private static final ConsumerGroupBlacklistFilter DEFAULT_INSTANCE =
            new ConsumerGroupBlacklistFilter(DEFAULT_EXCLUDE_GROUPS_REGEX);

    private final Pattern pattern;
    private final String regexString;

    public static ConsumerGroupBlacklistFilter defaultFilter() {
        return DEFAULT_INSTANCE;
    }

    public ConsumerGroupBlacklistFilter() {
        this(DEFAULT_EXCLUDE_GROUPS_REGEX);
    }

    public ConsumerGroupBlacklistFilter(String regex) {
        this.regexString = (regex != null && !regex.isBlank()) ? regex.trim() : DEFAULT_EXCLUDE_GROUPS_REGEX;
        this.pattern = Pattern.compile(this.regexString);
    }

    public ConsumerGroupBlacklistFilter(Pattern pattern) {
        this.pattern = Objects.requireNonNull(pattern, "pattern must not be null");
        this.regexString = pattern.pattern();
    }

    /**
     * Проверяет, находится ли группа в черном списке (исключается из репликации).
     *
     * @param groupId идентификатор консьюмер-группы
     * @return {@code true}, если группа служебная и должна быть исключена; {@code false}, если разрешена
     */
    public boolean isExcluded(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            return true;
        }
        return pattern.matcher(groupId.trim()).matches();
    }

    /**
     * Проверяет, разрешена ли группа для сканирования и синхронизации.
     *
     * @param groupId идентификатор консьюмер-группы
     * @return {@code true}, если группа разрешена для репликации
     */
    public boolean isAllowed(String groupId) {
        return !isExcluded(groupId);
    }

    /**
     * Фильтрует коллекцию групп, возвращая только разрешенные для репликации.
     */
    public List<String> filterAllowed(Collection<String> groupIds) {
        if (groupIds == null || groupIds.isEmpty()) {
            return Collections.emptyList();
        }
        return groupIds.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(this::isAllowed)
                .collect(Collectors.toList());
    }

    /**
     * Возвращает множество групп из переданной коллекции, которые попали в черный список.
     */
    public Set<String> filterExcluded(Collection<String> groupIds) {
        if (groupIds == null || groupIds.isEmpty()) {
            return Collections.emptySet();
        }
        return groupIds.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(this::isExcluded)
                .collect(Collectors.toSet());
    }

    public String getRegexString() {
        return regexString;
    }

    public Pattern getPattern() {
        return pattern;
    }
}
