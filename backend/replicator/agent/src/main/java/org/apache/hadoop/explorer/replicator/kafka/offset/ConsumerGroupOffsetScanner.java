package org.apache.hadoop.explorer.replicator.kafka.offset;

import org.apache.hadoop.explorer.replicator.kafka.filter.ConsumerGroupBlacklistFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.TopicPartitionOffset;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Сканер закоммиченных смещений консьюмер-групп в исходном кластере Kafka.
 * Обеспечивает строгую консистентность временных меток и предотвращает
 * перемотку отстающих (lagging) групп в конец топика.
 */
public class ConsumerGroupOffsetScanner {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupOffsetScanner.class);

    private final AdminClient sourceAdminClient;
    private final String sourceDcId;
    private final String targetDcId;
    private final ConsumerGroupBlacklistFilter blacklistFilter;

    public ConsumerGroupOffsetScanner(AdminClient sourceAdminClient, String sourceDcId, String targetDcId) {
        this(sourceAdminClient, sourceDcId, targetDcId, ConsumerGroupBlacklistFilter.defaultFilter());
    }

    public ConsumerGroupOffsetScanner(AdminClient sourceAdminClient, String sourceDcId, String targetDcId,
                                      ConsumerGroupBlacklistFilter blacklistFilter) {
        this.sourceAdminClient = Objects.requireNonNull(sourceAdminClient, "sourceAdminClient must not be null");
        this.sourceDcId = Objects.requireNonNull(sourceDcId, "sourceDcId must not be null");
        this.targetDcId = Objects.requireNonNull(targetDcId, "targetDcId must not be null");
        this.blacklistFilter = Objects.requireNonNull(blacklistFilter, "blacklistFilter must not be null");
    }

    public ConsumerGroupBlacklistFilter getBlacklistFilter() {
        return blacklistFilter;
    }

    public boolean isExcludedGroup(String groupId) {
        return blacklistFilter.isExcluded(groupId);
    }

    public List<String> filterAllowedGroups(Collection<String> groupIds) {
        return blacklistFilter.filterAllowed(groupIds);
    }

    /**
     * Считывает текущие закоммиченные смещения группы в источнике и формирует Protobuf-запрос синхронизации.
     * Защищает от перемотки оффсета группы в конец топика при отставании консьюмеров.
     *
     * @param groupId          идентификатор консьюмер-группы
     * @param topicFilter      множество разрешенных топиков (если пусто — все топики группы)
     * @param lastTimestampMap карта последних известных timestamps для партиций
     * @param forceCommit      флаг принудительного коммита
     * @return сформированный запрос синхронизации смещений
     */
    public Optional<OffsetSyncRequest> scanOffsets(String groupId,
                                                   Set<String> topicFilter,
                                                   Map<TopicPartition, Long> lastTimestampMap,
                                                   boolean forceCommit) {
        return scanOffsets(groupId, topicFilter, lastTimestampMap, null, null, null, forceCommit);
    }

    /**
     * Расширенный метод сканирования с явным указанием последних реплицированных смещений
     * и точных временных меток.
     *
     * @param groupId                 идентификатор консьюмер-группы
     * @param topicFilter             множество разрешенных топиков
     * @param lastTimestampMap        карта последних известных timestamps для партиций
     * @param lastReplicatedOffsetMap карта последних переданных смещений для проверки lag
     * @param exactOffsetTimestamps   карта точных соответствий (TopicPartition -> (Offset -> Timestamp))
     * @param directTargetOffsets     карта прямых целевых смещений (если известны)
     * @param forceCommit             флаг принудительного коммита
     * @return сформированный запрос синхронизации смещений
     */
    public Optional<OffsetSyncRequest> scanOffsets(String groupId,
                                                   Set<String> topicFilter,
                                                   Map<TopicPartition, Long> lastTimestampMap,
                                                   Map<TopicPartition, Long> lastReplicatedOffsetMap,
                                                   Map<TopicPartition, Map<Long, Long>> exactOffsetTimestamps,
                                                   Map<TopicPartition, Long> directTargetOffsets,
                                                   boolean forceCommit) {
        if (groupId == null || groupId.isBlank()) {
            return Optional.empty();
        }
        if (blacklistFilter.isExcluded(groupId)) {
            log.info("Сканирование оффсетов для группы '{}' пропущено: группа находится в черном списке служебных/инфраструктурных групп ({})",
                    groupId, blacklistFilter.getRegexString());
            return Optional.empty();
        }
        try {
            ListConsumerGroupOffsetsResult result = sourceAdminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> groupOffsets = result.partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);

            if (groupOffsets == null || groupOffsets.isEmpty()) {
                return Optional.empty();
            }

            // Фильтруем партиции
            Map<TopicPartition, OffsetAndMetadata> activePartitions = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : groupOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata meta = entry.getValue();

                if (meta == null || meta.offset() < 0) {
                    continue;
                }
                if (topicFilter != null && !topicFilter.isEmpty() && !topicFilter.contains(tp.topic())) {
                    continue;
                }
                activePartitions.put(tp, meta);
            }

            if (activePartitions.isEmpty()) {
                return Optional.empty();
            }

            // Если не передан lastReplicatedOffsetMap, определяем endOffsets топика через AdminClient
            Map<TopicPartition, Long> effectiveEndOffsets = (lastReplicatedOffsetMap != null)
                    ? new HashMap<>(lastReplicatedOffsetMap)
                    : fetchEndOffsets(activePartitions.keySet());

            List<TopicPartitionOffset> offsetList = new ArrayList<>();

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : activePartitions.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata meta = entry.getValue();
                long committedOffset = meta.offset();

                long timestamp = 0L;

                // 1. Проверяем точное соответствие смещения и временной метки (exact map)
                if (exactOffsetTimestamps != null && exactOffsetTimestamps.containsKey(tp)) {
                    Map<Long, Long> partitionMap = exactOffsetTimestamps.get(tp);
                    if (partitionMap != null && partitionMap.containsKey(committedOffset)) {
                        timestamp = partitionMap.get(committedOffset);
                    }
                }

                // 2. Если точный timestamp не найден, проверяем позицию группы относительно конца партиции/репликации
                if (timestamp <= 0 && lastTimestampMap != null && lastTimestampMap.containsKey(tp)) {
                    Long endOffset = effectiveEndOffsets.get(tp);
                    if (endOffset != null) {
                        // Подставлять timestamp конца партиции МОЖНО ТОЛЬКО ЕСЛИ группа догнала конец (lag == 0).
                        // Если группа отстает (committedOffset < endOffset), подстановка времени конца
                        // приведет к пропуску сообщений в целевом кластере!
                        if (committedOffset >= endOffset) {
                            timestamp = lastTimestampMap.get(tp);
                        } else {
                            log.debug("Группа '{}' отстает на {}:{} (оффсет группы={}, конец={}). " +
                                            "Timestamp конца топика не подставляется во избежание потери данных.",
                                    groupId, tp.topic(), tp.partition(), committedOffset, endOffset);
                        }
                    }
                }

                TopicPartitionOffset.Builder tpoBuilder = TopicPartitionOffset.newBuilder()
                        .setTopic(tp.topic())
                        .setPartition(tp.partition())
                        .setSourceOffset(committedOffset)
                        .setTimestamp(timestamp);

                if (directTargetOffsets != null && directTargetOffsets.containsKey(tp)) {
                    tpoBuilder.setTargetOffset(directTargetOffsets.get(tp));
                }

                offsetList.add(tpoBuilder.build());
            }

            if (offsetList.isEmpty()) {
                return Optional.empty();
            }

            OffsetSyncRequest request = OffsetSyncRequest.newBuilder()
                    .setGroupId(groupId)
                    .setSourceDc(sourceDcId)
                    .setTargetDc(targetDcId)
                    .addAllOffsets(offsetList)
                    .setForceCommit(forceCommit)
                    .build();

            return Optional.of(request);

        } catch (Exception e) {
            log.warn("Не удалось просканировать оффсеты для группы '{}': {}", groupId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Пакетное сканирование множества групп с автоматическим исключением групп из черного списка.
     */
    public List<OffsetSyncRequest> scanOffsetsForGroups(Collection<String> groupIds,
                                                        Set<String> topicFilter,
                                                        Map<TopicPartition, Long> lastTimestampMap,
                                                        Map<TopicPartition, Long> lastReplicatedOffsetMap,
                                                        Map<TopicPartition, Map<Long, Long>> exactOffsetTimestamps,
                                                        Map<TopicPartition, Long> directTargetOffsets,
                                                        boolean forceCommit) {
        if (groupIds == null || groupIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> allowed = blacklistFilter.filterAllowed(groupIds);
        List<OffsetSyncRequest> requests = new ArrayList<>();
        for (String gid : allowed) {
            scanOffsets(gid, topicFilter, lastTimestampMap, lastReplicatedOffsetMap,
                    exactOffsetTimestamps, directTargetOffsets, forceCommit)
                    .ifPresent(requests::add);
        }
        return requests;
    }

    private Map<TopicPartition, Long> fetchEndOffsets(Set<TopicPartition> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
            for (TopicPartition tp : partitions) {
                specs.put(tp, OffsetSpec.latest());
            }
            ListOffsetsResult listOffsetsResult = sourceAdminClient.listOffsets(specs);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> resultInfo =
                    listOffsetsResult.all().get(3, TimeUnit.SECONDS);

            Map<TopicPartition, Long> endOffsets = new HashMap<>();
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e : resultInfo.entrySet()) {
                endOffsets.put(e.getKey(), e.getValue().offset());
            }
            return endOffsets;
        } catch (Exception e) {
            log.debug("Не удалось получить latest offsets через AdminClient: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }
}
