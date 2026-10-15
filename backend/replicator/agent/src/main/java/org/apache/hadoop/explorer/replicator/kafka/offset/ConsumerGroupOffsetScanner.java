package org.apache.hadoop.explorer.replicator.kafka.offset;

import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.TopicPartitionOffset;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Сканер закоммиченных смещений консьюмер-групп в исходном кластере Kafka.
 */
public class ConsumerGroupOffsetScanner {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupOffsetScanner.class);

    private final AdminClient sourceAdminClient;
    private final String sourceDcId;
    private final String targetDcId;

    public ConsumerGroupOffsetScanner(AdminClient sourceAdminClient, String sourceDcId, String targetDcId) {
        this.sourceAdminClient = Objects.requireNonNull(sourceAdminClient, "sourceAdminClient must not be null");
        this.sourceDcId = Objects.requireNonNull(sourceDcId, "sourceDcId must not be null");
        this.targetDcId = Objects.requireNonNull(targetDcId, "targetDcId must not be null");
    }

    /**
     * Считывает текущие закоммиченные смещения группы в источнике и формирует Protobuf-запрос синхронизации.
     *
     * @param groupId       идентификатор консьюмер-группы
     * @param topicFilter   множество разрешенных топиков (если пусто — все топики группы)
     * @param lastTimestampMap карта последних известных timestamps для партиций
     * @param forceCommit   флаг принудительного коммита
     * @return сформированный запрос синхронизации смещений
     */
    public Optional<OffsetSyncRequest> scanOffsets(String groupId,
                                                   Set<String> topicFilter,
                                                   Map<TopicPartition, Long> lastTimestampMap,
                                                   boolean forceCommit) {
        try {
            ListConsumerGroupOffsetsResult result = sourceAdminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> groupOffsets = result.partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);

            if (groupOffsets == null || groupOffsets.isEmpty()) {
                return Optional.empty();
            }

            List<TopicPartitionOffset> offsetList = new ArrayList<>();

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : groupOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata meta = entry.getValue();

                if (meta == null || meta.offset() < 0) {
                    continue;
                }

                if (topicFilter != null && !topicFilter.isEmpty() && !topicFilter.contains(tp.topic())) {
                    continue; // Пропускаем топики, не включенные в задачу репликации
                }

                long timestamp = lastTimestampMap != null ? lastTimestampMap.getOrDefault(tp, 0L) : 0L;

                offsetList.add(TopicPartitionOffset.newBuilder()
                        .setTopic(tp.topic())
                        .setPartition(tp.partition())
                        .setSourceOffset(meta.offset())
                        .setTimestamp(timestamp)
                        .build());
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
}
