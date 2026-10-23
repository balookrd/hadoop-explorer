package org.apache.hadoop.explorer.replicator.kafka.offset;

import org.apache.hadoop.explorer.replicator.kafka.filter.ConsumerGroupBlacklistFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncResponse;
import org.apache.hadoop.explorer.replicator.kafka.generated.TopicPartitionOffset;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Менеджер безопасного применения смещений консьюмер-групп в целевом кластере Kafka.
 * <p>
 * Реализует защитный гейт: блокирует перезапись оффсетов, если консьюмер-группа активна
 * в целевом дата-центре (состояние STABLE / REBALANCING), исключая потерю или откат прогресса.
 */
public class SafetyCommitManager {

    private static final Logger log = LoggerFactory.getLogger(SafetyCommitManager.class);

    private final AdminClient targetAdminClient;
    private final KafkaConsumer<?, ?> timeResolverConsumer;
    private final ConsumerGroupBlacklistFilter blacklistFilter;
    private final java.util.concurrent.locks.ReentrantLock timeResolverLock = new java.util.concurrent.locks.ReentrantLock();

    public SafetyCommitManager(AdminClient targetAdminClient, KafkaConsumer<?, ?> timeResolverConsumer) {
        this(targetAdminClient, timeResolverConsumer, ConsumerGroupBlacklistFilter.defaultFilter());
    }

    public SafetyCommitManager(AdminClient targetAdminClient, KafkaConsumer<?, ?> timeResolverConsumer,
                               ConsumerGroupBlacklistFilter blacklistFilter) {
        this.targetAdminClient = Objects.requireNonNull(targetAdminClient, "targetAdminClient must not be null");
        this.timeResolverConsumer = Objects.requireNonNull(timeResolverConsumer, "timeResolverConsumer must not be null");
        this.blacklistFilter = Objects.requireNonNull(blacklistFilter, "blacklistFilter must not be null");
    }

    /**
     * Безопасно применяет смещения консьюмер-группы в целевом кластере.
     *
     * @param request запрос на синхронизацию оффсетов из удаленного кластера
     * @return результат синхронизации
     */
    public OffsetSyncResponse applyOffsetsSafely(OffsetSyncRequest request) {
        String groupId = request.getGroupId();
        boolean force = request.getForceCommit();

        if (blacklistFilter.isExcluded(groupId)) {
            log.warn("Синхронизация оффсетов группы '{}' отклонена: группа находится в черном списке служебных групп ({})",
                    groupId, blacklistFilter.getRegexString());
            return OffsetSyncResponse.newBuilder()
                    .setGroupId(groupId != null ? groupId : "unknown")
                    .setSuccess(false)
                    .setBypassedActiveGroup(false)
                    .setErrorMessage("Group is blacklisted as internal/infrastructure group: " + groupId)
                    .build();
        }

        try {
            // 1. Проверяем статус группы в целевом кластере
            boolean isActive = isGroupActiveInTarget(groupId);
            if (isActive && !force) {
                log.info("Синхронизация оффсетов группы '{}' пропущена: группа АКТИВНА в целевом ДЦ (защита от перезаписи)", groupId);
                return OffsetSyncResponse.newBuilder()
                        .setGroupId(groupId)
                        .setSuccess(true)
                        .setBypassedActiveGroup(true)
                        .build();
            }

            // 2. Транслируем timestamps записей в целевые смещения
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartitionOffset tpo : request.getOffsetsList()) {
                if (tpo.getTimestamp() > 0) {
                    timestampsToSearch.put(new TopicPartition(tpo.getTopic(), tpo.getPartition()), tpo.getTimestamp());
                }
            }

            Map<TopicPartition, OffsetAndTimestamp> foundOffsets = Collections.emptyMap();
            if (!timestampsToSearch.isEmpty()) {
                timeResolverLock.lock();
                try {
                    foundOffsets = timeResolverConsumer.offsetsForTimes(timestampsToSearch, Duration.ofSeconds(5));
                } finally {
                    timeResolverLock.unlock();
                }
            }

            // 3. Формируем мапу для alterConsumerGroupOffsets
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            List<TopicPartitionOffset> appliedOffsets = new ArrayList<>();

            for (TopicPartitionOffset tpo : request.getOffsetsList()) {
                TopicPartition tp = new TopicPartition(tpo.getTopic(), tpo.getPartition());
                long targetOffset = -1;

                OffsetAndTimestamp match = foundOffsets.get(tp);
                if (match != null) {
                    targetOffset = match.offset();
                } else if (tpo.hasTargetOffset()) {
                    targetOffset = tpo.getTargetOffset();
                }

                if (targetOffset >= 0) {
                    offsetsToCommit.put(tp, new OffsetAndMetadata(targetOffset, "Replicated by Kafka gRPC Replicator"));
                    appliedOffsets.add(TopicPartitionOffset.newBuilder()
                            .setTopic(tpo.getTopic())
                            .setPartition(tpo.getPartition())
                            .setSourceOffset(tpo.getSourceOffset())
                            .setTimestamp(tpo.getTimestamp())
                            .setTargetOffset(targetOffset)
                            .build());
                }
            }

            // 4. Фиксируем оффсеты в целевом кластере
            if (!offsetsToCommit.isEmpty()) {
                targetAdminClient.alterConsumerGroupOffsets(groupId, offsetsToCommit).all().get(5, TimeUnit.SECONDS);
                log.info("Успешно синхронизированы смещения для группы '{}' (обновлено {} партиций, force={})",
                        groupId, offsetsToCommit.size(), force);
            }

            return OffsetSyncResponse.newBuilder()
                    .setGroupId(groupId)
                    .setSuccess(true)
                    .setBypassedActiveGroup(false)
                    .addAllAppliedOffsets(appliedOffsets)
                    .build();

        } catch (Exception e) {
            log.error("Ошибка при безопасном коммите смещений группы '{}': {}", groupId, e.getMessage(), e);
            return OffsetSyncResponse.newBuilder()
                    .setGroupId(groupId)
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build();
        }
    }

    /**
     * Проверяет, активны ли живые консьюмеры в данной группе в целевом кластере.
     */
    public boolean isGroupActiveInTarget(String groupId) {
        try {
            Map<String, ConsumerGroupDescription> descriptions = targetAdminClient
                    .describeConsumerGroups(List.of(groupId))
                    .all()
                    .get(3, TimeUnit.SECONDS);

            ConsumerGroupDescription desc = descriptions.get(groupId);
            if (desc == null) {
                return false;
            }

            ConsumerGroupState state = desc.state();
            // Если состояние STABLE, PREPARING_REBALANCE или COMPLETING_REBALANCE — группа активна
            return state == ConsumerGroupState.STABLE ||
                    state == ConsumerGroupState.PREPARING_REBALANCE ||
                    state == ConsumerGroupState.COMPLETING_REBALANCE ||
                    !desc.members().isEmpty();
        } catch (Exception e) {
            // Если группы еще не существует в целевом кластере — она не активна
            log.debug("Группа '{}' не найдена в целевом кластере (будет создана при первом коммите): {}", groupId, e.getMessage());
            return false;
        }
    }
}
