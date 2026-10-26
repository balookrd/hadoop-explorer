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
            GroupLiveness liveness = describeGroupLiveness(groupId);
            if (liveness == GroupLiveness.UNKNOWN && !force) {
                // Неизвестно, читает ли кто-то группу прямо сейчас. Прежде любая ошибка —
                // недоступный брокер, таймаут, отказ по правам — означала «группа не активна»,
                // и смещения живой группы перезаписывались: консьюмеры целевого ДЦ откатывались
                // назад или перепрыгивали вперед.
                log.warn("Синхронизация оффсетов группы '{}' отменена: состояние группы в целевом ДЦ неизвестно", groupId);
                return OffsetSyncResponse.newBuilder()
                        .setGroupId(groupId)
                        .setSuccess(false)
                        .setBypassedActiveGroup(false)
                        .setErrorMessage("Состояние группы '" + groupId + "' в целевом кластере неизвестно: "
                                + "перенос смещений отменен, чтобы не перезаписать живую группу")
                        .build();
            }
            if (liveness == GroupLiveness.ACTIVE && !force) {
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

            // 3. Текущие смещения группы: перенос не должен двигать группу назад, иначе
            // целевой ДЦ перечитает уже обработанное
            Map<TopicPartition, OffsetAndMetadata> committed = targetAdminClient
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);

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

                OffsetAndMetadata current = committed != null ? committed.get(tp) : null;
                if (targetOffset >= 0 && current != null && targetOffset < current.offset() && !force) {
                    log.warn("Смещение группы '{}' для {}-{} не переносится: {} назад от текущего {}",
                            groupId, tpo.getTopic(), tpo.getPartition(), targetOffset, current.offset());
                    continue;
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
     * Состояние группы в целевом кластере: активна, неактивна или неизвестно.
     */
    enum GroupLiveness {
        ACTIVE,
        INACTIVE,
        UNKNOWN
    }

    /**
     * Активны ли живые консьюмеры группы в целевом кластере.
     *
     * <p>Неизвестное состояние трактуется как «активна»: не зная ответа, безопаснее считать,
     * что группу кто-то читает.
     */
    public boolean isGroupActiveInTarget(String groupId) {
        return describeGroupLiveness(groupId) != GroupLiveness.INACTIVE;
    }

    /**
     * Опрос состояния группы с разделением «группы нет» и «не удалось узнать».
     */
    GroupLiveness describeGroupLiveness(String groupId) {
        try {
            Map<String, ConsumerGroupDescription> descriptions = targetAdminClient
                    .describeConsumerGroups(List.of(groupId))
                    .all()
                    .get(3, TimeUnit.SECONDS);

            ConsumerGroupDescription desc = descriptions.get(groupId);
            if (desc == null) {
                return GroupLiveness.INACTIVE;
            }

            ConsumerGroupState state = desc.state();
            // Если состояние STABLE, PREPARING_REBALANCE или COMPLETING_REBALANCE — группа активна
            boolean active = state == ConsumerGroupState.STABLE ||
                    state == ConsumerGroupState.PREPARING_REBALANCE ||
                    state == ConsumerGroupState.COMPLETING_REBALANCE ||
                    !desc.members().isEmpty();
            return active ? GroupLiveness.ACTIVE : GroupLiveness.INACTIVE;
        } catch (Exception e) {
            if (isGroupNotFound(e)) {
                // Группы в целевом кластере еще нет — перезаписывать нечего
                log.debug("Группа '{}' не найдена в целевом кластере (будет создана при первом коммите): {}",
                        groupId, e.getMessage());
                return GroupLiveness.INACTIVE;
            }
            // Недоступный брокер, таймаут, отказ по правам: ответа нет, и выдавать это за
            // «группа не активна» нельзя
            log.warn("Не удалось определить состояние группы '{}' в целевом кластере: {}", groupId, e.getMessage());
            return GroupLiveness.UNKNOWN;
        }
    }

    private static boolean isGroupNotFound(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
                return true;
            }
            if (cause == cause.getCause()) {
                break;
            }
        }
        return false;
    }
}
