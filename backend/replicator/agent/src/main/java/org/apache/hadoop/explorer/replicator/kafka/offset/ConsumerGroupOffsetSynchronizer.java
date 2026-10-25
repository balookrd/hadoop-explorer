package org.apache.hadoop.explorer.replicator.kafka.offset;

import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.OffsetSyncResponse;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Периодический перенос смещений консьюмер-групп на целевой кластер.
 *
 * <p>Принимающая половина механизма была готова: RPC {@code SyncConsumerOffsets} реализован в
 * приемнике и применяет смещения через {@link SafetyCommitManager} с проверкой состояния группы и
 * ее отставания. Отправляющей половины не было: сканер {@link ConsumerGroupOffsetScanner} в
 * рабочем коде не создавался, а клиентский вызов {@code syncOffsets} не вызывался ниоткуда.
 * Из-за этого после переключения на целевой ЦОД группы начинали чтение с
 * {@code auto.offset.reset}, а не с перенесенных смещений.
 *
 * <p>Перенос не имеет права влиять на саму репликацию: любая ошибка сканирования или вызова
 * логируется и попадает в счетчик, но не прерывает ни проход, ни работу демона.
 */
public final class ConsumerGroupOffsetSynchronizer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupOffsetSynchronizer.class);

    /** Прогресс репликации: до каких смещений источника данные уже подтверждены приемником. */
    public interface ReplicationProgress {
        Map<TopicPartition, Long> replicatedOffsets();

        Map<TopicPartition, Long> lastTimestamps();
    }

    /** Итог одного прохода. */
    public record SyncResult(int sent, int failed) {
    }

    private final ConsumerGroupOffsetScanner scanner;
    private final KafkaReplicationClient client;
    private final Supplier<Collection<String>> groupsSupplier;
    private final Supplier<Set<String>> topicsSupplier;
    private final ReplicationProgress progress;
    private final long syncTimeoutSeconds;

    private final AtomicLong totalSent = new AtomicLong();
    private final AtomicLong totalFailed = new AtomicLong();
    private volatile ScheduledExecutorService scheduler;

    public ConsumerGroupOffsetSynchronizer(ConsumerGroupOffsetScanner scanner,
                                           KafkaReplicationClient client,
                                           Supplier<Collection<String>> groupsSupplier,
                                           Supplier<Set<String>> topicsSupplier,
                                           ReplicationProgress progress,
                                           long syncTimeoutSeconds) {
        this.scanner = scanner;
        this.client = client;
        this.groupsSupplier = groupsSupplier;
        this.topicsSupplier = topicsSupplier;
        this.progress = progress;
        this.syncTimeoutSeconds = syncTimeoutSeconds > 0 ? syncTimeoutSeconds : 10;
    }

    /** Один проход переноса. Исключения не выпускает. */
    public SyncResult syncOnce() {
        Collection<String> groups = safeGet(groupsSupplier);
        if (groups == null || groups.isEmpty()) {
            return new SyncResult(0, 0);
        }
        Set<String> topics = safeGet(topicsSupplier);

        List<OffsetSyncRequest> requests;
        try {
            requests = scanner.scanOffsetsForGroups(
                    groups,
                    topics != null ? topics : Set.of(),
                    progress.lastTimestamps(),
                    progress.replicatedOffsets(),
                    Map.of(),
                    Map.of(),
                    false);
        } catch (Exception e) {
            log.warn("Сканирование смещений консьюмер-групп не удалось: {}", e.getMessage());
            return new SyncResult(0, 0);
        }
        if (requests == null || requests.isEmpty()) {
            return new SyncResult(0, 0);
        }

        int sent = 0;
        int failed = 0;
        for (OffsetSyncRequest request : requests) {
            try {
                OffsetSyncResponse response = client.syncOffsets(request, syncTimeoutSeconds);
                if (response != null && response.getSuccess()) {
                    sent++;
                    continue;
                }
                failed++;
                if (response != null && response.getBypassedActiveGroup()) {
                    // Защитный гейт приемника: в целевом ЦОД группа активна, коммит пропущен намеренно
                    log.info("Перенос смещений группы '{}' пропущен: группа активна на целевом кластере",
                            request.getGroupId());
                } else {
                    log.warn("Приемник отклонил перенос смещений группы '{}': {}",
                            request.getGroupId(),
                            response != null ? response.getErrorMessage() : "пустой ответ");
                }
            } catch (Exception e) {
                failed++;
                log.warn("Сбой переноса смещений группы '{}': {}", request.getGroupId(), e.getMessage());
            }
        }

        totalSent.addAndGet(sent);
        totalFailed.addAndGet(failed);
        if (sent > 0 || failed > 0) {
            log.info("Перенос смещений консьюмер-групп: перенесено {}, с ошибками {}", sent, failed);
        }
        return new SyncResult(sent, failed);
    }

    /**
     * Запуск периодического переноса. Интервал меньше либо равный нулю означает, что перенос
     * выключен: это осознанный выбор оператора, а не сбой.
     */
    public void start(long intervalSeconds) {
        if (intervalSeconds <= 0) {
            log.info("Перенос смещений консьюмер-групп выключен (интервал не задан)");
            return;
        }
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kafka-offset-sync");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                syncOnce();
            } catch (Throwable t) {
                log.warn("Проход переноса смещений завершился ошибкой: {}", t.getMessage());
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        log.info("Перенос смещений консьюмер-групп включен с интервалом {} с", intervalSeconds);
    }

    public long getTotalSent() {
        return totalSent.get();
    }

    public long getTotalFailed() {
        return totalFailed.get();
    }

    private static <T> T safeGet(Supplier<T> supplier) {
        try {
            return supplier != null ? supplier.get() : null;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() {
        ScheduledExecutorService current = scheduler;
        if (current != null) {
            current.shutdownNow();
            scheduler = null;
        }
    }
}
