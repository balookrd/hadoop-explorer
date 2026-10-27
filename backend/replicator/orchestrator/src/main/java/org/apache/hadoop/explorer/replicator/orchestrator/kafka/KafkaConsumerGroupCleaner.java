package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Удаление консьюмер-групп удаленной Kafka-задачи.
 *
 * <p>Группа задачи ({@code kafka-replicator-<dc>-<jobId>}) оставалась на кластере источника с
 * зафиксированными смещениями: мониторинг лага видел «отстающую» группу, которой больше нет.
 * Сразу после удаления задачи группа еще не пуста: агенты останавливают пулы в течение
 * нескольких секунд, а участники со static membership числятся в группе до истечения
 * {@code session.timeout.ms}. Поэтому удаление повторяется, пока брокер не перестанет отвечать
 * GroupNotEmpty.
 */
@Component
public class KafkaConsumerGroupCleaner implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerGroupCleaner.class);

    /** Где лежит группа и с какими учетными данными к нему идти. */
    public record GroupTarget(String bootstrapServers, boolean kerberosEnabled, String principal,
                              String keytabPath, String serviceName, String groupId) {}

    private final KafkaAdminClientFactory adminClientFactory;
    private final long retryDelayMs;
    private final int maxAttempts;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "kafka-group-cleaner");
        t.setDaemon(true);
        return t;
    });

    @Autowired
    public KafkaConsumerGroupCleaner(KafkaAdminClientFactory adminClientFactory) {
        // 12 попыток через 15 с — три минуты: с запасом больше session.timeout.ms агентов (45 с)
        this(adminClientFactory, 15_000L, 12);
    }

    KafkaConsumerGroupCleaner(KafkaAdminClientFactory adminClientFactory, long retryDelayMs, int maxAttempts) {
        this.adminClientFactory = adminClientFactory;
        this.retryDelayMs = retryDelayMs;
        this.maxAttempts = maxAttempts;
    }

    public void scheduleDeletion(String jobId, List<GroupTarget> targets) {
        for (GroupTarget target : targets) {
            if (target.bootstrapServers() == null || target.bootstrapServers().isBlank()) {
                log.warn("Группа '{}' задачи '{}' не удалена: неизвестен адрес брокеров", target.groupId(), jobId);
                continue;
            }
            executor.schedule(() -> attempt(jobId, target, 1), retryDelayMs, TimeUnit.MILLISECONDS);
        }
    }

    private void attempt(String jobId, GroupTarget target, int attempt) {
        try (AdminClient admin = adminClientFactory.createAdminClient(target.bootstrapServers(),
                target.kerberosEnabled(), target.principal(), target.keytabPath(), target.serviceName())) {
            admin.deleteConsumerGroups(List.of(target.groupId())).all().get(10, TimeUnit.SECONDS);
            log.info("Консьюмер-группа '{}' удаленной задачи '{}' удалена с [{}]",
                    target.groupId(), jobId, target.bootstrapServers());
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            if (root instanceof GroupIdNotFoundException) {
                log.debug("Консьюмер-группы '{}' задачи '{}' нет: удалять нечего", target.groupId(), jobId);
                return;
            }
            if (attempt < maxAttempts) {
                log.debug("Группа '{}' задачи '{}' пока не удалена (попытка {} из {}): {}",
                        target.groupId(), jobId, attempt, maxAttempts, root.getMessage());
                executor.schedule(() -> attempt(jobId, target, attempt + 1), retryDelayMs, TimeUnit.MILLISECONDS);
            } else {
                log.warn("Консьюмер-группа '{}' удаленной задачи '{}' осталась на [{}]: {}. Удалите ее вручную: "
                                + "kafka-consumer-groups --delete --group {}",
                        target.groupId(), jobId, target.bootstrapServers(), root.getMessage(), target.groupId());
            }
        }
    }

    @Override
    public void destroy() {
        executor.shutdownNow();
    }
}
