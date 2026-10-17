package org.apache.hadoop.explorer.replicator.kafka.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Балансировщик нагрузки целевых агентов-приемников с привязкой по партициям (Partition-Pinned Balancer).
 * <p>
 * Обеспечивает:
 * 1. Детерминированное распределение 128+ партиций по M узлам-приемникам:
 *    {@code targetIndex = abs(partition) % activeTargets.size()}.
 * 2. Сохранение строгого порядка следования записей внутри партиции (все батчи партиции P
 *    гарантированно идут на один и тот же целевой узел).
 * 3. Автоматический Failover: при сбое узла он временно переводится в карантин,
 *    а партиции перераспределяются по оставшимся живым узлам.
 */
public class KafkaTargetBalancer {

    private static final Logger log = LoggerFactory.getLogger(KafkaTargetBalancer.class);

    private final List<String> endpoints;
    private final ConcurrentHashMap<Integer, Long> quarantinedUntilMs = new ConcurrentHashMap<>();
    private final long quarantineDurationMs;

    public KafkaTargetBalancer(List<String> endpoints) {
        this(endpoints, 10_000L); // По умолчанию 10 секунд карантина
    }

    public KafkaTargetBalancer(List<String> endpoints, long quarantineDurationMs) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Список целевых эндпоинтов не может быть пустым");
        }
        this.endpoints = List.copyOf(endpoints);
        this.quarantineDurationMs = Math.max(1000L, quarantineDurationMs);
    }

    public int getTargetCount() {
        return endpoints.size();
    }

    public List<String> getEndpoints() {
        return endpoints;
    }

    public String getEndpoint(int index) {
        return endpoints.get(index);
    }

    /**
     * Возвращает список индексов активных (здоровых) целевых узлов на текущий момент.
     */
    public List<Integer> getHealthyIndices() {
        long now = System.currentTimeMillis();
        List<Integer> healthy = new ArrayList<>();
        for (int i = 0; i < endpoints.size(); i++) {
            Long quarantinedUntil = quarantinedUntilMs.get(i);
            if (quarantinedUntil == null || now >= quarantinedUntil) {
                if (quarantinedUntil != null) {
                    quarantinedUntilMs.remove(i);
                    log.info("Целевой узел #{} ({}) вышел из карантина и вернулся в ротацию", i, endpoints.get(i));
                }
                healthy.add(i);
            }
        }
        // Если все узлы в карантине — сбрасываем карантин во избежание полной остановки репликации
        if (healthy.isEmpty()) {
            log.warn("Все целевые узлы оказались в карантине! Экстренный сброс карантина для продолжения работы.");
            quarantinedUntilMs.clear();
            for (int i = 0; i < endpoints.size(); i++) {
                healthy.add(i);
            }
        }
        return healthy;
    }

    /**
     * Выбирает целевой узел для заданной партиции с сохранением детерминированности.
     *
     * @param partition номер партиции Kafka
     * @return индекс целевого узла (0 .. endpoints.size() - 1)
     */
    public int selectTargetIndex(int partition) {
        List<Integer> healthy = getHealthyIndices();
        int slot = Math.abs(partition) % healthy.size();
        return healthy.get(slot);
    }

    /**
     * Помещает узел в карантин при обнаружении сбоя сети или gRPC стрима.
     */
    public void markFailed(int targetIndex, Throwable cause) {
        if (targetIndex >= 0 && targetIndex < endpoints.size()) {
            long until = System.currentTimeMillis() + quarantineDurationMs;
            quarantinedUntilMs.put(targetIndex, until);
            log.warn("Целевой узел #{} ({}) помещен в карантин на {} мс из-за ошибки: {}",
                    targetIndex, endpoints.get(targetIndex), quarantineDurationMs,
                    cause != null ? cause.getMessage() : "unknown");
        }
    }

    /**
     * Досрочно возвращает узел в строй (например, после успешного пинга).
     */
    public void markHealthy(int targetIndex) {
        if (quarantinedUntilMs.remove(targetIndex) != null) {
            log.info("Целевой узел #{} ({}) принудительно возвращен в строй после успешной проверки связи",
                    targetIndex, endpoints.get(targetIndex));
        }
    }

    public boolean isHealthy(int targetIndex) {
        Long until = quarantinedUntilMs.get(targetIndex);
        return until == null || System.currentTimeMillis() >= until;
    }
}
