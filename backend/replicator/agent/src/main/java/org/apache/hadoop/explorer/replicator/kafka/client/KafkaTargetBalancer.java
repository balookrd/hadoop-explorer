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
 * 1. Детерминированное распределение партиций по M узлам-приемникам с использованием
 *    Consistent Hashing (Rendezvous / Highest Random Weight Hashing).
 * 2. Сохранение строгого порядка следования записей внутри партиции (все батчи партиции P
 *    гарантированно идут на один и тот же целевой узел).
 * 3. Локализованный Failover без каскадного reshuffle: при сбое узла он временно переводится в карантин,
 *    и только его партиции (1/M) перераспределяются по оставшимся узлам. Все остальные партиции
 *    остаются на своих прежних целевых агентах, предотвращая сброс сетевых буферов и гонки порядков.
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
     * Вычисляет 64-битный вес узла для заданной партиции (Rendezvous / Highest Random Weight Hashing).
     * Быстрый перемешивающий хеш (SplitMix64 finalizer) гарантирует псевдослучайное равномерное
     * распределение и минимальное количество перестановок (1/M) при изменении состава узлов.
     */
    static long computeHashWeight(int partition, String endpoint) {
        long h = ((long) partition << 32) ^ (endpoint.hashCode() * 0x9E3779B97F4A7C15L);
        h ^= (h >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= (h >>> 33);
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= (h >>> 33);
        return h;
    }

    /**
     * Выбирает целевой узел для заданной партиции с использованием Consistent Hashing (Rendezvous / HRW).
     *
     * @param partition номер партиции Kafka
     * @return индекс целевого узла (0 .. endpoints.size() - 1)
     */
    public int selectTargetIndex(int partition) {
        List<Integer> healthy = getHealthyIndices();
        if (healthy.size() == 1) {
            return healthy.get(0);
        }

        int bestIndex = healthy.get(0);
        long maxWeight = Long.MIN_VALUE;

        for (int nodeIndex : healthy) {
            String ep = endpoints.get(nodeIndex);
            long weight = computeHashWeight(partition, ep);
            if (weight > maxWeight) {
                maxWeight = weight;
                bestIndex = nodeIndex;
            }
        }
        return bestIndex;
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
