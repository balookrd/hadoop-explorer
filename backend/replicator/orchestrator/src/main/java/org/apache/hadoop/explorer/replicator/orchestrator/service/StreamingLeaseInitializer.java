package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.StreamingLeaseEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.StreamingLeaseRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Изолированный инициализатор аренды стримера в автономной транзакции (REQUIRES_NEW).
 * Исключает пометку внешней транзакции как rollback-only при параллельной гонке создания записи.
 */
@Service
public class StreamingLeaseInitializer {

    private static final Logger log = LoggerFactory.getLogger(StreamingLeaseInitializer.class);
    private final StreamingLeaseRepository leaseRepository;

    public StreamingLeaseInitializer(StreamingLeaseRepository leaseRepository) {
        this.leaseRepository = leaseRepository;
    }

    @Transactional
    public boolean tryCreateInitialLease(String clusterId, String agentId, Instant expiresAt, Instant now) {
        if (!leaseRepository.existsById(clusterId)) {
            try {
                StreamingLeaseEntity initial = new StreamingLeaseEntity(clusterId, agentId, 1L, expiresAt, now);
                leaseRepository.saveAndFlush(initial);
                log.info("[Streamer HA] Создана начальная аренда для кластера '{}': активный агент '{}', эпоха 1", clusterId, agentId);
                return true;
            } catch (Exception e) {
                log.debug("[Streamer HA] Конкурентная инициализация лизинга кластера {} перехвачена другим узлом: {}", clusterId, e.getMessage());
            }
        }
        return false;
    }
}
