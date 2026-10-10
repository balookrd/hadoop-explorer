package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.ClusterLockEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.ClusterLockRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Сервис распределенных блокировок для обеспечения высокой доступности (Orchestrator HA)
 * и предотвращения параллельного исполнения шедулеров и фоновых задач на разных инстансах.
 */
@Service
public class DistributedLockService {

    private static final Logger log = LoggerFactory.getLogger(DistributedLockService.class);

    private final ClusterLockRepository lockRepository;
    private final String nodeId = "orch-node-" + UUID.randomUUID().toString().substring(0, 8);

    public DistributedLockService(ClusterLockRepository lockRepository) {
        this.lockRepository = lockRepository;
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * Попытка захватить или продлить распределенную блокировку на указанную длительность.
     */
    @Transactional
    public boolean tryLock(String lockName, Duration duration) {
        Instant now = Instant.now();
        Instant until = now.plus(duration);

        try {
            int updated = lockRepository.tryAcquireOrRenew(lockName, nodeId, until, now);
            if (updated > 0) {
                return true;
            }

            // Если записи еще не было, вставляем новую
            if (!lockRepository.existsById(lockName)) {
                try {
                    lockRepository.saveAndFlush(new ClusterLockEntity(lockName, nodeId, until));
                    return true;
                } catch (Exception e) {
                    // Конкурентная вставка другим инстансом
                    return false;
                }
            }
            return false;
        } catch (Exception e) {
            log.debug("[DistributedLock] Ошибка при попытке захвата блокировки {}: {}", lockName, e.getMessage());
            return false;
        }
    }

    /**
     * Освобождение ранее захваченной блокировки текущим инстансом.
     */
    @Transactional
    public void releaseLock(String lockName) {
        try {
            lockRepository.releaseLock(lockName, nodeId);
        } catch (Exception e) {
            log.debug("[DistributedLock] Ошибка при освобождении блокировки {}: {}", lockName, e.getMessage());
        }
    }

    /**
     * Выполнение действия строго под защитой распределенной блокировки.
     */
    public void runWithLock(String lockName, Duration duration, Runnable action) {
        if (tryLock(lockName, duration)) {
            try {
                action.run();
            } finally {
                releaseLock(lockName);
            }
        } else {
            log.trace("[DistributedLock] Пропуск задачи '{}', так как блокировка удерживается другим инстансом", lockName);
        }
    }
}
