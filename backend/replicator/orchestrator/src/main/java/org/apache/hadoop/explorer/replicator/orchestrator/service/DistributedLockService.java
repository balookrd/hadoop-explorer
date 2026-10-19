package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.ClusterLockEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.ClusterLockRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Сервис распределенных блокировок для обеспечения высокой доступности (Orchestrator HA)
 * и предотвращения параллельного исполнения шедулеров и фоновых задач на разных инстансах.
 * Поддерживает монотонные Fencing Tokens (Epoch) для предотвращения Split-Brain при GC-паузах.
 */
@Service
public class DistributedLockService {

    private static final Logger log = LoggerFactory.getLogger(DistributedLockService.class);

    public record LockHandle(boolean acquired, long epoch, String lockName, String nodeId) {
        public static LockHandle failed(String lockName, String nodeId) {
            return new LockHandle(false, 0L, lockName, nodeId);
        }
    }

    private final ClusterLockRepository lockRepository;
    private final String nodeId = "orch-node-" + UUID.randomUUID().toString().substring(0, 8);

    public DistributedLockService(ClusterLockRepository lockRepository) {
        this.lockRepository = lockRepository;
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * Попытка захватить или продлить распределенную блокировку на указанную длительность
     * с возвратом LockHandle, содержащего актуальный Fencing Token (epoch).
     */
    @Transactional
    public LockHandle tryLockWithHandle(String lockName, Duration duration) {
        if (lockName == null || lockName.isBlank() || duration == null) {
            return LockHandle.failed(lockName, nodeId);
        }
        Instant now = Instant.now();
        Instant until = now.plus(duration);

        try {
            int updated = lockRepository.tryAcquireOrRenew(lockName, nodeId, until, now);
            if (updated > 0) {
                long epoch = lockRepository.findEpochByLockName(lockName).orElse(1L);
                return new LockHandle(true, epoch, lockName, nodeId);
            }

            // Если записи еще не было, вставляем новую
            if (!lockRepository.existsById(lockName)) {
                try {
                    ClusterLockEntity saved = lockRepository.saveAndFlush(new ClusterLockEntity(lockName, nodeId, until, 1L));
                    return new LockHandle(true, saved.getEpoch(), lockName, nodeId);
                } catch (Exception e) {
                    // Конкурентная вставка другим инстансом
                    return LockHandle.failed(lockName, nodeId);
                }
            }
            return LockHandle.failed(lockName, nodeId);
        } catch (Exception e) {
            log.debug("[DistributedLock] Ошибка при попытке захвата блокировки {}: {}", lockName, e.getMessage());
            return LockHandle.failed(lockName, nodeId);
        }
    }

    /**
     * Попытка захватить или продлить распределенную блокировку на указанную длительность.
     */
    @Transactional
    public boolean tryLock(String lockName, Duration duration) {
        return tryLockWithHandle(lockName, duration).acquired();
    }

    /**
     * Проверка, что распределенная блокировка по-прежнему удерживается данным инстансом с указанной эпохой.
     */
    @Transactional(readOnly = true)
    public boolean isLockHeld(String lockName, long expectedEpoch) {
        if (lockName == null || expectedEpoch <= 0) return false;
        try {
            return lockRepository.isLockHeldValid(lockName, nodeId, expectedEpoch, Instant.now());
        } catch (Exception e) {
            log.debug("[DistributedLock] Ошибка проверки владения блокировкой {}: {}", lockName, e.getMessage());
            return false;
        }
    }

    /**
     * Получение текущей эпохи распределенной блокировки.
     */
    @Transactional(readOnly = true)
    public Optional<Long> getLockEpoch(String lockName) {
        if (lockName == null) return Optional.empty();
        return lockRepository.findEpochByLockName(lockName);
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

    @Transactional
    public void releaseLock(LockHandle handle) {
        if (handle != null && handle.acquired()) {
            releaseLock(handle.lockName());
        }
    }

    /**
     * Выполнение действия строго под защитой распределенной блокировки
     * с передачей Fencing Token (эпохи) и автоматическим фоновым продлением аренды (Heartbeat Renewal).
     */
    public void runWithLock(String lockName, Duration duration, Consumer<Long> actionWithEpoch) {
        LockHandle handle = tryLockWithHandle(lockName, duration);
        if (handle.acquired()) {
            final long lockedEpoch = handle.epoch();
            java.util.concurrent.atomic.AtomicBoolean active = new java.util.concurrent.atomic.AtomicBoolean(true);
            Thread renewerThread = null;
            if (duration.toMillis() > 1000) {
                renewerThread = Thread.ofVirtual().name("lock-renew-" + lockName).start(() -> {
                    long sleepMs = Math.max(500, duration.toMillis() / 2);
                    while (active.get()) {
                        try {
                            Thread.sleep(sleepMs);
                            if (active.get()) {
                                LockHandle renewed = tryLockWithHandle(lockName, duration);
                                if (!renewed.acquired() || renewed.epoch() != lockedEpoch) {
                                    active.set(false);
                                    log.warn("[DistributedLock Fencing Alert] Аренда блокировки '{}' была потеряна или перехвачена другим инстансом (ожидаемая эпоха: {}, результат продления: {})",
                                            lockName, lockedEpoch, renewed);
                                    break;
                                }
                            }
                        } catch (InterruptedException e) {
                            break;
                        } catch (Exception e) {
                            log.debug("[DistributedLock] Ошибка автопродления блокировки {}: {}", lockName, e.getMessage());
                        }
                    }
                });
            }

            try {
                actionWithEpoch.accept(lockedEpoch);
            } finally {
                active.set(false);
                if (renewerThread != null) {
                    renewerThread.interrupt();
                }
                releaseLock(lockName);
            }
        } else {
            log.trace("[DistributedLock] Пропуск задачи '{}', так как блокировка удерживается другим инстансом", lockName);
        }
    }

    /**
     * Перегруженный метод runWithLock для действий Runnable (без явного использования эпохи).
     */
    public void runWithLock(String lockName, Duration duration, Runnable action) {
        runWithLock(lockName, duration, epoch -> action.run());
    }
}
