package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.ClusterLockEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.ClusterLockRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Монотонность Fencing Token при освобождении блокировки.
 *
 * <p>Эпоха — это токен ограждения: обладатель предъявляет её при записи, а отстающий держатель
 * (например, после GC-паузы) отсекается по меньшей эпохе. Это работает только пока эпоха
 * монотонно растёт в пределах имени блокировки. Снятие аренды, удаляющее строку, обнуляет
 * счётчик: следующий захват начинает с единицы, и зомби со старым большим токеном выглядит
 * «свежее» законного владельца.
 */
@SpringBootTest
@ActiveProfiles("test")
@Transactional
class DistributedLockEpochTest {

    @Autowired
    private ClusterLockRepository lockRepository;

    private DistributedLockService node1;
    private DistributedLockService node2;

    @BeforeEach
    void setUp() {
        node1 = new DistributedLockService(lockRepository);
        node2 = new DistributedLockService(lockRepository);
    }

    @Test
    @DisplayName("Снятие блокировки сохраняет строку и накопленную эпоху, а не удаляет её")
    void releaseShouldPreserveRowAndEpoch() {
        String lockName = "test_epoch_release_keeps_row";

        DistributedLockService.LockHandle handle = node1.tryLockWithHandle(lockName, Duration.ofSeconds(10));
        assertTrue(handle.acquired());

        node1.releaseLock(handle);

        Optional<ClusterLockEntity> row = lockRepository.findById(lockName);
        assertTrue(row.isPresent(), "Строка блокировки должна остаться после снятия аренды");
        assertEquals(handle.epoch(), row.get().getEpoch(),
                "Снятие аренды не должно терять накопленную эпоху");
        assertFalse(row.get().getLockedUntil().isAfter(Instant.now()),
                "Снятая аренда должна быть просрочена, чтобы её мог взять любой узел");
        assertFalse(node1.isLockHeld(lockName, handle.epoch()),
                "После снятия аренды прежний владелец не удерживает блокировку");
    }

    @Test
    @DisplayName("Повторный захват тем же узлом после снятия аренды выдаёт строго большую эпоху")
    void reacquireBySameNodeShouldAdvanceEpoch() {
        String lockName = "test_epoch_reacquire_same_node";

        DistributedLockService.LockHandle first = node1.tryLockWithHandle(lockName, Duration.ofSeconds(10));
        assertTrue(first.acquired());
        node1.releaseLock(first);

        DistributedLockService.LockHandle second = node1.tryLockWithHandle(lockName, Duration.ofSeconds(10));
        assertTrue(second.acquired(), "Снятая аренда должна быть доступна для повторного захвата");
        assertTrue(second.epoch() > first.epoch(),
                "Новая аренда — новый токен: эпоха должна вырасти, а не начаться заново (было "
                        + first.epoch() + ", стало " + second.epoch() + ")");
    }

    @Test
    @DisplayName("Токен прежней аренды отсекается после повторного захвата")
    void staleEpochShouldBeFencedAfterReacquire() {
        String lockName = "test_epoch_fences_stale_holder";

        DistributedLockService.LockHandle stale = node1.tryLockWithHandle(lockName, Duration.ofSeconds(10));
        assertTrue(stale.acquired());
        node1.releaseLock(stale);

        DistributedLockService.LockHandle fresh = node2.tryLockWithHandle(lockName, Duration.ofSeconds(10));
        assertTrue(fresh.acquired());

        assertFalse(node1.isLockHeld(lockName, stale.epoch()),
                "Зомби с токеном прошлой аренды не должен проходить проверку владения");
        assertTrue(node2.isLockHeld(lockName, fresh.epoch()),
                "Актуальный владелец должен проходить проверку владения");
        assertTrue(fresh.epoch() > stale.epoch(),
                "Токен нового владельца должен быть больше токена прошлой аренды");
    }

    @Test
    @DisplayName("Серия снятий и захватов разными узлами монотонно наращивает эпоху")
    void epochShouldGrowAcrossHandoffs() {
        String lockName = "test_epoch_monotonic_handoff";
        long previous = 0L;

        for (int i = 0; i < 4; i++) {
            DistributedLockService owner = (i % 2 == 0) ? node1 : node2;
            DistributedLockService.LockHandle handle = owner.tryLockWithHandle(lockName, Duration.ofSeconds(10));
            assertTrue(handle.acquired(), "Захват #" + i + " должен удаться");
            assertTrue(handle.epoch() > previous,
                    "Эпоха должна строго расти при каждой новой аренде: шаг " + i
                            + ", было " + previous + ", стало " + handle.epoch());
            previous = handle.epoch();
            owner.releaseLock(handle);
        }
    }
}
