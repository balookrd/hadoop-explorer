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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class DistributedLockFencingTest {

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
    @DisplayName("RES-07: Первый захват блокировки должен присваивать Fencing Token epoch = 1")
    void shouldAcquireLockWithEpochOne() {
        String lockName = "test_fencing_initial";

        DistributedLockService.LockHandle handle = node1.tryLockWithHandle(lockName, Duration.ofSeconds(10));
        assertTrue(handle.acquired(), "Node 1 должна успешно захватить блокировку");
        assertEquals(1L, handle.epoch(), "Первоначальная эпоха должна быть равна 1");
        assertEquals(node1.getNodeId(), handle.nodeId());

        assertTrue(node1.isLockHeld(lockName, 1L), "isLockHeld должен подтверждать владение с эпохой 1");
        assertFalse(node1.isLockHeld(lockName, 2L), "isLockHeld должен отклонять несовпадающую эпоху");

        node1.releaseLock(handle);
    }

    @Test
    @DisplayName("RES-07: Автопродление блокировки тем же инстансом сохраняет текущую эпоху (epoch не меняется)")
    void shouldKeepEpochOnRenewBySameNode() {
        String lockName = "test_fencing_renew";

        DistributedLockService.LockHandle handle1 = node1.tryLockWithHandle(lockName, Duration.ofSeconds(10));
        assertTrue(handle1.acquired());
        assertEquals(1L, handle1.epoch());

        // Продление тем же инстансом
        DistributedLockService.LockHandle handle2 = node1.tryLockWithHandle(lockName, Duration.ofSeconds(15));
        assertTrue(handle2.acquired());
        assertEquals(1L, handle2.epoch(), "Продление тем же узлом не должно увеличивать эпоху");

        node1.releaseLock(handle2);
    }

    @Test
    @DisplayName("RES-07: Перехват истекшей блокировки другим узлом монотонно инкрементирует Fencing Token (epoch = 2)")
    void shouldIncrementEpochOnStealByAnotherNode() {
        String lockName = "test_fencing_steal";

        // Симулируем уже существующую протухшую блокировку от node1 с эпохой 1
        Instant past = Instant.now().minusSeconds(10);
        ClusterLockEntity expiredLock = new ClusterLockEntity(lockName, node1.getNodeId(), past, 1L);
        lockRepository.saveAndFlush(expiredLock);

        // Node 2 перехватывает протухшую блокировку
        DistributedLockService.LockHandle handle2 = node2.tryLockWithHandle(lockName, Duration.ofSeconds(10));
        assertTrue(handle2.acquired(), "Node 2 должна перехватить протухшую блокировку");
        assertEquals(2L, handle2.epoch(), "При смене владельца эпоха должна инкрементироваться (1 -> 2)");
        assertEquals(node2.getNodeId(), handle2.nodeId());

        // Проверяем, что node 1 больше не владеет блокировкой
        assertFalse(node1.isLockHeld(lockName, 1L), "Node 1 со старой эпохой 1 больше не владеет замком");
        assertTrue(node2.isLockHeld(lockName, 2L), "Node 2 с актуальной эпохой 2 владеет замком");

        node2.releaseLock(handle2);
    }

    @Test
    @DisplayName("RES-07: runWithLock передает актуальный Fencing Token (epoch) в целевое действие")
    void shouldExecuteRunWithLockWithFencingToken() {
        String lockName = "test_run_with_fencing_token";
        AtomicLong capturedEpoch = new AtomicLong(0L);
        AtomicBoolean executed = new AtomicBoolean(false);

        node1.runWithLock(lockName, Duration.ofSeconds(5), epoch -> {
            capturedEpoch.set(epoch);
            executed.set(true);
            assertTrue(node1.isLockHeld(lockName, epoch));
            // Второй узел не должен иметь возможности захватить замок
            assertFalse(node2.tryLock(lockName, Duration.ofSeconds(5)));
        });

        assertTrue(executed.get());
        assertEquals(1L, capturedEpoch.get(), "В действие должна быть передана эпоха 1");
    }
}
