package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.repository.ClusterLockRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Граница транзакции при захвате распределённой блокировки.
 *
 * <p>Блокировка нужна остальным узлам кластера видимой сразу, а не после коммита бизнес-работы:
 * если строка аренды пишется в транзакции вызывающего, то пока задача идёт (минуты на обходе
 * HDFS), соседний инстанс не видит записи и запускает ту же задачу — split-brain, от которого
 * блокировка и должна защищать.
 */
@SpringBootTest
@ActiveProfiles("test")
class DistributedLockTransactionBoundaryTest {

    @Autowired
    private DistributedLockService lockService;

    @Autowired
    private ClusterLockRepository lockRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    @DisplayName("Захваченная блокировка видна другим транзакциям до коммита бизнес-транзакции")
    void lockShouldBeCommittedBeforeBusinessTransactionCompletes() {
        String lockName = "test_lock_boundary_" + UUID.randomUUID().toString().substring(0, 8);
        TransactionTemplate business = new TransactionTemplate(transactionManager);

        try {
            business.executeWithoutResult(status -> {
                DistributedLockService.LockHandle handle =
                        lockService.tryLockWithHandle(lockName, Duration.ofSeconds(30));
                assertTrue(handle.acquired(), "Блокировка должна быть захвачена");

                Optional<Long> seenFromOutside = readEpochInSeparateTransaction(lockName);
                assertNotNull(seenFromOutside,
                        "Чтение аренды из соседней транзакции не должно блокироваться на незакоммиченной строке");
                assertTrue(seenFromOutside.isPresent(),
                        "Соседний узел должен видеть аренду сразу после захвата, не дожидаясь "
                                + "коммита бизнес-транзакции");
                assertEquals(handle.epoch(), seenFromOutside.get(),
                        "Снаружи должна быть видна та же эпоха, что выдана владельцу");
            });
        } finally {
            new TransactionTemplate(transactionManager)
                    .executeWithoutResult(status -> lockRepository.deleteById(lockName));
        }
    }

    /**
     * Чтение эпохи в отдельном потоке и отдельной транзакции: {@code null} означает, что чтение
     * не завершилось за отведённое время (строка заблокирована пишущей транзакцией).
     */
    private Optional<Long> readEpochInSeparateTransaction(String lockName) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Optional<Long>> future = executor.submit(() ->
                    new TransactionTemplate(transactionManager)
                            .execute(status -> lockRepository.findEpochByLockName(lockName)));
            return future.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            return null;
        } finally {
            executor.shutdownNow();
        }
    }
}
