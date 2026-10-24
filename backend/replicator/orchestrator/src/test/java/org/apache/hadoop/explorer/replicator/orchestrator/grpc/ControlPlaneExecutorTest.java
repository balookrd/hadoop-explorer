package org.apache.hadoop.explorer.replicator.orchestrator.grpc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Поведение пула Control Plane при перегрузке.
 *
 * <p>Ключевая гарантия: перегрузка не должна выполнять вызов в потоке вызывающего.
 * У gRPC-сервера вызывающий — это поток Netty event loop, поэтому {@code CallerRunsPolicy}
 * останавливал транспорт целиком: пока один вызов исполнялся в event loop, остальные агенты
 * не обслуживались вовсе. Честный отказ превращается в {@code UNAVAILABLE} одному вызывающему.
 */
@DisplayName("Пул Control Plane: отказ вместо блокировки транспорта")
class ControlPlaneExecutorTest {

    @Test
    @DisplayName("Переполнение пула отвергает задачу, а не выполняет ее в вызывающем потоке")
    void testOverloadRejectsInsteadOfRunningInCallerThread() throws Exception {
        ThreadPoolExecutor executor = OrchestratorGrpcServer.createGrpcExecutor();
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean ranInCallerThread = new AtomicBoolean(false);
        Thread callerThread = Thread.currentThread();

        try {
            // Занимаем все потоки и забиваем очередь
            int flood = executor.getMaximumPoolSize() + executor.getQueue().remainingCapacity() + 64;
            assertThrows(RejectedExecutionException.class, () -> {
                for (int i = 0; i < flood; i++) {
                    executor.execute(() -> {
                        if (Thread.currentThread() == callerThread) {
                            ranInCallerThread.set(true);
                        }
                        try {
                            release.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
            }, "Перегруженный пул обязан отвергать задачи");

            assertFalse(ranInCallerThread.get(),
                    "Задача выполнилась в вызывающем потоке: для gRPC это блокировка Netty event loop");
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("Потоки пула — демоны с узнаваемым именем")
    void testPoolThreadsAreNamedDaemons() throws Exception {
        ThreadPoolExecutor executor = OrchestratorGrpcServer.createGrpcExecutor();
        try {
            CountDownLatch started = new CountDownLatch(1);
            AtomicBoolean daemon = new AtomicBoolean(false);
            AtomicBoolean named = new AtomicBoolean(false);
            executor.execute(() -> {
                daemon.set(Thread.currentThread().isDaemon());
                named.set(Thread.currentThread().getName().startsWith("grpc-orchestrator-"));
                started.countDown();
            });

            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertTrue(daemon.get(), "Потоки пула не должны мешать завершению JVM");
            assertTrue(named.get(), "Имя потока должно указывать на Control Plane");
        } finally {
            executor.shutdownNow();
        }
    }
}
