package org.apache.hadoop.explorer.replicator.kafka.agent;

import org.apache.hadoop.explorer.replicator.generated.control.KafkaJobItemProto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Синхронизация пулов задач с Control Plane идет в своем потоке и не мешает heartbeat.
 */
@DisplayName("Kafka-агент: синхронизация задач вне потока heartbeat")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KafkaJobSyncTest {

    @Test
    @DisplayName("Синхронизация идет в потоке kafka-agent-job-sync")
    void testRunsOnDedicatedThread() throws Exception {
        AtomicReference<String> thread = new AtomicReference<>();
        CountDownLatch synced = new CountDownLatch(1);
        ScheduledExecutorService executor = KafkaReplicatorAgentMain.startJobSync(List::of, jobs -> {
            thread.set(Thread.currentThread().getName());
            synced.countDown();
        }, 50L);
        try {
            assertTrue(synced.await(10, TimeUnit.SECONDS));
            assertEquals("kafka-agent-job-sync", thread.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("Долгая синхронизация не копит опросы, null и ошибки не трогают пулы")
    void testBlockedSyncDoesNotPileUpPolls() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        AtomicInteger syncs = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blocked = new CountDownLatch(1);
        ScheduledExecutorService executor = KafkaReplicatorAgentMain.startJobSync(() -> {
            int n = polls.incrementAndGet();
            if (n == 1) {
                return null; // оркестратор не ответил
            }
            if (n == 2) {
                throw new IllegalStateException("UNAVAILABLE");
            }
            return List.<KafkaJobItemProto>of();
        }, jobs -> {
            syncs.incrementAndGet();
            blocked.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 20L);
        try {
            assertTrue(blocked.await(10, TimeUnit.SECONDS), "после null и ошибки опрос должен продолжаться");
            int pollsWhileBlocked = polls.get();
            Thread.sleep(300);
            assertEquals(pollsWhileBlocked, polls.get(), "пока идет синхронизация, новые опросы не начинаются");
            assertEquals(1, syncs.get(), "null и ошибка опроса не должны доходить до пулов");
            release.countDown();
        } finally {
            executor.shutdownNow();
        }
    }
}
