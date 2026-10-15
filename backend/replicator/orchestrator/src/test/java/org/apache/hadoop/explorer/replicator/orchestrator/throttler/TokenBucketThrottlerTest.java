package org.apache.hadoop.explorer.replicator.orchestrator.throttler;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TokenBucketThrottlerTest {

    @Test
    @DisplayName("SingleBucket должен рассчитывать задержку при нехватке токенов")
    void shouldCalculateWaitWhenDeficit() {
        TokenBucketThrottler.SingleBucket bucket = new TokenBucketThrottler.SingleBucket("test", 1000.0, 1.0);
        long now = System.nanoTime();

        // 500 байт при лимите 1000 байт/сек и начальной емкости 1000 - без задержки
        assertEquals(0.0, bucket.calculateWait(500, now));
        bucket.consume(1000);

        // После списания 1000 токенов запрос 500 байт должен давать задержку ~0.5 сек
        double wait = bucket.calculateWait(500, now);
        assertEquals(0.5, Math.round(wait * 10.0) / 10.0);
    }

    @Test
    @DisplayName("Throttler должен учитывать глобальный лимит и отдавать рассчитанное время ожидания")
    void shouldEnforceGlobalLimit() {
        ReplicatorProperties props = new ReplicatorProperties();
        props.setGlobalLimitBytesPerSec(10_000_000); // 10 MB/s

        TopologyRegistry topology = new TopologyRegistry(props);
        TokenBucketThrottler throttler = new TokenBucketThrottler(props, topology);

        assertEquals(10_000_000.0, throttler.getGlobalLimit());

        // Запрос в пределах емкости
        double wait1 = throttler.requestTokens(1_000_000, "dc1", "dc2");
        assertEquals(0.0, wait1);

        // Изменение глобального лимита
        throttler.setGlobalLimit(20_000_000);
        assertEquals(20_000_000.0, throttler.getGlobalLimit());
    }

    @Test
    @DisplayName("P1: Параллельный запрос токенов из нескольких потоков без дедлоков и гонок")
    void shouldHandleConcurrentRequestsSafely() throws InterruptedException {
        ReplicatorProperties props = new ReplicatorProperties();
        props.setGlobalLimitBytesPerSec(100_000_000); // 100 MB/s

        TopologyRegistry topology = new TopologyRegistry(props);
        TokenBucketThrottler throttler = new TokenBucketThrottler(props, topology);

        int threadCount = 20;
        int requestsPerThread = 50;
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(threadCount);
        java.util.concurrent.atomic.AtomicInteger errorCount = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final String src = (i % 2 == 0) ? "dc1" : "dc2";
            final String dst = (i % 2 == 0) ? "dc2" : "dc1";
            executor.submit(() -> {
                try {
                    for (int j = 0; j < requestsPerThread; j++) {
                        double wait = throttler.requestTokens(10_000, src, dst);
                        if (wait < 0) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean finished = latch.await(10, java.util.concurrent.TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(finished, "Все параллельные запросы токенов должны завершиться без зависаний");
        assertEquals(0, errorCount.get(), "Не должно быть ошибок при конкурентном запросе токенов");
    }
}
