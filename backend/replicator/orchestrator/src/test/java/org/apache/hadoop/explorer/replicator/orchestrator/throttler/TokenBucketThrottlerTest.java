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

    @Test
    @DisplayName("P4: Динамическое обновление лимитов DC на лету в TokenBucketThrottler")
    void shouldDynamicallyUpdateDcLimitsOnTheFly() {
        ReplicatorProperties props = new ReplicatorProperties();
        props.setGlobalLimitBytesPerSec(100_000_000); // 100 MB/s
        props.getClusters().add(new ReplicatorProperties.ClusterConfig("dc1", "Cluster 1", "dc1", "hdfs://nn1:8020", "agent1", 50051));
        props.getClusters().add(new ReplicatorProperties.ClusterConfig("dc2", "Cluster 2", "dc2", "hdfs://nn2:8020", "agent2", 50051));

        TopologyRegistry topology = new TopologyRegistry(props);
        topology.setDcLimit("dc1", "dc2", 10_000_000); // 10 MB/s начальный лимит

        TokenBucketThrottler throttler = new TokenBucketThrottler(props, topology);

        // Первый запрос: бакет создается с 10 МБ/с
        throttler.requestTokens(1000, "dc1", "dc2");

        // Меняем лимит в топологии на 50 МБ/с
        topology.setDcLimit("dc1", "dc2", 50_000_000);

        // Следующий запрос должен обновить существующий бакет
        throttler.requestTokens(1000, "dc1", "dc2");

        // Проверяем, что в dcBuckets обновился limitBytesPerSec
        TokenBucketThrottler.SingleBucket bucket = throttler.getDcBuckets().get("dc1->dc2");
        assertNotNull(bucket);
        assertEquals(50_000_000.0, bucket.getLimitBytesPerSec(), 1e-6);
    }

    @Test
    @DisplayName("HTB: Kafka и HDFS делят общий корневой бакет DC-to-DC и соблюдают лимит канала")
    void shouldShareDcBucketBetweenKafkaAndHdfs() {
        ReplicatorProperties props = new ReplicatorProperties();
        props.setGlobalLimitBytesPerSec(100_000_000); // 100 MB/s
        props.getClusters().add(new ReplicatorProperties.ClusterConfig("dc1", "Cluster 1", "dc1", "hdfs://nn1:8020", "agent1", 50051));
        props.getClusters().add(new ReplicatorProperties.ClusterConfig("dc2", "Cluster 2", "dc2", "hdfs://nn2:8020", "agent2", 50051));

        TopologyRegistry topology = new TopologyRegistry(props);
        topology.setDcLimit("dc1", "dc2", 1_000_000); // 1 MB/s лимит между DC1 и DC2

        TokenBucketThrottler throttler = new TokenBucketThrottler(props, topology);

        // Kafka делает запрос токенов
        double waitKafka = throttler.requestTokens(200_000, "dc1", "dc2", TokenBucketThrottler.TrafficClass.KAFKA);
        assertEquals(0.0, waitKafka, "Первый запрос Kafka в пределах емкости должен быть без задержки");

        // HDFS делает запрос токенов
        double waitHdfs = throttler.requestTokens(300_000, "dc1", "dc2", TokenBucketThrottler.TrafficClass.HDFS);
        assertEquals(0.0, waitHdfs, "Запрос HDFS в пределах емкости должен быть без задержки");

        // Оба списали токены из общего корневого dcBucket (всего списано 500_000 из 1_000_000)
        TokenBucketThrottler.SingleBucket dcBucket = throttler.getDcBuckets().get("dc1->dc2");
        assertNotNull(dcBucket);

        // Когда суммарный трафик превышает емкость канала (еще 600_000 байт при остатке ~500_000), появляется задержка
        double waitExcess = throttler.requestTokens(600_000, "dc1", "dc2", TokenBucketThrottler.TrafficClass.KAFKA);
        assertTrue(waitExcess > 0.0, "При исчерпании емкости общего канала DC-to-DC должна возникнуть задержка (throttling)");
    }
}
