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
}
