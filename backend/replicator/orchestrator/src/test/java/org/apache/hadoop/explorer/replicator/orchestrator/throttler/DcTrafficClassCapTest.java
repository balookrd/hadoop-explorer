package org.apache.hadoop.explorer.replicator.orchestrator.throttler;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Class shares inside a DC-DC link are hard caps: an idle class does not lend its share.
 */
@DisplayName("Токен-бакет: доли HDFS и Kafka в канале DC-DC — потолок без заимствования")
class DcTrafficClassCapTest {

    private static final long DC_LIMIT = 10_000_000L;

    private TopologyRegistry topology;
    private TokenBucketThrottler throttler;

    @BeforeEach
    void setUp() {
        ReplicatorProperties properties = new ReplicatorProperties();
        properties.setGlobalLimitBytesPerSec(1024L * 1024 * 1024);
        topology = new TopologyRegistry(properties);
        throttler = new TokenBucketThrottler(properties, topology);
    }

    @Test
    @DisplayName("HDFS без Kafka получает не больше 60% канала")
    void hdfsAloneIsCappedAtSixtyPercent() {
        topology.setDcLimit("dc1", "dc2", DC_LIMIT);

        assertEquals(0.0, throttler.requestTokens(6_000_000, "dc1", "dc2", TokenBucketThrottler.TrafficClass.HDFS));
        // Link bucket still holds 4 MB; the 6 MB/s class bucket is empty: 3 MB / 6 MB/s = 0.5 s
        double wait = throttler.requestTokens(3_000_000, "dc1", "dc2", TokenBucketThrottler.TrafficClass.HDFS);
        assertEquals(0.5, wait, 0.05);
    }

    @Test
    @DisplayName("Kafka без HDFS получает не больше 40% канала")
    void kafkaAloneIsCappedAtFortyPercent() {
        topology.setDcLimit("dc1", "dc2", DC_LIMIT);

        assertEquals(0.0, throttler.requestTokens(4_000_000, "dc1", "dc2", TokenBucketThrottler.TrafficClass.KAFKA));
        double wait = throttler.requestTokens(2_000_000, "dc1", "dc2", TokenBucketThrottler.TrafficClass.KAFKA);
        assertEquals(0.5, wait, 0.05);
    }

    @Test
    @DisplayName("Ненастроенный канал: 100 МБ/с по умолчанию, HDFS упирается в 60 МБ/с")
    void unconfiguredLinkCapsHdfsAtSixtyPercentOfDefault() {
        long hdfsCap = (long) (100L * 1024 * 1024 * 0.60); // 62_914_560

        assertEquals(0.0, throttler.requestTokens(hdfsCap, "dc1", "dc2", TokenBucketThrottler.TrafficClass.HDFS));
        double wait = throttler.requestTokens(hdfsCap / 2, "dc1", "dc2", TokenBucketThrottler.TrafficClass.HDFS);
        assertEquals(0.5, wait, 0.05);
    }
}
