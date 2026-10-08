package org.apache.hadoop.explorer.replicator.orchestrator.throttler;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Иерархический Token Bucket Throttler полосы пропускания репликации (WAN / DC-DC / HDFS-HDFS).
 */
@Component
public class TokenBucketThrottler {

    private static final Logger log = LoggerFactory.getLogger(TokenBucketThrottler.class);

    public static class SingleBucket {
        private final String name;
        private double limitBytesPerSec;
        private double burstSeconds = 1.0;
        private double capacity;
        private double tokens;
        private long lastUpdateTimeNanos;

        public SingleBucket(String name, double limitBytesPerSec, double burstSeconds) {
            this.name = name;
            this.limitBytesPerSec = limitBytesPerSec;
            this.burstSeconds = Math.max(0.1, burstSeconds);
            this.capacity = limitBytesPerSec > 0 ? limitBytesPerSec * this.burstSeconds : 0.0;
            this.tokens = this.capacity;
            this.lastUpdateTimeNanos = System.nanoTime();
        }

        public synchronized void setLimit(double newLimit) {
            this.limitBytesPerSec = newLimit;
            if (newLimit > 0) {
                this.capacity = newLimit * burstSeconds;
                this.tokens = Math.min(this.tokens, this.capacity);
            } else {
                this.capacity = 0.0;
                this.tokens = 0.0;
            }
            this.lastUpdateTimeNanos = System.nanoTime();
        }

        public synchronized void replenish(long nowNanos) {
            if (limitBytesPerSec <= 0) return;
            double elapsedSec = (nowNanos - lastUpdateTimeNanos) / 1_000_000_000.0;
            if (elapsedSec > 0) {
                double newTokens = elapsedSec * limitBytesPerSec;
                this.tokens = Math.min(capacity, this.tokens + newTokens);
                this.lastUpdateTimeNanos = nowNanos;
            }
        }

        public synchronized double calculateWait(long requestedBytes, long nowNanos) {
            if (requestedBytes <= 0 || limitBytesPerSec <= 0) {
                return 0.0;
            }
            replenish(nowNanos);
            if (tokens >= requestedBytes) {
                return 0.0;
            }
            double deficit = requestedBytes - tokens;
            return deficit / limitBytesPerSec;
        }

        public synchronized void consume(long requestedBytes) {
            if (limitBytesPerSec > 0 && requestedBytes > 0) {
                this.tokens -= requestedBytes;
            }
        }

        public double getLimitBytesPerSec() { return limitBytesPerSec; }
        public String getName() { return name; }
    }

    private final TopologyRegistry topology;
    private final SingleBucket globalBucket;
    private final Map<String, SingleBucket> dcBuckets = new ConcurrentHashMap<>();
    private final Map<String, SingleBucket> hdfsBuckets = new ConcurrentHashMap<>();

    public TokenBucketThrottler(ReplicatorProperties properties, TopologyRegistry topology) {
        this.topology = topology;
        this.globalBucket = new SingleBucket("global", properties.getGlobalLimitBytesPerSec(), 1.0);
    }

    public synchronized double requestTokens(long requestedBytes, String srcCluster, String dstCluster) {
        if (requestedBytes <= 0) {
            return 0.0;
        }

        long nowNanos = System.nanoTime();
        double waitGlobal = globalBucket.calculateWait(requestedBytes, nowNanos);

        String dcKey = null;
        if (srcCluster != null && dstCluster != null) {
            var srcC = topology.getCluster(srcCluster);
            var dstC = topology.getCluster(dstCluster);
            if (srcC.isPresent() && dstC.isPresent()) {
                dcKey = srcC.get().getDcId() + "->" + dstC.get().getDcId();
            }
        }

        double waitDc = 0.0;
        SingleBucket dcBucket = null;
        if (dcKey != null && topology.getDcLimit(srcCluster, dstCluster).isPresent()) {
            long limit = topology.getDcLimit(srcCluster, dstCluster).get();
            dcBucket = dcBuckets.computeIfAbsent(dcKey, k -> new SingleBucket("dc:" + k, limit, 1.0));
            waitDc = dcBucket.calculateWait(requestedBytes, nowNanos);
        }

        String hdfsKey = (srcCluster != null && dstCluster != null) ? srcCluster + "->" + dstCluster : null;
        double waitHdfs = 0.0;
        SingleBucket hdfsBucket = null;
        if (hdfsKey != null && topology.getHdfsLimit(srcCluster, dstCluster).isPresent()) {
            long limit = topology.getHdfsLimit(srcCluster, dstCluster).get();
            hdfsBucket = hdfsBuckets.computeIfAbsent(hdfsKey, k -> new SingleBucket("hdfs:" + k, limit, 1.0));
            waitHdfs = hdfsBucket.calculateWait(requestedBytes, nowNanos);
        }

        double maxWait = Math.max(waitGlobal, Math.max(waitDc, waitHdfs));

        // Списываем токены
        globalBucket.consume(requestedBytes);
        if (dcBucket != null) dcBucket.consume(requestedBytes);
        if (hdfsBucket != null) hdfsBucket.consume(requestedBytes);

        if (maxWait > 0.05) {
            log.debug("Throttler requested {} bytes (src={}, dst={}): wait {} s",
                requestedBytes, srcCluster, dstCluster, Math.round(maxWait * 1000.0) / 1000.0);
        }

        return maxWait;
    }

    public synchronized void setGlobalLimit(long bytesPerSec) {
        globalBucket.setLimit(bytesPerSec);
        log.info("Global replication limit set to {} MB/s", bytesPerSec / (1024.0 * 1024.0));
    }

    public double getGlobalLimit() {
        return globalBucket.getLimitBytesPerSec();
    }
}
