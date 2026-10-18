package org.apache.hadoop.explorer.replicator.orchestrator.throttler;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Иерархический Token Bucket Throttler полосы пропускания репликации (WAN / DC-DC / HDFS-HDFS).
 * Использует ReentrantLock вместо synchronized для предотвращения thread pinning на Java 21 Virtual Threads.
 */
@Component
public class TokenBucketThrottler {

    private static final Logger log = LoggerFactory.getLogger(TokenBucketThrottler.class);

    public static class SingleBucket {
        private final String name;
        private final ReentrantLock lock = new ReentrantLock();
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

        public void setLimit(double newLimit) {
            lock.lock();
            try {
                this.limitBytesPerSec = newLimit;
                if (newLimit > 0) {
                    this.capacity = newLimit * burstSeconds;
                    this.tokens = Math.min(this.tokens, this.capacity);
                } else {
                    this.capacity = 0.0;
                    this.tokens = 0.0;
                }
                this.lastUpdateTimeNanos = System.nanoTime();
            } finally {
                lock.unlock();
            }
        }

        public void replenish(long nowNanos) {
            lock.lock();
            try {
                if (limitBytesPerSec <= 0) return;
                double elapsedSec = (nowNanos - lastUpdateTimeNanos) / 1_000_000_000.0;
                if (elapsedSec > 0) {
                    double newTokens = elapsedSec * limitBytesPerSec;
                    this.tokens = Math.min(capacity, this.tokens + newTokens);
                    this.lastUpdateTimeNanos = nowNanos;
                }
            } finally {
                lock.unlock();
            }
        }

        public double calculateWait(long requestedBytes, long nowNanos) {
            lock.lock();
            try {
                if (requestedBytes <= 0 || limitBytesPerSec <= 0) {
                    return 0.0;
                }
                if (limitBytesPerSec > 0) {
                    double elapsedSec = (nowNanos - lastUpdateTimeNanos) / 1_000_000_000.0;
                    if (elapsedSec > 0) {
                        double newTokens = elapsedSec * limitBytesPerSec;
                        this.tokens = Math.min(capacity, this.tokens + newTokens);
                        this.lastUpdateTimeNanos = nowNanos;
                    }
                }
                if (tokens >= requestedBytes) {
                    return 0.0;
                }
                double deficit = requestedBytes - tokens;
                return deficit / limitBytesPerSec;
            } finally {
                lock.unlock();
            }
        }

        public void consume(long requestedBytes) {
            lock.lock();
            try {
                if (limitBytesPerSec > 0 && requestedBytes > 0) {
                    this.tokens -= requestedBytes;
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Атомарный расчет задержки и списание токенов под единой блокировкой конкретного бакета
         * без необходимости глобальной сериализации всего оркестратора.
         */
        public double calculateWaitAndConsume(long requestedBytes, long nowNanos) {
            lock.lock();
            try {
                if (requestedBytes <= 0 || limitBytesPerSec <= 0) {
                    return 0.0;
                }
                double elapsedSec = (nowNanos - lastUpdateTimeNanos) / 1_000_000_000.0;
                if (elapsedSec > 0) {
                    double newTokens = elapsedSec * limitBytesPerSec;
                    this.tokens = Math.min(capacity, this.tokens + newTokens);
                    this.lastUpdateTimeNanos = nowNanos;
                }
                double wait = 0.0;
                if (tokens < requestedBytes) {
                    double deficit = requestedBytes - tokens;
                    wait = deficit / limitBytesPerSec;
                }
                this.tokens -= requestedBytes;
                return wait;
            } finally {
                lock.unlock();
            }
        }

        public void updateLimit(double newLimitBytesPerSec) {
            lock.lock();
            try {
                if (Math.abs(this.limitBytesPerSec - newLimitBytesPerSec) > 1e-6) {
                    this.limitBytesPerSec = newLimitBytesPerSec;
                    this.capacity = newLimitBytesPerSec * burstSeconds;
                    this.tokens = Math.min(this.tokens, this.capacity);
                }
            } finally {
                lock.unlock();
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

    public void updateGlobalLimit(long newLimitBytesPerSec) {
        globalBucket.updateLimit(newLimitBytesPerSec);
    }

    public double requestTokens(long requestedBytes, String srcCluster, String dstCluster) {
        if (requestedBytes <= 0) {
            return 0.0;
        }

        long nowNanos = System.nanoTime();
        double waitGlobal = globalBucket.calculateWaitAndConsume(requestedBytes, nowNanos);

        String dcKey = null;
        String srcDc = null;
        String dstDc = null;
        if (srcCluster != null && dstCluster != null) {
            var srcC = topology.getCluster(srcCluster);
            var dstC = topology.getCluster(dstCluster);
            if (srcC.isPresent() && dstC.isPresent()) {
                srcDc = srcC.get().getDcId();
                dstDc = dstC.get().getDcId();
                dcKey = srcDc + "->" + dstDc;
            }
        }

        double waitDc = 0.0;
        if (dcKey != null && srcDc != null && dstDc != null && topology.getDcLimit(srcDc, dstDc).isPresent()) {
            long limit = topology.getDcLimit(srcDc, dstDc).get();
            SingleBucket dcBucket = dcBuckets.computeIfAbsent(dcKey, k -> new SingleBucket("dc:" + k, limit, 1.0));
            dcBucket.updateLimit(limit);
            waitDc = dcBucket.calculateWaitAndConsume(requestedBytes, nowNanos);
        }

        String hdfsKey = (srcCluster != null && dstCluster != null) ? srcCluster + "->" + dstCluster : null;
        double waitHdfs = 0.0;
        if (hdfsKey != null && topology.getHdfsLimit(srcCluster, dstCluster).isPresent()) {
            long limit = topology.getHdfsLimit(srcCluster, dstCluster).get();
            SingleBucket hdfsBucket = hdfsBuckets.computeIfAbsent(hdfsKey, k -> new SingleBucket("hdfs:" + k, limit, 1.0));
            hdfsBucket.updateLimit(limit);
            waitHdfs = hdfsBucket.calculateWaitAndConsume(requestedBytes, nowNanos);
        }

        double maxWait = Math.max(waitGlobal, Math.max(waitDc, waitHdfs));

        if (maxWait > 0.05) {
            log.debug("Throttler requested {} bytes (src={}, dst={}): wait {} s",
                requestedBytes, srcCluster, dstCluster, Math.round(maxWait * 1000.0) / 1000.0);
        }

        return maxWait;
    }

    public void setGlobalLimit(long bytesPerSec) {
        globalBucket.setLimit(bytesPerSec);
        log.info("Global replication limit set to {} MB/s", bytesPerSec / (1024.0 * 1024.0));
    }

    public double getGlobalLimit() {
        return globalBucket.getLimitBytesPerSec();
    }

    public Map<String, SingleBucket> getDcBuckets() {
        return java.util.Collections.unmodifiableMap(dcBuckets);
    }

    public Map<String, SingleBucket> getHdfsBuckets() {
        return java.util.Collections.unmodifiableMap(hdfsBuckets);
    }
}
