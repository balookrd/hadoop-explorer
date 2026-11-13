package io.github.balookrd.pristan.parom.orchestrator.throttler;

import io.github.balookrd.pristan.parom.orchestrator.config.ParomProperties;
import io.github.balookrd.pristan.parom.orchestrator.topology.TopologyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Иерархический Token Bucket Throttler полосы пропускания репликации (WAN / DC-DC / HDFS-HDFS).
 * Использует ReentrantLock вместо synchronized для предотвращения thread pinning на Java 21 Virtual Threads.
 */
@Component
public class TokenBucketThrottler {

    /**
     * Ожидание, выдаваемое при перекрытом канале (лимит равен нулю).
     *
     * <p>Контракт выдачи токенов — время ожидания в секундах, поэтому запрет выражается
     * заведомо большим ожиданием: агент выдерживает паузу и повторяет запрос, то есть канал
     * фактически закрыт, пока ограждение не снято.
     */
    public static final double BLOCKED_WAIT_SECONDS = 60.0;

    private static final Logger log = LoggerFactory.getLogger(TokenBucketThrottler.class);

    public static class SingleBucket {
        private final String name;
        private final ReentrantLock lock = new ReentrantLock();
        private double configuredLimitBytesPerSec;
        private int activeNodeCount = 1;
        private double limitBytesPerSec;
        private double burstSeconds = 1.0;
        private double capacity;
        private double tokens;
        private long lastUpdateTimeNanos;

        public SingleBucket(String name, double configuredLimitBytesPerSec, double burstSeconds) {
            this(name, configuredLimitBytesPerSec, burstSeconds, 1);
        }

        public SingleBucket(String name, double configuredLimitBytesPerSec, double burstSeconds, int activeNodes) {
            this.name = name;
            this.configuredLimitBytesPerSec = configuredLimitBytesPerSec;
            this.burstSeconds = Math.max(0.1, burstSeconds);
            this.activeNodeCount = Math.max(1, activeNodes);
            this.limitBytesPerSec = configuredLimitBytesPerSec > 0 ? configuredLimitBytesPerSec / this.activeNodeCount : 0.0;
            this.capacity = limitBytesPerSec > 0 ? limitBytesPerSec * this.burstSeconds : 0.0;
            this.tokens = this.capacity;
            this.lastUpdateTimeNanos = System.nanoTime();
        }

        public void setLimit(double newConfiguredLimit) {
            lock.lock();
            try {
                this.configuredLimitBytesPerSec = newConfiguredLimit;
                recalculateEffectiveLimitLocked();
            } finally {
                lock.unlock();
            }
        }

        public void setActiveNodeCount(int count) {
            lock.lock();
            try {
                int sanitized = Math.max(1, count);
                if (this.activeNodeCount != sanitized) {
                    this.activeNodeCount = sanitized;
                    recalculateEffectiveLimitLocked();
                }
            } finally {
                lock.unlock();
            }
        }

        private void recalculateEffectiveLimitLocked() {
            this.limitBytesPerSec = this.configuredLimitBytesPerSec > 0
                    ? this.configuredLimitBytesPerSec / this.activeNodeCount
                    : 0.0;
            if (this.limitBytesPerSec > 0) {
                this.capacity = this.limitBytesPerSec * this.burstSeconds;
                this.tokens = Math.min(this.tokens, this.capacity);
            } else {
                this.capacity = 0.0;
                this.tokens = 0.0;
            }
            this.lastUpdateTimeNanos = System.nanoTime();
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
                if (requestedBytes <= 0) {
                    return 0.0;
                }
                if (limitBytesPerSec == 0) {
                    return BLOCKED_WAIT_SECONDS;
                }
                if (limitBytesPerSec < 0) {
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
                if (requestedBytes <= 0) {
                    return 0.0;
                }
                // Ноль означает «канал перекрыт» (сетевое ограждение Kill-Switch), а не
                // «лимита нет»: прежняя проверка limitBytesPerSec <= 0 возвращала нулевое
                // ожидание, и агенты продолжали качать данные на полной скорости.
                if (limitBytesPerSec == 0) {
                    return BLOCKED_WAIT_SECONDS;
                }
                if (limitBytesPerSec < 0) {
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

        public void updateLimit(double newConfiguredLimit) {
            lock.lock();
            try {
                if (Math.abs(this.configuredLimitBytesPerSec - newConfiguredLimit) > 1e-6) {
                    this.configuredLimitBytesPerSec = newConfiguredLimit;
                    recalculateEffectiveLimitLocked();
                }
            } finally {
                lock.unlock();
            }
        }

        public double getConfiguredLimitBytesPerSec() {
            lock.lock();
            try {
                return configuredLimitBytesPerSec;
            } finally {
                lock.unlock();
            }
        }

        public double getLimitBytesPerSec() {
            lock.lock();
            try {
                return limitBytesPerSec;
            } finally {
                lock.unlock();
            }
        }

        public int getActiveNodeCount() {
            lock.lock();
            try {
                return activeNodeCount;
            } finally {
                lock.unlock();
            }
        }

        public String getName() { return name; }
    }

    public enum TrafficClass {
        HDFS,
        /** Not used by the gRPC path ({@code TokenRequest} has no traffic class); throttler API and tests only. */
        KAFKA
    }

    private final TopologyRegistry topology;
    private final SingleBucket globalBucket;
    private final Map<String, SingleBucket> dcBuckets = new ConcurrentHashMap<>();
    private final Map<String, SingleBucket> dcKafkaBuckets = new ConcurrentHashMap<>();
    private final Map<String, SingleBucket> dcHdfsBuckets = new ConcurrentHashMap<>();
    private final Map<String, SingleBucket> hdfsBuckets = new ConcurrentHashMap<>();
    private volatile int activeNodeCount = 1;

    public TokenBucketThrottler(ParomProperties properties, TopologyRegistry topology) {
        this.topology = topology;
        this.globalBucket = new SingleBucket("global", properties.getGlobalLimitBytesPerSec(), 1.0, 1);
    }

    public void updateGlobalLimit(long newLimitBytesPerSec) {
        globalBucket.updateLimit(newLimitBytesPerSec);
    }

    /**
     * Динамическое масштабирование локальных квот бакетов пропорционально
     * числу активных узлов Orchestrator в Active-Active пуле.
     */
    public void rebalanceQuotas(int activeNodes) {
        int sanitized = Math.max(1, activeNodes);
        this.activeNodeCount = sanitized;
        globalBucket.setActiveNodeCount(sanitized);
        for (SingleBucket b : dcBuckets.values()) {
            b.setActiveNodeCount(sanitized);
        }
        for (SingleBucket b : dcKafkaBuckets.values()) {
            b.setActiveNodeCount(sanitized);
        }
        for (SingleBucket b : dcHdfsBuckets.values()) {
            b.setActiveNodeCount(sanitized);
        }
        for (SingleBucket b : hdfsBuckets.values()) {
            b.setActiveNodeCount(sanitized);
        }
        log.debug("TokenBucket квоты успешно перебалансированы для {} активных узлов Orchestrator", sanitized);
    }

    public int getActiveNodeCount() {
        return activeNodeCount;
    }

    public double getGlobalConfiguredLimit() {
        return globalBucket.getConfiguredLimitBytesPerSec();
    }

    /**
     * Throttler answer to a token request.
     *
     * @param waitSeconds how long the caller waits before sending (or before asking again when blocked)
     * @param blocked     a bucket on the path has limit 0: nothing was charged and nothing may be sent
     */
    public record TokenDecision(double waitSeconds, boolean blocked) {}

    public TokenDecision acquireTokens(long requestedBytes, String srcCluster, String dstCluster) {
        return acquireTokens(requestedBytes, srcCluster, dstCluster, TrafficClass.HDFS);
    }

    public double requestTokens(long requestedBytes, String srcCluster, String dstCluster) {
        return requestTokens(requestedBytes, srcCluster, dstCluster, TrafficClass.HDFS);
    }

    /**
     * Hierarchical token request (HTB without borrowing):
     * <ol>
     *   <li>Global bucket: limit across all DC links.</li>
     *   <li>DC link bucket ({@code dc:srcDc->dstDc}): keeps the WAN link within its limit. Without
     *       an explicit limit {@link TopologyRegistry#getDcLimit} returns 100 MB/s.</li>
     *   <li>Traffic class bucket, a hard cap inside the link: HDFS up to 60% of the link
     *       ({@code dcHdfsBuckets}), KAFKA up to 40% ({@code dcKafkaBuckets}). The share is not
     *       lent: HDFS gets at most 60% of the link even when Kafka is idle.</li>
     *   <li>HDFS cluster-pair bucket ({@code hdfs:src->dst}) when a pair limit is configured.</li>
     * </ol>
     * Every bucket on the path is charged; the returned wait is the maximum over them, in seconds.
     * If any bucket on the path has limit 0 the link is closed: nothing is charged and the answer
     * is {@code blocked} with {@link #BLOCKED_WAIT_SECONDS}. Agents ask again while the link is
     * closed, so charging the other buckets would leave a debt to pay off after the fence is lifted.
     *
     * <p>Only HDFS reaches this method in production: the gRPC {@code RequestTokens} call
     * ({@code OrchestratorControlPlaneServiceImpl}) uses the three-argument overload, and
     * {@code TokenRequest} in {@code orchestrator_control_plane.proto} carries no traffic class.
     * The KAFKA branch is reachable only through this API directly (tests). Kafka jobs are limited
     * by their {@code bandwidth_limit_mb}, which {@code KafkaReplicationOrchestratorService} derives
     * from 40% of an explicitly configured link limit when the request leaves it empty.
     */
    public double requestTokens(long requestedBytes, String srcCluster, String dstCluster, TrafficClass trafficClass) {
        return acquireTokens(requestedBytes, srcCluster, dstCluster, trafficClass).waitSeconds();
    }

    public TokenDecision acquireTokens(long requestedBytes, String srcCluster, String dstCluster, TrafficClass trafficClass) {
        if (requestedBytes <= 0) {
            return new TokenDecision(0.0, false);
        }

        TrafficClass effectiveClass = trafficClass != null ? trafficClass : TrafficClass.HDFS;
        long nowNanos = System.nanoTime();
        List<SingleBucket> path = new ArrayList<>(4);
        path.add(globalBucket);

        String dcKey = null;
        String srcDc = null;
        String dstDc = null;
        if (srcCluster != null && dstCluster != null) {
            var srcC = topology.getCluster(srcCluster);
            var dstC = topology.getCluster(dstCluster);
            if (srcC.isPresent() && dstC.isPresent()) {
                srcDc = srcC.get().getDcId();
                dstDc = dstC.get().getDcId();
            } else {
                for (var dc : topology.getDatacenters()) {
                    if (dc.getId().equalsIgnoreCase(srcCluster)) srcDc = dc.getId();
                    if (dc.getId().equalsIgnoreCase(dstCluster)) dstDc = dc.getId();
                }
                if (srcDc == null) srcDc = srcCluster;
                if (dstDc == null) dstDc = dstCluster;
            }
            dcKey = srcDc + "->" + dstDc;
        }

        if (dcKey != null && srcDc != null && dstDc != null && topology.getDcLimit(srcDc, dstDc).isPresent()) {
            long limit = topology.getDcLimit(srcDc, dstDc).get();
            SingleBucket dcBucket = dcBuckets.computeIfAbsent(dcKey, k -> new SingleBucket("dc:" + k, limit, 1.0, activeNodeCount));
            dcBucket.updateLimit(limit);
            // Link bucket charged for every traffic class passed in; over gRPC that is HDFS only
            path.add(dcBucket);

            // Traffic class caps; unused share is not lent to the other class
            if (effectiveClass == TrafficClass.KAFKA) {
                long kafkaFloorLimit = (long) (limit * 0.40); // hard cap: 40% of the link for Kafka
                SingleBucket kafkaBucket = dcKafkaBuckets.computeIfAbsent(dcKey, k -> new SingleBucket("dc-kafka:" + k, kafkaFloorLimit, 1.0, activeNodeCount));
                kafkaBucket.updateLimit(kafkaFloorLimit);
                path.add(kafkaBucket);
            } else {
                long hdfsFloorLimit = (long) (limit * 0.60); // hard cap: 60% of the link for HDFS
                SingleBucket hdfsBucket = dcHdfsBuckets.computeIfAbsent(dcKey, k -> new SingleBucket("dc-hdfs:" + k, hdfsFloorLimit, 1.0, activeNodeCount));
                hdfsBucket.updateLimit(hdfsFloorLimit);
                path.add(hdfsBucket);
            }
        }

        if (effectiveClass == TrafficClass.HDFS) {
            String hdfsKey = (srcCluster != null && dstCluster != null) ? srcCluster + "->" + dstCluster : null;
            if (hdfsKey != null && topology.getHdfsLimit(srcCluster, dstCluster).isPresent()) {
                long limit = topology.getHdfsLimit(srcCluster, dstCluster).get();
                SingleBucket hdfsBucket = hdfsBuckets.computeIfAbsent(hdfsKey, k -> new SingleBucket("hdfs:" + k, limit, 1.0, activeNodeCount));
                hdfsBucket.updateLimit(limit);
                path.add(hdfsBucket);
            }
        }

        for (SingleBucket bucket : path) {
            if (bucket.getLimitBytesPerSec() == 0) {
                log.debug("Throttler [{}] link closed by bucket '{}' (src={}, dst={})",
                        effectiveClass, bucket.getName(), srcCluster, dstCluster);
                return new TokenDecision(BLOCKED_WAIT_SECONDS, true);
            }
        }

        // Total wait: maximum over the hierarchy (global, DC link, traffic class, cluster pair)
        double maxWait = 0.0;
        for (SingleBucket bucket : path) {
            maxWait = Math.max(maxWait, bucket.calculateWaitAndConsume(requestedBytes, nowNanos));
        }

        if (maxWait > 0.05) {
            log.debug("Throttler [{}] requested {} bytes (src={}, dst={}): wait {} s",
                effectiveClass, requestedBytes, srcCluster, dstCluster, Math.round(maxWait * 1000.0) / 1000.0);
        }

        return new TokenDecision(maxWait, false);
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
