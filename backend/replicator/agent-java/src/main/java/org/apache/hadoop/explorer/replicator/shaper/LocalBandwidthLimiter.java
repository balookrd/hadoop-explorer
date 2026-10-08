package org.apache.hadoop.explorer.replicator.shaper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Локальный Token Bucket шейпер для ограничения пропускной способности агента репликации.
 *
 * <p>Защищает сетевые интерфейсы и дисковую подсистему узла Hadoop (DataNode, Edge Node)
 * от насыщения фоновым репликационным трафиком. Полный аналог Python-версии LocalBandwidthLimiter.
 */
public class LocalBandwidthLimiter {

    private static final Logger logger = LoggerFactory.getLogger(LocalBandwidthLimiter.class);

    private volatile double limitBytesPerSec;
    private final double burstSeconds;
    private double capacity;
    private double tokens;
    private long lastUpdateNano;
    private final Object lock = new Object();

    public LocalBandwidthLimiter(double limitMbPerSec) {
        this(limitMbPerSec, 0.5);
    }

    public LocalBandwidthLimiter(double limitMbPerSec, double burstSeconds) {
        this.burstSeconds = Math.max(0.1, burstSeconds);
        setLimit(limitMbPerSec);
    }

    public boolean isEnabled() {
        return limitBytesPerSec > 0;
    }

    public double getLimitMbPerSec() {
        return Math.round((limitBytesPerSec / (1024.0 * 1024.0)) * 100.0) / 100.0;
    }

    /**
     * Динамическое изменение лимита пропускной способности (в МБ/с).
     */
    public void setLimit(double limitMbPerSec) {
        synchronized (lock) {
            this.limitBytesPerSec = Math.max(0.0, limitMbPerSec) * 1024.0 * 1024.0;
            if (this.limitBytesPerSec > 0) {
                this.capacity = this.limitBytesPerSec * this.burstSeconds;
                this.tokens = this.capacity;
            } else {
                this.capacity = 0.0;
                this.tokens = 0.0;
            }
            this.lastUpdateNano = System.nanoTime();
        }
    }

    /**
     * Сдерживает скорость при передаче или приеме порции данных, если лимит превышен.
     *
     * @param bytesCount количество переданных/принятых байт
     * @return время ожидания в секундах
     */
    public double throttle(long bytesCount) {
        if (!isEnabled() || bytesCount <= 0) {
            return 0.0;
        }

        double waitSeconds = 0.0;
        synchronized (lock) {
            long now = System.nanoTime();
            double elapsedSec = (now - lastUpdateNano) / 1_000_000_000.0;
            if (elapsedSec > 0) {
                this.tokens = Math.min(this.capacity, this.tokens + elapsedSec * this.limitBytesPerSec);
                this.lastUpdateNano = now;
            }

            if (this.tokens >= bytesCount) {
                this.tokens -= bytesCount;
                return 0.0;
            }

            double deficit = bytesCount - this.tokens;
            waitSeconds = deficit / this.limitBytesPerSec;
            this.tokens -= bytesCount;
        }

        if (waitSeconds > 0) {
            try {
                long millis = (long) (waitSeconds * 1000);
                int nanos = (int) ((waitSeconds * 1000 - millis) * 1_000_000);
                Thread.sleep(millis, nanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Прервано ожидание троттлера полосы: {}", e.getMessage());
            }
        }

        return waitSeconds;
    }
}
