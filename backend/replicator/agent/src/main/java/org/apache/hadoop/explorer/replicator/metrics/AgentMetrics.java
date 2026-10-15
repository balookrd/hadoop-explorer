package org.apache.hadoop.explorer.replicator.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Централизованный сервис сбора и учета метрик Micrometer в Replicator Agent:
 * - Эффективность сжатия (compression_ratio, raw/compressed bytes)
 * - Гистограмма задержек передачи чанков gRPC (transfer_latency_histogram)
 * - Очередь и объем активных staging-файлов (staging_queue_size, staging_active_bytes)
 */
public class AgentMetrics {

    private static volatile AgentMetrics INSTANCE = new AgentMetrics();

    public static AgentMetrics getInstance() {
        return INSTANCE;
    }

    public static synchronized void resetInstanceForTest() {
        INSTANCE = new AgentMetrics();
    }

    private final MeterRegistry registry;

    // --- 1. Сжатие трафика (Wire Compression) ---
    private final Counter rawBytesCounter;
    private final Counter compressedBytesCounter;
    private final AtomicLong totalRawBytes = new AtomicLong(0);
    private final AtomicLong totalCompressedBytes = new AtomicLong(0);
    private final Map<String, AtomicLong> rawByCodec = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> compressedByCodec = new ConcurrentHashMap<>();

    // --- 2. Задержка передачи gRPC чанков ---
    private final Timer chunkTransferTimer;
    private final AtomicLong chunkTransferCount = new AtomicLong(0);
    private final AtomicLong chunkTransferNanosSum = new AtomicLong(0);
    private final AtomicLong maxChunkLatencyNanos = new AtomicLong(0);

    // --- 3. Очередь и файлы Staging ---
    private final AtomicInteger activeStagingFiles = new AtomicInteger(0);
    private final AtomicLong activeStagingBytes = new AtomicLong(0);
    private final Counter orphansCleanedCounter;
    private final AtomicLong totalOrphansCleaned = new AtomicLong(0);

    // --- 4. Отмены gRPC стримов ---
    private final Counter cancelledTransfersCounter;
    private final AtomicLong totalCancelledTransfers = new AtomicLong(0);

    public AgentMetrics() {
        this(new SimpleMeterRegistry());
    }

    public AgentMetrics(MeterRegistry registry) {
        this.registry = registry != null ? registry : new SimpleMeterRegistry();

        // Регистрация метрик сжатия в Micrometer
        this.rawBytesCounter = Counter.builder("replicator.compression.raw.bytes")
                .description("Суммарный объем данных до сжатия в байтах")
                .baseUnit("bytes")
                .register(this.registry);

        this.compressedBytesCounter = Counter.builder("replicator.compression.compressed.bytes")
                .description("Суммарный объем сжатых данных в канале (сетевой трафик)")
                .baseUnit("bytes")
                .register(this.registry);

        Gauge.builder("replicator.compression.ratio", this, AgentMetrics::getCompressionRatio)
                .description("Эффективный коэффициент сжатия (raw_bytes / compressed_bytes)")
                .register(this.registry);

        // Регистрация таймера сжатия/передачи чанков с SLA-бакетами и перцентилями
        this.chunkTransferTimer = Timer.builder("replicator.grpc.chunk.transfer.latency")
                .description("Гистограмма задержки передачи чанка данных через gRPC канал")
                .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99)
                .publishPercentileHistogram()
                .sla(
                        Duration.ofMillis(1),
                        Duration.ofMillis(5),
                        Duration.ofMillis(10),
                        Duration.ofMillis(25),
                        Duration.ofMillis(50),
                        Duration.ofMillis(100),
                        Duration.ofMillis(250),
                        Duration.ofMillis(500),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(5)
                )
                .register(this.registry);

        // Регистрация очереди staging в Micrometer
        Gauge.builder("replicator.staging.queue.size", activeStagingFiles, AtomicInteger::doubleValue)
                .description("Количество активных файлов в процессе staging на целевом кластере")
                .register(this.registry);

        Gauge.builder("replicator.staging.active.bytes", activeStagingBytes, AtomicLong::doubleValue)
                .description("Текущий совокупный объем файлов в staging-буфере в байтах")
                .baseUnit("bytes")
                .register(this.registry);

        this.orphansCleanedCounter = Counter.builder("replicator.staging.orphans.cleaned")
                .description("Количество удаленных осиротевших staging-файлов сборщиком мусора")
                .register(this.registry);

        this.cancelledTransfersCounter = Counter.builder("replicator.grpc.transfers.cancelled")
                .description("Количество отмененных клиентом или прерванных gRPC передач данных")
                .register(this.registry);
    }

    public MeterRegistry getRegistry() {
        return registry;
    }

    // =========================================================================
    // Методы учета сжатия
    // =========================================================================

    public void recordCompression(String codec, long raw, long compressed) {
        if (raw <= 0 && compressed <= 0) return;
        totalRawBytes.addAndGet(raw);
        totalCompressedBytes.addAndGet(compressed);
        rawBytesCounter.increment(raw);
        compressedBytesCounter.increment(compressed);

        String key = (codec != null && !codec.isBlank()) ? codec.toLowerCase() : "none";
        rawByCodec.computeIfAbsent(key, k -> new AtomicLong(0)).addAndGet(raw);
        compressedByCodec.computeIfAbsent(key, k -> new AtomicLong(0)).addAndGet(compressed);
    }

    public double getCompressionRatio() {
        long raw = totalRawBytes.get();
        long comp = totalCompressedBytes.get();
        if (raw == 0L || comp == 0L) {
            return 1.0;
        }
        return (double) raw / Math.max(1L, comp);
    }

    public long getTotalRawBytes() {
        return totalRawBytes.get();
    }

    public long getTotalCompressedBytes() {
        return totalCompressedBytes.get();
    }

    // =========================================================================
    // Методы учета задержки передачи чанков
    // =========================================================================

    public void recordChunkTransferLatency(long durationNanos) {
        if (durationNanos < 0) return;
        chunkTransferTimer.record(durationNanos, TimeUnit.NANOSECONDS);
        chunkTransferCount.incrementAndGet();
        chunkTransferNanosSum.addAndGet(durationNanos);
        maxChunkLatencyNanos.accumulateAndGet(durationNanos, Math::max);
    }

    public long getChunkTransferCount() {
        return chunkTransferCount.get();
    }

    public double getChunkLatencyAvgMs() {
        long count = chunkTransferCount.get();
        if (count == 0) return 0.0;
        double sumNanos = chunkTransferNanosSum.get();
        return (sumNanos / count) / 1_000_000.0;
    }

    public double getChunkLatencyMaxMs() {
        return maxChunkLatencyNanos.get() / 1_000_000.0;
    }

    public Timer getChunkTransferTimer() {
        return chunkTransferTimer;
    }

    // =========================================================================
    // Методы учета очереди staging
    // =========================================================================

    public void recordStagingFileStart() {
        activeStagingFiles.incrementAndGet();
    }

    public void recordStagingBytes(long deltaBytes) {
        if (deltaBytes > 0) {
            activeStagingBytes.addAndGet(deltaBytes);
        }
    }

    public void recordStagingFileComplete(long totalFileBytes) {
        activeStagingFiles.decrementAndGet();
        if (activeStagingFiles.get() < 0) {
            activeStagingFiles.set(0);
        }
        if (totalFileBytes > 0) {
            activeStagingBytes.addAndGet(-totalFileBytes);
            if (activeStagingBytes.get() < 0) {
                activeStagingBytes.set(0);
            }
        }
    }

    public void recordStagingOrphansCleaned(int count) {
        if (count > 0) {
            totalOrphansCleaned.addAndGet(count);
            orphansCleanedCounter.increment(count);
        }
    }

    public int getActiveStagingFiles() {
        return Math.max(0, activeStagingFiles.get());
    }

    public long getActiveStagingBytes() {
        return Math.max(0L, activeStagingBytes.get());
    }

    public long getTotalOrphansCleaned() {
        return totalOrphansCleaned.get();
    }

    public void recordTransferCancelled() {
        totalCancelledTransfers.incrementAndGet();
        cancelledTransfersCounter.increment();
    }

    public long getTotalCancelledTransfers() {
        return totalCancelledTransfers.get();
    }

    public void reset() {
        totalRawBytes.set(0);
        totalCompressedBytes.set(0);
        rawByCodec.clear();
        compressedByCodec.clear();
        chunkTransferCount.set(0);
        chunkTransferNanosSum.set(0);
        maxChunkLatencyNanos.set(0);
        activeStagingFiles.set(0);
        activeStagingBytes.set(0);
        totalOrphansCleaned.set(0);
        totalCancelledTransfers.set(0);
    }
}
