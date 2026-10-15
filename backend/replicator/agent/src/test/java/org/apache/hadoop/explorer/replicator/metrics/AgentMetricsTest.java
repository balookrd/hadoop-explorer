package org.apache.hadoop.explorer.replicator.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class AgentMetricsTest {

    private AgentMetrics metrics;

    @BeforeEach
    void setUp() {
        AgentMetrics.resetInstanceForTest();
        metrics = AgentMetrics.getInstance();
    }

    @Test
    @DisplayName("Сжатие трафика: корректный расчет compression_ratio и счетчиков Micrometer")
    void testWireCompressionMetrics() {
        assertEquals(1.0, metrics.getCompressionRatio(), 1e-6);
        assertEquals(0L, metrics.getTotalRawBytes());
        assertEquals(0L, metrics.getTotalCompressedBytes());

        // Записываем 100 КБ исходных данных и 25 КБ сжатых zstd
        metrics.recordCompression("zstd", 102400L, 25600L);
        assertEquals(102400L, metrics.getTotalRawBytes());
        assertEquals(25600L, metrics.getTotalCompressedBytes());
        assertEquals(4.0, metrics.getCompressionRatio(), 1e-6);

        // Проверяем Micrometer MeterRegistry
        Counter rawCounter = metrics.getRegistry().find("replicator.compression.raw.bytes").counter();
        assertNotNull(rawCounter);
        assertEquals(102400.0, rawCounter.count(), 1e-6);

        Counter compCounter = metrics.getRegistry().find("replicator.compression.compressed.bytes").counter();
        assertNotNull(compCounter);
        assertEquals(25600.0, compCounter.count(), 1e-6);

        Gauge ratioGauge = metrics.getRegistry().find("replicator.compression.ratio").gauge();
        assertNotNull(ratioGauge);
        assertEquals(4.0, ratioGauge.value(), 1e-6);

        // Дополнительный чанк без сжатия
        metrics.recordCompression("none", 50000L, 50000L);
        assertEquals(152400L, metrics.getTotalRawBytes());
        assertEquals(75600L, metrics.getTotalCompressedBytes());
        double expectedRatio = 152400.0 / 75600.0;
        assertEquals(expectedRatio, metrics.getCompressionRatio(), 1e-6);
        assertEquals(expectedRatio, ratioGauge.value(), 1e-6);
    }

    @Test
    @DisplayName("Задержки gRPC чанков: учет таймера, среднего и максимального времени")
    void testChunkTransferLatencyMetrics() {
        assertEquals(0L, metrics.getChunkTransferCount());
        assertEquals(0.0, metrics.getChunkLatencyAvgMs(), 1e-6);
        assertEquals(0.0, metrics.getChunkLatencyMaxMs(), 1e-6);

        // Записываем 10 мс (10_000_000 ns) и 30 мс (30_000_000 ns)
        metrics.recordChunkTransferLatency(10_000_000L);
        metrics.recordChunkTransferLatency(30_000_000L);

        assertEquals(2L, metrics.getChunkTransferCount());
        assertEquals(20.0, metrics.getChunkLatencyAvgMs(), 1e-6);
        assertEquals(30.0, metrics.getChunkLatencyMaxMs(), 1e-6);

        // Проверяем Micrometer Timer
        Timer timer = metrics.getRegistry().find("replicator.grpc.chunk.transfer.latency").timer();
        assertNotNull(timer);
        assertEquals(2, timer.count());
        assertEquals(40.0, timer.totalTime(TimeUnit.MILLISECONDS), 1e-6);
    }

    @Test
    @DisplayName("Очередь staging: корректное отслеживание активных файлов, байтов и очистки сирот")
    void testStagingMetrics() {
        assertEquals(0, metrics.getActiveStagingFiles());
        assertEquals(0L, metrics.getActiveStagingBytes());
        assertEquals(0L, metrics.getTotalOrphansCleaned());

        // Начался прием 1-го файла
        metrics.recordStagingFileStart();
        metrics.recordStagingBytes(1048576L); // 1 MB
        assertEquals(1, metrics.getActiveStagingFiles());
        assertEquals(1048576L, metrics.getActiveStagingBytes());

        // Начался прием 2-го файла
        metrics.recordStagingFileStart();
        metrics.recordStagingBytes(2097152L); // 2 MB
        assertEquals(2, metrics.getActiveStagingFiles());
        assertEquals(3145728L, metrics.getActiveStagingBytes());

        // Проверяем Micrometer Gauges
        Gauge queueGauge = metrics.getRegistry().find("replicator.staging.queue.size").gauge();
        assertNotNull(queueGauge);
        assertEquals(2.0, queueGauge.value(), 1e-6);

        Gauge bytesGauge = metrics.getRegistry().find("replicator.staging.active.bytes").gauge();
        assertNotNull(bytesGauge);
        assertEquals(3145728.0, bytesGauge.value(), 1e-6);

        // 1-й файл успешно закоммичен
        metrics.recordStagingFileComplete(1048576L);
        assertEquals(1, metrics.getActiveStagingFiles());
        assertEquals(2097152L, metrics.getActiveStagingBytes());
        assertEquals(1.0, queueGauge.value(), 1e-6);
        assertEquals(2097152.0, bytesGauge.value(), 1e-6);

        // Сборщик мусора удалил осиротевшие файлы
        metrics.recordStagingOrphansCleaned(7);
        assertEquals(7L, metrics.getTotalOrphansCleaned());

        Counter orphansCounter = metrics.getRegistry().find("replicator.staging.orphans.cleaned").counter();
        assertNotNull(orphansCounter);
        assertEquals(7.0, orphansCounter.count(), 1e-6);

        // 2-й файл завершен
        metrics.recordStagingFileComplete(2097152L);
        assertEquals(0, metrics.getActiveStagingFiles());
        assertEquals(0L, metrics.getActiveStagingBytes());
    }
}
