package org.apache.hadoop.explorer.replicator.concurrency;

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.inotify.InotifyBatchProcessor;
import org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

/**
 * Пиннинг несущего потока на замке вокруг сетевого вызова.
 *
 * <p>Виртуальный поток, который блокируется внутри монитора, не может размонтироваться от
 * несущего потока: несущий занят на все время ожидания. Пакетный сброс задач inotify держит
 * замок во время gRPC-вызова в Оркестратор с таймаутом в секунды, поэтому замок здесь —
 * {@link java.util.concurrent.locks.ReentrantLock}, на котором виртуальный поток размонтируется
 * штатно.
 *
 * <p>Проверка опирается на событие JFR {@code jdk.VirtualThreadPinned} и начинается с
 * контрольного случая: если проба не видит пиннинг даже на настоящем мониторе, тест ничего не
 * проверяет и падает сам. С JDK 24 монитор больше не пиннит виртуальный поток (JEP 491), и тест
 * пропускается — платформа проекта и CI остается Java 21 LTS.
 */
@DisplayName("Loom: замок вокруг сетевого вызова не пиннит несущий поток")
class CarrierPinningTest {

    private static final String PINNED_EVENT = "jdk.VirtualThreadPinned";
    private static final long BLOCKING_CALL_MS = 120L;
    private static final Object CONTROL_MONITOR = new Object();

    @Test
    @DisplayName("Пакетный сброс inotify не пиннит несущий поток на gRPC-вызове")
    void inotifyFlushShouldNotPinCarrier() throws Exception {
        assumeTrue(Runtime.version().feature() < 24,
                "С JDK 24 монитор не пиннит виртуальный поток (JEP 491): проверять нечего");

        long controlPinned = countPinnedEvents(() -> {
            synchronized (CONTROL_MONITOR) {
                blockForWhile();
            }
        });
        assertTrue(controlPinned > 0,
                "Проба JFR не увидела пиннинг на настоящем мониторе — тест не проверял бы ничего");

        HadoopFsManager fsManager = mock(HadoopFsManager.class);
        OrchestratorGrpcClient slowOrchestrator = new OrchestratorGrpcClient() {
            @Override
            public boolean batchCreateTasks(BatchCreateTasksRequest request) {
                // Вызов в Оркестратор с ответом в сотни миллисекунд — обычное дело на WAN
                blockForWhile();
                return true;
            }

            @Override
            public boolean updateJobStreamingTxid(String jobId, long txid, long lag) {
                return true;
            }
        };

        // Длинный интервал автосброса: фоновый поток не должен опустошить очередь до проверки
        try (InotifyBatchProcessor processor = new InotifyBatchProcessor(
                fsManager, slowOrchestrator, 100, 0.8, 0.3, 10, 999_999L)) {

            JobDto job = streamingJob();
            int queued = processor.enqueueDelete(job, job.getSourcePath() + "/dt=2026-10-25/part-0.parquet", 101L, 0L);
            assertEquals(1, queued, "Задача удаления должна попасть в очередь");

            long pinned = countPinnedEvents(processor::flush);

            assertEquals(0, pinned,
                    "Сброс держит замок во время gRPC-вызова: на мониторе это заняло бы несущий поток "
                            + "на все время ожидания сети");
        }
    }

    private JobDto streamingJob() {
        JobDto job = new JobDto();
        job.setId("job-pinning-1");
        job.setSourcePath("/warehouse/analytics.db/sales_daily");
        job.setTargetPath("/backup/warehouse/analytics.db/sales_daily");
        job.setSyncMode("STREAMING_INOTIFY");
        job.setSyncDeletes(true);
        return job;
    }

    private static void blockForWhile() {
        try {
            Thread.sleep(BLOCKING_CALL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Количество событий пиннинга за время выполнения действия в виртуальном потоке.
     */
    private long countPinnedEvents(Runnable action) throws Exception {
        Path dump = Files.createTempFile("virtual-thread-pinned", ".jfr");
        try (Recording recording = new Recording()) {
            recording.enable(PINNED_EVENT).withThreshold(Duration.ofMillis(1));
            recording.start();

            Thread virtual = Thread.ofVirtual().name("pinning-probe").start(action);
            virtual.join();

            recording.stop();
            recording.dump(dump);
        }
        try (RecordingFile file = new RecordingFile(dump)) {
            long count = 0;
            while (file.hasMoreEvents()) {
                RecordedEvent event = file.readEvent();
                if (PINNED_EVENT.equals(event.getEventType().getName())) {
                    count++;
                }
            }
            return count;
        } finally {
            Files.deleteIfExists(dump);
        }
    }
}
