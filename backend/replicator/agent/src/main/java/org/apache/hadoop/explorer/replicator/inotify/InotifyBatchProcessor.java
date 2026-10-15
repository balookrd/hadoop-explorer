package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.model.TaskCreateItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Процессор событий HDFS Inotify с поддержкой Backpressure и динамического пакетного буфера.
 *
 * <p>Возможности:
 * <ul>
 *   <li>Динамическое ограничение емкости внутренней очереди (Backpressure High/Low Watermark).</li>
 *   <li>Пакетирование пофайловых задач пачками (Micro-batching) до maxBatchSize для снижения сетевого оверхеда.</li>
 *   <li>Автоматическая дедупликация путей в рамках одного сброса.</li>
 *   <li>Адаптивная сигнализация перегрузки для приостановки вычитки NameNode EditLog.</li>
 *   <li>Распознавание коммитов Spark/Hive (пофайлово и партициями) с фильтрацией staging-путей.</li>
 * </ul>
 */
public class InotifyBatchProcessor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InotifyBatchProcessor.class);

    public record QueuedInotifyTask(
            String jobId,
            TaskCreateItem item,
            long txid,
            long lag
    ) {}

    private final HadoopFsManager fsManager;
    private final OrchestratorClient orchestratorClient;

    private final int maxQueueCapacity;
    private final double highWatermark;
    private final double lowWatermark;
    private final int maxBatchSize;
    private final long flushIntervalMs;

    private final BlockingQueue<QueuedInotifyTask> taskQueue;
    private final AtomicInteger currentQueueSize = new AtomicInteger(0);
    private volatile boolean queueSaturated = false;
    private volatile boolean orchestratorSaturated = false;

    private final ScheduledExecutorService flushExecutor;

    public InotifyBatchProcessor(HadoopFsManager fsManager, OrchestratorClient orchestratorClient) {
        // Конструктор по умолчанию (синхронный сброс flushIntervalMs = 0 для мгновенной обработки в юнит-тестах)
        this(fsManager, orchestratorClient, 10000, 0.8, 0.3, 500, 0L);
    }

    public InotifyBatchProcessor(
            HadoopFsManager fsManager,
            OrchestratorClient orchestratorClient,
            int maxQueueCapacity,
            double highWatermark,
            double lowWatermark,
            int maxBatchSize,
            long flushIntervalMs
    ) {
        this.fsManager = fsManager;
        this.orchestratorClient = orchestratorClient;
        this.maxQueueCapacity = maxQueueCapacity > 0 ? maxQueueCapacity : 10000;
        this.highWatermark = (highWatermark > 0.0 && highWatermark <= 1.0) ? highWatermark : 0.8;
        this.lowWatermark = (lowWatermark >= 0.0 && lowWatermark < this.highWatermark) ? lowWatermark : 0.3;
        this.maxBatchSize = maxBatchSize > 0 ? maxBatchSize : 500;
        this.flushIntervalMs = flushIntervalMs;

        this.taskQueue = new LinkedBlockingQueue<>(this.maxQueueCapacity);

        if (this.flushIntervalMs > 0) {
            this.flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "inotify-batch-flusher");
                t.setDaemon(true);
                return t;
            });
            this.flushExecutor.scheduleWithFixedDelay(this::flush, this.flushIntervalMs, this.flushIntervalMs, TimeUnit.MILLISECONDS);
            log.info("[Inotify Batch Processor] Инициализирован асинхронный сброс буфера (интервал: {} мс, емкость: {}, батч: {})",
                    this.flushIntervalMs, this.maxQueueCapacity, this.maxBatchSize);
        } else {
            this.flushExecutor = null;
        }
    }

    /**
     * Обработка одного HDFS Inotify события в контексте активных streaming-задач.
     * Возвращает количество поставленных в очередь/отправленных задач репликации.
     */
    public int processEvent(Event event, long txid, long lag, List<JobDto> activeStreamingJobs) {
        if (event == null || activeStreamingJobs == null || activeStreamingJobs.isEmpty()) {
            return 0;
        }

        int totalQueued = 0;

        for (JobDto job : activeStreamingJobs) {
            String srcPrefix = job.getSourcePath();

            if (event instanceof Event.CloseEvent closeEvent) {
                String path = closeEvent.getPath();
                if (InotifyPathFilter.matchesJobPrefix(path, srcPrefix) && !InotifyPathFilter.isStagedPath(path)) {
                    log.debug("[Inotify CloseEvent] Зафиксирован файл {} (размер {} байт, txid {})", path, closeEvent.getFileSize(), txid);
                    int count = enqueueFile(job, path, closeEvent.getFileSize(), txid, lag);
                    totalQueued += count;
                }
            } else if (event instanceof Event.RenameEvent renameEvent) {
                String src = renameEvent.getSrcPath();
                String dst = renameEvent.getDstPath();

                if (InotifyPathFilter.matchesJobPrefix(dst, srcPrefix)) {
                    if (InotifyPathFilter.isStagingCommit(src, dst)) {
                        log.info("[Inotify Commit] Обнаружена фиксация данных из staging: '{}' -> '{}' (txid {})", src, dst, txid);
                        int count = handleStagingCommit(job, dst, txid, lag);
                        totalQueued += count;
                    } else if (!InotifyPathFilter.isStagedPath(src) && !InotifyPathFilter.isStagedPath(dst)) {
                        log.info("[Inotify Rename] Переименование пути в постоянном каталоге: '{}' -> '{}' (txid {})", src, dst, txid);
                        int count = handleStagingCommit(job, dst, txid, lag);
                        totalQueued += count;
                    }
                }
            } else if (event instanceof Event.UnlinkEvent unlinkEvent) {
                String path = unlinkEvent.getPath();
                if (InotifyPathFilter.matchesJobPrefix(path, srcPrefix) && !InotifyPathFilter.isStagedPath(path)) {
                    log.info("[Inotify Unlink] Удаление пути: '{}' (txid {})", path, txid);
                    if (job.isSyncDeletes()) {
                        enqueueDelete(job, path);
                    }
                    if (flushIntervalMs <= 0) {
                        orchestratorClient.updateJobStreamingTxid(job.getId(), txid, lag);
                    }
                }
            }
        }

        // Если сброс выполняется синхронно (режим без фонового таймера или для тестов)
        if (flushIntervalMs <= 0 && totalQueued > 0) {
            flush();
        }

        return totalQueued;
    }

    /**
     * Помещение задачи в очередь буфера с контролем емкости и Backpressure.
     */
    private boolean offerTask(String jobId, TaskCreateItem item, long txid, long lag) {
        if (currentQueueSize.get() >= maxQueueCapacity) {
            log.warn("[Inotify Queue Overflow] Внутренняя очередь переполнена ({}/{}). Задача для '{}' отклонена.",
                    currentQueueSize.get(), maxQueueCapacity, item.getSourcePath());
            updateSaturationState();
            return false;
        }

        boolean offered = taskQueue.offer(new QueuedInotifyTask(jobId, item, txid, lag));
        if (offered) {
            currentQueueSize.incrementAndGet();
            updateSaturationState();
        }
        return offered;
    }

    /**
     * Обновление состояния насыщения внутренней очереди (гистерезис).
     */
    private void updateSaturationState() {
        int size = currentQueueSize.get();
        double saturationRatio = (double) size / maxQueueCapacity;

        if (!queueSaturated && saturationRatio >= highWatermark) {
            queueSaturated = true;
            log.warn("[Inotify Backpressure] Очередь достигла верхнего порога насыщения: {}/{} ({:.1f}% >= {:.1f}%). Активирован локальный Backpressure.",
                    size, maxQueueCapacity, saturationRatio * 100.0, highWatermark * 100.0);
        } else if (queueSaturated && saturationRatio <= lowWatermark) {
            queueSaturated = false;
            log.info("[Inotify Backpressure] Очередь опустилась ниже порога восстановления: {}/{} ({:.1f}% <= {:.1f}%). Локальный Backpressure деактивирован.",
                    size, maxQueueCapacity, saturationRatio * 100.0, lowWatermark * 100.0);
        }
    }

    /**
     * Обработка фиксации пути из staging (директория партиции или файл).
     */
    private int handleStagingCommit(JobDto job, String dstPath, long txid, long lag) {
        try {
            FileSystem fs = fsManager.getFileSystem();
            Path path = new Path(dstPath);

            if (!fs.exists(path)) {
                log.warn("[Inotify Commit] Путь '{}' не найден в HDFS (возможно, удален)", dstPath);
                return 0;
            }

            FileStatus status = fs.getFileStatus(path);
            if (status.isDirectory()) {
                log.info("[Inotify Commit] Путь '{}' является директорией партиции. Сканирование файлов...", dstPath);
                int count = 0;
                RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, true);
                while (it.hasNext()) {
                    LocatedFileStatus file = it.next();
                    String filePath = file.getPath().toUri().getPath();
                    if (!InotifyPathFilter.isStagedPath(filePath)) {
                        String targetFilePath = computeTargetPath(job, filePath);
                        TaskCreateItem item = new TaskCreateItem(filePath, targetFilePath, file.getLen(), false);
                        if (offerTask(job.getId(), item, txid, lag)) {
                            count++;
                        }
                    }
                }
                log.info("[Inotify Commit] Поставлено в очередь пакетов {} файлов партиции '{}'", count, dstPath);
                return count;
            } else {
                return enqueueFile(job, dstPath, status.getLen(), txid, lag);
            }
        } catch (IOException e) {
            log.error("[Inotify Commit Error] Ошибка проверки пути '{}': {}", dstPath, e.getMessage(), e);
        }
        return 0;
    }

    private int enqueueFile(JobDto job, String filePath, long fileSize, long txid, long lag) {
        String targetPath = computeTargetPath(job, filePath);
        TaskCreateItem item = new TaskCreateItem(filePath, targetPath, fileSize, false);
        return offerTask(job.getId(), item, txid, lag) ? 1 : 0;
    }

    private void enqueueDelete(JobDto job, String deletedPath) {
        log.debug("[Inotify SyncDelete] Постановка задачи удаления: {}", deletedPath);
    }

    /**
     * Пакетный сброс накопившихся задач из очереди в Оркестратор.
     * Возвращает общее количество успешно отправленных задач.
     */
    public synchronized int flush() {
        if (taskQueue.isEmpty()) {
            return 0;
        }

        List<QueuedInotifyTask> drained = new ArrayList<>();
        taskQueue.drainTo(drained);
        if (drained.isEmpty()) {
            return 0;
        }

        currentQueueSize.addAndGet(-drained.size());
        updateSaturationState();

        // Группировка по jobId для пакетной отправки
        Map<String, List<QueuedInotifyTask>> grouped = drained.stream()
                .collect(Collectors.groupingBy(QueuedInotifyTask::jobId));

        int totalFlushed = 0;
        for (Map.Entry<String, List<QueuedInotifyTask>> entry : grouped.entrySet()) {
            String jobId = entry.getKey();
            List<QueuedInotifyTask> tasks = entry.getValue();

            // Дедупликация задач по sourcePath: берем самое последнее состояние
            Map<String, QueuedInotifyTask> dedupMap = new LinkedHashMap<>();
            long maxTxid = 0L;
            long lastLag = 0L;
            for (QueuedInotifyTask t : tasks) {
                dedupMap.put(t.item().getSourcePath(), t);
                if (t.txid() > maxTxid) {
                    maxTxid = t.txid();
                    lastLag = t.lag();
                }
            }

            List<TaskCreateItem> items = dedupMap.values().stream()
                    .map(QueuedInotifyTask::item)
                    .toList();

            // Отправка порциями до maxBatchSize
            for (int i = 0; i < items.size(); i += maxBatchSize) {
                int end = Math.min(i + maxBatchSize, items.size());
                List<TaskCreateItem> chunk = items.subList(i, end);
                try {
                    boolean ok = orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(jobId, chunk));
                    if (ok) {
                        totalFlushed += chunk.size();
                    } else {
                        log.warn("[Inotify Flush] Оркестратор вернул ошибку при создании пачки из {} задач для job '{}'", chunk.size(), jobId);
                    }
                } catch (Exception e) {
                    log.error("[Inotify Flush Error] Сбой отправки пакета задач в оркестратор: {}", e.getMessage(), e);
                }
            }

            if (maxTxid > 0) {
                try {
                    orchestratorClient.updateJobStreamingTxid(jobId, maxTxid, lastLag);
                } catch (Exception e) {
                    log.debug("[Inotify Flush] Ошибка обновления txid для задачи {}: {}", jobId, e.getMessage());
                }
            }
        }

        log.debug("[Inotify Flush] Успешно сброшено {} задач в Оркестратор (остаток в очереди: {})",
                totalFlushed, currentQueueSize.get());
        return totalFlushed;
    }

    public boolean isQueueSaturated() {
        return queueSaturated;
    }

    public boolean isQueueFull() {
        return currentQueueSize.get() >= maxQueueCapacity;
    }

    public int getQueueSize() {
        return currentQueueSize.get();
    }

    public int getQueueCapacity() {
        return maxQueueCapacity;
    }

    public double getSaturationPercentage() {
        return maxQueueCapacity > 0 ? (currentQueueSize.get() * 100.0 / maxQueueCapacity) : 0.0;
    }

    public boolean isOrchestratorSaturated() {
        return orchestratorSaturated;
    }

    public void setOrchestratorSaturated(boolean orchestratorSaturated) {
        this.orchestratorSaturated = orchestratorSaturated;
    }

    public static String computeTargetPath(JobDto job, String sourceFilePath) {
        String srcRoot = normalize(job.getSourcePath());
        String dstRoot = normalize(job.getTargetPath());
        String normalizedFile = normalize(sourceFilePath);

        if (normalizedFile.startsWith(srcRoot)) {
            String rel = normalizedFile.substring(srcRoot.length());
            if (rel.startsWith("/")) {
                rel = rel.substring(1);
            }
            return dstRoot + (dstRoot.endsWith("/") ? "" : "/") + rel;
        }

        return dstRoot + "/" + new Path(sourceFilePath).getName();
    }

    private static String normalize(String p) {
        if (p == null) return "";
        String s = p.trim();
        if (!s.startsWith("/")) s = "/" + s;
        if (s.endsWith("/") && s.length() > 1) s = s.substring(0, s.length() - 1);
        return s;
    }

    @Override
    public void close() {
        if (flushExecutor != null) {
            flushExecutor.shutdown();
            try {
                if (!flushExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        flush();
        log.info("[Inotify Batch Processor] Процессор остановлен, буфер сброшен.");
    }
}
