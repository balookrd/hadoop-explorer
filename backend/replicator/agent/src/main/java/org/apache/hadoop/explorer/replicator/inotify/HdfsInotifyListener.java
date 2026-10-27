package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.model.StreamingLeaseRenewRequest;
import org.apache.hadoop.explorer.replicator.model.StreamingLeaseRenewResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Слушатель потоковых событий HDFS Inotify (Near-Zero RPO) с поддержкой адаптивного Backpressure.
 *
 * <p>Возможности:
 * <ul>
 *   <li>Active-Standby лизинг с эпохами (fencing token) для защиты от Split-Brain.</li>
 *   <li>Динамический Backpressure: адаптивное регулирование задержки poll() при насыщении очередей процессора или пула задач оркестратора.</li>
 *   <li>Пакетная обработка событий с дедупликацией и сбросом пачками через InotifyBatchProcessor.</li>
 *   <li>Перехват MissingEventsException с авто-запуском Reconciliation Scan.</li>
 * </ul>
 */
public class HdfsInotifyListener {

    private static final Logger log = LoggerFactory.getLogger(HdfsInotifyListener.class);

    private final ReplicatorAgentConfig config;
    private final OrchestratorGrpcClient orchestratorClient;
    private final HadoopFsManager fsManager;
    private final InotifyBatchProcessor batchProcessor;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean active = new AtomicBoolean(false);

    private Thread listenerThread;
    private Thread leaseThread;
    private volatile long currentTxid = 0L;

    /** Список потоковых задач не удалось обновить: решать по нему, что событий ждать не нужно, нельзя. */
    private volatile boolean streamingJobsUnknown = false;
    private volatile long currentEpoch = 0L;
    private volatile long lastSuccessfulLeaseRenewTimeMs = System.currentTimeMillis();
    private static final long LEASE_EXPIRATION_SAFETY_TIMEOUT_MS = 10_000L;

    // Параметры адаптивного Backpressure
    private volatile boolean orchestratorSaturated = false;
    private volatile boolean backpressureActive = false;
    private volatile long currentPollDelayMs = 0L;
    private final long minPollDelayMs = 0L;
    private final long maxPollDelayMs;
    private final long backpressureStepMs = 500L;

    // Кэширование активных потоковых задач для устранения избыточных HTTP-запросов к оркестратору
    private volatile List<JobDto> cachedStreamingJobs = java.util.Collections.emptyList();
    private volatile long lastJobsFetchTimeMs = 0L;
    private static final long JOBS_CACHE_TTL_MS = 5000L;

    /** Статусы, в которых потоковое задание не переносит события (пауза, остановка, завершение). */
    private static final Set<String> INACTIVE_STATUSES = Set.of("STOPPED", "PAUSED", "CANCELLED", "FAILED", "COMPLETED");
    /** Запас по времени модификации при сверке после паузы: часы NameNode и агента расходятся. */
    private static final long RESUME_RECONCILE_MARGIN_MS = 60_000L;

    /** С какого момента задание замечено остановленным: после возобновления сверяются файлы новее. */
    private final Map<String, Long> stoppedSinceMs = new ConcurrentHashMap<>();
    /**
     * Удаления, замеченные в журнале у остановленных заданий. Сверка после паузы смотрит только
     * на файлы источника и удаленное не видит, поэтому пути копятся здесь и переносятся после
     * возобновления. Живут в памяти стримера: смена активного стримера на паузе их теряет.
     */
    private final Map<String, Set<String>> deletesWhilePaused = new ConcurrentHashMap<>();
    /** Предел путей на задание: дальше удаления на паузе отбрасываются с предупреждением. */
    static final int MAX_DELETES_WHILE_PAUSED = 100_000;
    private volatile List<JobDto> pausedJobs = java.util.Collections.emptyList();

    /** Сверка каталогов идет в стороне от чтения журнала, чтобы не задерживать события. */
    private Executor reconcileExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "streamer-reconcile");
        t.setDaemon(true);
        return t;
    });

    /**
     * Обработка пачки событий журнала NameNode.
     *
     * @return {@code true}, если позицию журнала можно двигать вперед
     */
    boolean processBatch(EventBatch batch, long lag) {
        long txid = batch.getTxid();
        List<JobDto> jobs = getStreamingJobs();
        if (streamingJobsUnknown) {
            // Список потоковых задач неизвестен: оркестратор недоступен, а кэш пуст или устарел.
            // Двинуть позицию сейчас значит потерять события безвозвратно — после перезапуска
            // стример продолжит с новой позиции и к ним не вернется.
            log.warn("[Inotify] Позиция журнала (txid={}) не продвинута: список потоковых задач неизвестен", txid);
            return false;
        }

        List<JobDto> paused = pausedJobs;
        if (!jobs.isEmpty() || !paused.isEmpty()) {
            // Исключение на постановке в очередь (переполнение) не дает продвинуть позицию:
            // пачка будет перечитана после переоткрытия потока
            for (var event : batch.getEvents()) {
                if (!jobs.isEmpty()) {
                    batchProcessor.processEvent(event, txid, lag, jobs);
                }
                if (!paused.isEmpty()) {
                    rememberDeleteWhilePaused(event, paused);
                }
            }
            batchProcessor.flush();
        }

        this.currentTxid = txid;
        return true;
    }

    public List<JobDto> getStreamingJobs() {
        long now = System.currentTimeMillis();
        if (now - lastJobsFetchTimeMs > JOBS_CACHE_TTL_MS || cachedStreamingJobs.isEmpty()) {
            try {
                List<JobDto> streaming = orchestratorClient.getJobs("STREAMING", false).stream()
                        .filter(j -> "STREAMING_INOTIFY".equalsIgnoreCase(j.getSyncMode()) && matchesCluster(j.getSourceClusterId(), config.getClusterId()))
                        .toList();
                cachedStreamingJobs = activeAfterPauseCheck(streaming, now);
                lastJobsFetchTimeMs = now;
                streamingJobsUnknown = false;
            } catch (Exception e) {
                // Пустой список из-за недоступного оркестратора — это не «задач нет»:
                // по нему нельзя решать, что события никому не нужны
                streamingJobsUnknown = true;
                log.warn("[Inotify] Не удалось обновить список потоковых задач: {}", e.getMessage());
            }
        }
        return cachedStreamingJobs;
    }

    /**
     * Задания, события которых переносятся, и сверка возобновленных.
     *
     * <p>Список отбирался только по режиму синхронизации, и пауза HMS-задачи компаньон не
     * останавливала: файлы продолжали переноситься. Остановленное задание теперь пропускается,
     * а после возобновления его каталог сверяется по файлам, измененным с момента паузы, —
     * события, пришедшие на паузе, уже прочитаны и к стримеру не вернутся. Полная сверка
     * каталога базы на петабайты после каждой паузы заново гнала бы все данные.
     */
    private List<JobDto> activeAfterPauseCheck(List<JobDto> streaming, long now) {
        List<JobDto> active = new java.util.ArrayList<>(streaming.size());
        List<JobDto> paused = new java.util.ArrayList<>();
        for (JobDto job : streaming) {
            String status = job.getStatus() != null ? job.getStatus().toUpperCase() : "";
            if (INACTIVE_STATUSES.contains(status)) {
                stoppedSinceMs.putIfAbsent(job.getId(), now);
                paused.add(job);
                continue;
            }
            active.add(job);
            Long stoppedAt = stoppedSinceMs.remove(job.getId());
            if (stoppedAt != null) {
                long modifiedAfter = stoppedAt - JOBS_CACHE_TTL_MS - RESUME_RECONCILE_MARGIN_MS;
                Set<String> deleted = deletesWhilePaused.remove(job.getId());
                log.info("[Inotify] Задание '{}' возобновлено: сверка каталога '{}' по файлам, измененным после паузы, "
                        + "и {} удалениям с паузы", job.getId(), job.getSourcePath(), deleted != null ? deleted.size() : 0);
                reconcileExecutor.execute(() -> {
                    replayDeletesAfterPause(job, deleted);
                    reconcileJobDirectory(job, modifiedAfter);
                });
            }
        }
        // Удаленное задание не возобновится: его накопленные удаления больше не нужны
        Set<String> known = new java.util.HashSet<>();
        streaming.forEach(j -> known.add(j.getId()));
        deletesWhilePaused.keySet().retainAll(known);
        pausedJobs = paused;
        return active;
    }

    /** Запоминает удаление (или вынос наружу) пути остановленного задания. */
    void rememberDeleteWhilePaused(Event event, List<JobDto> paused) {
        String path;
        if (event instanceof Event.UnlinkEvent unlink) {
            path = unlink.getPath();
        } else if (event instanceof Event.RenameEvent rename) {
            String src = rename.getSrcPath();
            String dst = rename.getDstPath();
            path = src;
            boolean outward = paused.stream().anyMatch(j -> InotifyPathFilter.matchesJobPrefix(src, j.getSourcePath())
                    && !InotifyPathFilter.matchesJobPrefix(dst, j.getSourcePath()));
            if (!outward) {
                return;
            }
        } else {
            return;
        }
        for (JobDto job : paused) {
            if (!InotifyPathFilter.matchesJobPrefix(path, job.getSourcePath()) || InotifyPathFilter.isStagedPath(path)
                    || !InotifyBatchProcessor.deleteAllowed(job, path)) {
                continue;
            }
            Set<String> paths = deletesWhilePaused.computeIfAbsent(job.getId(), k -> ConcurrentHashMap.newKeySet());
            if (paths.size() < MAX_DELETES_WHILE_PAUSED) {
                paths.add(path);
            } else if (paths.size() == MAX_DELETES_WHILE_PAUSED) {
                paths.add(path);
                log.warn("[Inotify] Задание '{}': на паузе удалено больше {} путей, дальнейшие удаления на приемник не перенесутся",
                        job.getId(), MAX_DELETES_WHILE_PAUSED);
            }
        }
    }

    /**
     * Переносит удаления с паузы. Путь, снова созданный на источнике, не удаляется: его новое
     * содержимое приедет сверкой по времени модификации.
     */
    void replayDeletesAfterPause(JobDto job, Set<String> deleted) {
        if (deleted == null || deleted.isEmpty() || batchProcessor == null) {
            return;
        }
        int queued = 0;
        try {
            FileSystem fs = fsManager.getFileSystem();
            for (String path : deleted) {
                if (!fs.exists(new Path(path))) {
                    queued += batchProcessor.enqueueDelete(job, path, currentTxid, 0L);
                }
            }
            if (queued > 0) {
                batchProcessor.flush();
            }
            log.info("[Inotify] Задание '{}': перенесено {} удалений с паузы", job.getId(), queued);
        } catch (Exception e) {
            log.error("[Inotify] Задание '{}': ошибка переноса удалений с паузы: {}", job.getId(), e.getMessage(), e);
        }
    }

    void setReconcileExecutorForTesting(Executor executor) {
        this.reconcileExecutor = executor;
    }

    public HdfsInotifyListener(
            ReplicatorAgentConfig config,
            OrchestratorGrpcClient orchestratorClient,
            HadoopFsManager fsManager
    ) {
        this(config, orchestratorClient, fsManager, new InotifyBatchProcessor(
                fsManager,
                orchestratorClient,
                config.getInotifyQueueCapacity(),
                config.getInotifyHighWatermark(),
                config.getInotifyLowWatermark(),
                config.getInotifyMaxBatchSize(),
                config.getInotifyFlushIntervalMs()
        ));
    }

    public HdfsInotifyListener(
            ReplicatorAgentConfig config,
            OrchestratorGrpcClient orchestratorClient,
            HadoopFsManager fsManager,
            InotifyBatchProcessor batchProcessor
    ) {
        this.config = config;
        this.orchestratorClient = orchestratorClient;
        this.fsManager = fsManager;
        this.batchProcessor = batchProcessor;
        this.maxPollDelayMs = Math.max(500L, config.getInotifyBackpressureMaxDelayMs());
    }

    public boolean isActive() {
        return active.get();
    }

    public long getCurrentTxid() {
        return currentTxid;
    }

    public long getCurrentEpoch() {
        return currentEpoch;
    }

    public InotifyBatchProcessor getBatchProcessor() {
        return batchProcessor;
    }

    public boolean isBackpressureActive() {
        return backpressureActive;
    }

    public long getCurrentPollDelayMs() {
        return currentPollDelayMs;
    }

    public boolean isOrchestratorSaturated() {
        return orchestratorSaturated;
    }

    public void setOrchestratorSaturated(boolean saturated) {
        this.orchestratorSaturated = saturated;
        if (this.batchProcessor != null) {
            this.batchProcessor.setOrchestratorSaturated(saturated);
        }
    }

    /**
     * Запуск потоков Active-Standby координации и Inotify стриминга.
     */
    public synchronized void start() {
        if (running.get()) {
            return;
        }
        running.set(true);

        log.info("[Inotify Streamer] Запуск сервиса стримера: agentId='{}', clusterId='{}', maxBackpressureDelay={}ms",
                config.getAgentId(), config.getClusterId(), maxPollDelayMs);

        // 1. Поток поддержания распределенного лизинга (Heartbeat, Failover & Saturation)
        leaseThread = new Thread(this::runLeaseLoop, "streamer-lease-loop");
        leaseThread.setDaemon(true);
        leaseThread.start();

        // 2. Поток чтения событий Inotify
        listenerThread = new Thread(this::runInotifyLoop, "streamer-inotify-loop");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    /**
     * Остановка стримера.
     */
    public synchronized void stop() {
        running.set(false);
        active.set(false);
        if (leaseThread != null) {
            leaseThread.interrupt();
        }
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
        if (batchProcessor != null) {
            batchProcessor.close();
        }
        log.info("[Inotify Streamer] Сервис стримера остановлен");
    }

    /**
     * Фоновый цикл продления аренды и отслеживания роли Active / Standby и насыщения оркестратора.
     */
    private void runLeaseLoop() {
        while (running.get()) {
            renewLeaseOnce();

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Один цикл продления аренды стримера.
     */
    void renewLeaseOnce() {
        {
            try {
                StreamingLeaseRenewResponse resp = orchestratorClient.renewStreamingLease(
                        new StreamingLeaseRenewRequest(config.getClusterId(), config.getAgentId())
                );

                if (resp != null) {
                    this.lastSuccessfulLeaseRenewTimeMs = System.currentTimeMillis();
                    // Отслеживание насыщения оркестратора
                    this.orchestratorSaturated = resp.isQueueSaturated();
                    if (this.batchProcessor != null) {
                        this.batchProcessor.setOrchestratorSaturated(this.orchestratorSaturated);
                    }

                    if ("DISABLED".equalsIgnoreCase(resp.getStatus())) {
                        log.debug("[Streamer Lease] Стриминг выключен на оркестраторе");
                        active.set(false);
                    } else if ("ACTIVE".equalsIgnoreCase(resp.getStatus())) {
                        if (!active.get()) {
                            log.info("[Streamer Active] Агент назначен АКТИВНЫМ стримером (эпоха {}, lastCommittedTxid={})",
                                    resp.getEpoch(), resp.getLastCommittedTxid());
                            this.currentEpoch = resp.getEpoch();
                            if (this.currentTxid == 0L && resp.getLastCommittedTxid() > 0) {
                                this.currentTxid = resp.getLastCommittedTxid();
                            }
                            active.set(true);
                        }
                    } else {
                        // STANDBY
                        if (active.get()) {
                            log.warn("[Streamer Standby] Переход в режим горячего резерва (активен: {}, эпоха {})",
                                    resp.getActiveAgentId(), resp.getEpoch());
                            active.set(false);
                        }
                    }

                    if (resp.isRedundancyWarning()) {
                        log.warn("[Streamer Warning] Отсутствует резервный стример для кластера '{}' (всего {} активных)!",
                                config.getClusterId(), resp.getRegisteredStreamersCount());
                    }
                } else {
                    // Клиент гасит исключения продления и отдает null, поэтому отсутствие ответа —
                    // такой же сбой проверки аренды, как и исключение. Без этой ветки self-fencing
                    // недостижим, и при сетевой изоляции агент остается активным навсегда.
                    log.debug("[Streamer Lease Error] Оркестратор не подтвердил аренду (пустой ответ)");
                    handleLeaseCheckFailure();
                }
            } catch (Exception e) {
                log.debug("[Streamer Lease Error] Ошибка проверки лизинга: {}", e.getMessage());
                handleLeaseCheckFailure();
            }
        }
    }

    /**
     * Основной цикл чтения HDFS Inotify событий с адаптивным Backpressure сдерживанием.
     * Активен только при active == true.
     */
    private void runInotifyLoop() {
        DFSInotifyEventInputStream stream = null;
        HdfsAdmin admin = null;

        while (running.get()) {
            if (!active.get()) {
                // В режиме Standby не читаем Edits логи NameNode
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                continue;
            }

            // =================================================================
            // Регулятор Backpressure: оценка локального и удаленного насыщения
            // =================================================================
            boolean localSaturated = (batchProcessor != null && batchProcessor.isQueueSaturated());
            boolean isSaturated = localSaturated || orchestratorSaturated;

            if (isSaturated) {
                if (!backpressureActive) {
                    backpressureActive = true;
                    log.warn("[Inotify Backpressure] ВНИМАНИЕ: Активировано адаптивное сдерживание стримера (локальная очередь: {}/{}, оркестратор перегружен: {}). Замедление опроса poll()...",
                            batchProcessor != null ? batchProcessor.getQueueSize() : 0,
                            batchProcessor != null ? batchProcessor.getQueueCapacity() : 0,
                            orchestratorSaturated);
                }

                // Ступенчатое увеличение задержки poll()
                currentPollDelayMs = Math.min(maxPollDelayMs, Math.max(500L, currentPollDelayMs + backpressureStepMs));
                log.debug("[Inotify Backpressure] Задержка опроса poll(): {} мс (очередь процессора: {}/{})",
                        currentPollDelayMs,
                        batchProcessor != null ? batchProcessor.getQueueSize() : 0,
                        batchProcessor != null ? batchProcessor.getQueueCapacity() : 0);

                // Сбрасываем накопившиеся задачи в процессоре
                if (batchProcessor != null) {
                    batchProcessor.flush();
                }

                try {
                    Thread.sleep(currentPollDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } else {
                if (backpressureActive) {
                    currentPollDelayMs = Math.max(minPollDelayMs, currentPollDelayMs - backpressureStepMs);
                    if (currentPollDelayMs <= minPollDelayMs) {
                        backpressureActive = false;
                        log.info("[Inotify Backpressure] Очереди разгрузились (размер буфера: {}). Восстановлен штатный режим опроса poll().",
                                batchProcessor != null ? batchProcessor.getQueueSize() : 0);
                    }
                }
            }

            try {
                // Инициализация HdfsAdmin
                if (admin == null && config.getDefaultFsUri() != null) {
                    Configuration conf = fsManager.getConfiguration();
                    URI nnUri = URI.create(config.getDefaultFsUri());
                    admin = new HdfsAdmin(nnUri, conf);
                }

                if (stream == null && admin != null) {
                    long startTxid = (currentTxid > 0) ? (currentTxid + 1) : 0L;
                    log.info("[Inotify Init] Открытие DFSInotifyEventInputStream с txid={}", startTxid);
                    stream = (startTxid > 0) ? admin.getInotifyEventStream(startTxid) : admin.getInotifyEventStream();
                }

                if (stream != null) {
                    EventBatch batch = stream.poll(1000, TimeUnit.MILLISECONDS);
                    if (batch != null) {
                        long lag = stream.getTxidsBehindEstimate();
                        if (!processBatch(batch, lag)) {
                            // Позиция осталась прежней: переоткрываем поток, чтобы перечитать пачку
                            stream = null;
                            Thread.sleep(1000);
                        }
                    } else {
                        // Журнал молчит, а список заданий обновляется по-прежнему: иначе
                        // возобновленное с паузы задание замечалось бы лишь с первым новым
                        // событием, и сверка файлов, записанных на паузе, откладывалась бы
                        getStreamingJobs();
                    }
                } else {
                    // Если локальное подключение к NameNode отсутствует (например, в демо или тесте)
                    Thread.sleep(1000);
                }
            } catch (MissingEventsException e) {
                log.error("[Inotify Gap Alert] NameNode Edits журналы утеряны (gap: expected={}, actual={}). " +
                          "Запуск Reconciliation Scan для устранения расхождений...",
                        e.getExpectedTxid(), e.getActualTxid());

                // Обновляем текущий txid до актуального NameNode txid, предотвращая зацикливание
                this.currentTxid = Math.max(this.currentTxid, e.getActualTxid());

                // Запуск Reconciliation Scan
                runReconciliationScan();

                // Сброс стрима на актуальный txid
                stream = null;
            } catch (InotifyBatchProcessor.InotifyQueueOverflowException e) {
                log.warn("[Inotify Backpressure Peak] Зафиксировано пиковое переполнение очереди буфера: {}. Приостановка чтения EditLog на 2с...", e.getMessage());
                if (batchProcessor != null) {
                    batchProcessor.flush();
                }
                // Сбрасываем stream, чтобы переоткрыть с текущего currentTxid без пропуска событий
                stream = null;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.debug("[Inotify Poll Error] Ошибка чтения событий: {}", e.getMessage());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Сверка файловых деревьев при пропуске событий NameNode (MissingEventsException).
     */
    public int runReconciliationScan() {
        log.info("[Inotify Reconciliation] Выполняется фоновая сверка целевых каталогов потоковых задач для кластера '{}'...",
                config.getClusterId());
        int totalReconciled = 0;
        try {
            List<JobDto> jobs = getStreamingJobs();

            for (JobDto job : jobs) {
                log.info("[Inotify Reconciliation] Сверка задачи '{}' (каталог '{}')", job.getId(), job.getSourcePath());
                totalReconciled += reconcileJobDirectory(job);
            }
        } catch (Exception e) {
            log.error("[Inotify Reconciliation Error] Ошибка при сверке: {}", e.getMessage(), e);
        }
        return totalReconciled;
    }

    public int reconcileJobDirectory(JobDto job) {
        return reconcileJobDirectory(job, Long.MIN_VALUE);
    }

    /** Сверка каталога задания по файлам, измененным позже {@code modifiedAfterMs}. */
    int reconcileJobDirectory(JobDto job, long modifiedAfterMs) {
        if (job == null || job.getSourcePath() == null || job.getSourcePath().isBlank()) {
            return 0;
        }
        int count = 0;
        try {
            FileSystem fs = fsManager.getFileSystem();
            Path path = new Path(job.getSourcePath());
            if (!fs.exists(path)) {
                log.warn("[Inotify Reconciliation] Корневой каталог задачи '{}' не существует: {}", job.getId(), path);
                return 0;
            }

            RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, true);
            while (it.hasNext()) {
                LocatedFileStatus file = it.next();
                String filePath = file.getPath().toUri().getPath();
                if (file.getModificationTime() > modifiedAfterMs && !InotifyPathFilter.isStagedPath(filePath)) {
                    if (batchProcessor != null) {
                        count += batchProcessor.enqueueFile(job, filePath, file.getLen(), currentTxid, 0L);
                    }
                }
            }
            if (batchProcessor != null && count > 0) {
                batchProcessor.flush();
            }
            log.info("[Inotify Reconciliation] Сверка задачи '{}' завершена: поставлено в очередь {} файлов",
                    job.getId(), count);
        } catch (Exception e) {
            log.error("[Inotify Reconciliation] Ошибка обхода каталога '{}' для задачи '{}': {}",
                    job.getSourcePath(), job.getId(), e.getMessage(), e);
        }
        return count;
    }

    void handleLeaseCheckFailure() {
        if (active.get() && (System.currentTimeMillis() - lastSuccessfulLeaseRenewTimeMs > LEASE_EXPIRATION_SAFETY_TIMEOUT_MS)) {
            log.warn("[Streamer Self-Fencing] Потеря связи с оркестратором на {} мс (> {} мс). Аварийный переход в STANDBY",
                    System.currentTimeMillis() - lastSuccessfulLeaseRenewTimeMs, LEASE_EXPIRATION_SAFETY_TIMEOUT_MS);
            active.set(false);
        }
    }

    void setActiveForTesting(boolean activeState, long lastRenewTimeMs) {
        this.active.set(activeState);
        this.lastSuccessfulLeaseRenewTimeMs = lastRenewTimeMs;
    }

    private static boolean matchesCluster(String jobCluster, String agentCluster) {
        if (jobCluster == null || agentCluster == null) return false;
        if (jobCluster.equalsIgnoreCase(agentCluster)) return true;
        String c1n = jobCluster.toLowerCase().replace("-", "").replace("_", "");
        String c2n = agentCluster.toLowerCase().replace("-", "").replace("_", "");
        return c1n.equals(c2n);
    }
}
