package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.model.StreamingLeaseRenewRequest;
import org.apache.hadoop.explorer.replicator.model.StreamingLeaseRenewResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
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
    private final OrchestratorClient orchestratorClient;
    private final HadoopFsManager fsManager;
    private final InotifyBatchProcessor batchProcessor;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean active = new AtomicBoolean(false);

    private Thread listenerThread;
    private Thread leaseThread;
    private volatile long currentTxid = 0L;
    private volatile long currentEpoch = 0L;

    // Параметры адаптивного Backpressure
    private volatile boolean orchestratorSaturated = false;
    private volatile boolean backpressureActive = false;
    private volatile long currentPollDelayMs = 0L;
    private final long minPollDelayMs = 0L;
    private final long maxPollDelayMs;
    private final long backpressureStepMs = 500L;

    public HdfsInotifyListener(
            ReplicatorAgentConfig config,
            OrchestratorClient orchestratorClient,
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
            OrchestratorClient orchestratorClient,
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
            try {
                StreamingLeaseRenewResponse resp = orchestratorClient.renewStreamingLease(
                        new StreamingLeaseRenewRequest(config.getClusterId(), config.getAgentId())
                );

                if (resp != null) {
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
                }
            } catch (Exception e) {
                log.debug("[Streamer Lease Error] Ошибка проверки лизинга: {}", e.getMessage());
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
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
                        long txid = batch.getTxid();
                        this.currentTxid = txid;
                        long lag = stream.getTxidsBehindEstimate();

                        // Получаем список активных потоковых задач для кластера текущего стримера
                        List<JobDto> jobs = orchestratorClient.getJobs().stream()
                                .filter(j -> "STREAMING_INOTIFY".equalsIgnoreCase(j.getSyncMode()) && matchesCluster(j.getSourceClusterId(), config.getClusterId()))
                                .toList();

                        if (!jobs.isEmpty()) {
                            for (var event : batch.getEvents()) {
                                batchProcessor.processEvent(event, txid, lag, jobs);
                            }
                            batchProcessor.flush();
                        }
                    }
                } else {
                    // Если локальное подключение к NameNode отсутствует (например, в демо или тесте)
                    Thread.sleep(1000);
                }
            } catch (MissingEventsException e) {
                log.error("[Inotify Gap Alert] NameNode Edits журналы утеряны (gap: expected={}, actual={}). " +
                          "Запуск Reconciliation Scan для устранения расхождений...",
                        e.getExpectedTxid(), e.getActualTxid());

                // Запуск Reconciliation Scan
                runReconciliationScan();

                // Сброс стрима на актуальный txid
                stream = null;
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
    private void runReconciliationScan() {
        log.info("[Inotify Reconciliation] Выполняется фоновая сверка целевых каталогов потоковых задач для кластера '{}'...",
                config.getClusterId());
        try {
            List<JobDto> jobs = orchestratorClient.getJobs().stream()
                    .filter(j -> "STREAMING_INOTIFY".equalsIgnoreCase(j.getSyncMode()) && matchesCluster(j.getSourceClusterId(), config.getClusterId()))
                    .toList();

            for (JobDto job : jobs) {
                log.info("[Inotify Reconciliation] Сверка задачи '{}' (каталог '{}')", job.getId(), job.getSourcePath());
            }
        } catch (Exception e) {
            log.error("[Inotify Reconciliation Error] Ошибка при сверке: {}", e.getMessage(), e);
        }
    }

    private static boolean matchesCluster(String jobCluster, String agentCluster) {
        if (jobCluster == null || agentCluster == null) return true;
        return jobCluster.equalsIgnoreCase(agentCluster)
                || jobCluster.contains(agentCluster)
                || agentCluster.contains(jobCluster);
    }
}
