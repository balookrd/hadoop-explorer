package org.apache.hadoop.explorer.replicator.agent;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.metrics.AgentMetrics;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.sender.ReplicationSender;
import org.apache.hadoop.explorer.replicator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Универсальный агент репликации Hadoop gRPC на Java (ReplicatorAgent).
 *
 * <p>Поддерживает режимы:
 * <ul>
 *   <li><b>all</b> (по умолчанию) — полный дуплекс: прием входящих файлов по gRPC (:50051)
 *       и отправка исходящих файлов из очереди Оркестратора;</li>
 *   <li><b>sender</b> — только отправка исходящих задач репликации;</li>
 *   <li><b>receiver</b> — только прием входящих файлов и сохранение в HDFS/FS.</li>
 * </ul>
 */
public class ReplicatorAgent {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorAgent.class);

    private final ReplicatorAgentConfig config;
    private final LocalBandwidthLimiter bandwidthLimiter;
    private final HadoopFsManager fsManager;
    private final OrchestratorGrpcClient orchestratorClient;
    private final ReplicationSender sender;
    private HmsClient hmsClient;
    private org.apache.hadoop.explorer.replicator.hms.service.HmsTaskExecutor hmsTaskExecutor;

    private Server grpcServer;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledExecutorService stagingCleanupExecutor;
    private ExecutorService workerTaskPool;
    private final ExecutorService hmsTaskPool;
    private final Set<String> activeRunningHmsJobs = ConcurrentHashMap.newKeySet();
    private Thread senderThread;
    private Thread hmsThread;
    private DataTransferServiceImpl receiverService;
    private org.apache.hadoop.explorer.replicator.inotify.HdfsInotifyListener inotifyListener;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeTransfers = new AtomicInteger(0);

    // Кэш gRPC адресов целевых кластеров
    private final Map<String, String> clusterAddressCache = new ConcurrentHashMap<>();
    private volatile long lastTopologySyncTime = 0;

    public ReplicatorAgent(ReplicatorAgentConfig config) {
        this.config = config;
        this.bandwidthLimiter = new LocalBandwidthLimiter(config.getMaxBandwidthMbS() != null ? config.getMaxBandwidthMbS() : 0.0);
        this.fsManager = new HadoopFsManager(config.getDefaultFsUri(), config.getKeytabPath(), config.getPrincipal(), config.getLocalSandboxDir());
        this.hmsTaskPool = Executors.newVirtualThreadPerTaskExecutor();
        this.workerTaskPool = Executors.newFixedThreadPool(
                Math.max(2, config.getBundleCommitConcurrency()),
                r -> {
                    Thread t = new Thread(r, "transfer-worker-" + config.getAgentId());
                    t.setDaemon(true);
                    return t;
                }
        );
        this.orchestratorClient = new OrchestratorGrpcClient(
                config.getOrchestratorGrpcAddress(),
                config.getAgentSecret(),
                config.isGrpcTlsEnabled(),
                config.getGrpcTrustCertCollectionPath(),
                config.isOrchestratorInsecureSkipVerify()
        );
        this.sender = new ReplicationSender(
                config.getAgentId(),
                orchestratorClient,
                fsManager,
                bandwidthLimiter,
                config.getChunkSize(),
                config.isGrpcTlsEnabled(),
                config.getGrpcTrustCertCollectionPath(),
                config.getGrpcCertChainPath(),
                config.getGrpcPrivateKeyPath(),
                config.isGrpcInsecureSkipVerify(),
                config.getSmallFileThresholdBytes(),
                config.getBundleTargetSizeBytes(),
                config.getMaxBundleFiles(),
                config.getWireCompressionCodec(),
                config.getWireCompressionLevel(),
                config.getAgentSecret(),
                config.getTokenBatchMinBytes(),
                config.getTokenBatchMaxBytes()
        );

        if ("all".equalsIgnoreCase(config.getMode()) || "hms".equalsIgnoreCase(config.getMode()) || "receiver".equalsIgnoreCase(config.getMode())) {
            if ("demo-http".equalsIgnoreCase(config.getHmsMode())) {
                logger.info("Агент '{}' инициализирован с Demo HttpRemoteHmsClient (тестовый симулятор стенда)", config.getAgentId());
                this.hmsClient = new org.apache.hadoop.explorer.replicator.hms.client.HttpRemoteHmsClient(
                        config.getOrchestratorUrl(), config.getClusterId(), config.getAgentSecret(), config.isOrchestratorInsecureSkipVerify()
                );
            } else if ("mock".equalsIgnoreCase(config.getHmsMode())) {
                this.hmsClient = new MockHmsClient(config.getClusterId(), config.getHiveVersion());
            } else if ((config.getHmsThriftUris() != null && !config.getHmsThriftUris().isBlank()) || "thrift".equalsIgnoreCase(config.getHmsMode())) {
                logger.info("Боевой режим: прямое подключение агента к Hive Metastore по Thrift: {}", config.getHmsThriftUris());
                this.hmsClient = new org.apache.hadoop.explorer.replicator.hms.client.ThriftHmsClient(
                        config.getClusterId(),
                        config.getHmsThriftUris(),
                        config.getHmsKerberosPrincipal(),
                        config.getKeytabPath()
                );
            } else if ("auto".equalsIgnoreCase(config.getHmsMode()) && config.getOrchestratorUrl() != null && !config.getOrchestratorUrl().isBlank()) {
                this.hmsClient = new org.apache.hadoop.explorer.replicator.hms.client.HttpRemoteHmsClient(
                        config.getOrchestratorUrl(), config.getClusterId(), config.getAgentSecret(), config.isOrchestratorInsecureSkipVerify()
                );
            } else {
                this.hmsClient = new MockHmsClient(config.getClusterId(), config.getHiveVersion());
            }
        }

        if ("all".equalsIgnoreCase(config.getMode()) || "hms".equalsIgnoreCase(config.getMode()) || "sender".equalsIgnoreCase(config.getMode())) {
            var mappings = org.apache.hadoop.explorer.replicator.hms.rewriter.HmsPathRewriter.parseMappings(config.getFederationMappings());
            var pathRewriter = new org.apache.hadoop.explorer.replicator.hms.rewriter.HmsPathRewriter(mappings, config.getTargetDefaultFsUri());
            this.hmsTaskExecutor = new org.apache.hadoop.explorer.replicator.hms.service.HmsTaskExecutor(
                    config.getAgentId(),
                    config.getClusterId(),
                    hmsClient,
                    orchestratorClient,
                    pathRewriter,
                    config.isGrpcTlsEnabled(),
                    config.isGrpcInsecureSkipVerify(),
                    8,
                    1000,
                    config.getAgentSecret()
            );
        }
    }

    public HmsClient getHmsClient() { return hmsClient; }
    public void setHmsClient(HmsClient hmsClient) { this.hmsClient = hmsClient; }
    public org.apache.hadoop.explorer.replicator.hms.service.HmsTaskExecutor getHmsTaskExecutor() { return hmsTaskExecutor; }
    public void setHmsTaskExecutor(org.apache.hadoop.explorer.replicator.hms.service.HmsTaskExecutor executor) { this.hmsTaskExecutor = executor; }


    /**
     * Запуск всех компонентов агента.
     */
    public synchronized void start() throws IOException {
        if (running.get()) {
            logger.warn("Агент уже запущен");
            return;
        }
        running.set(true);

        logger.info("======================================================================");
        logger.info("Старт Replicator Agent (Java 17): id='{}', cluster='{}', mode='{}'",
                config.getAgentId(), config.getClusterId(), config.getMode());
        logger.info("Оркестратор: {}, Advertised gRPC: {}",
                config.getOrchestratorUrl(), config.getAdvertisedGrpcAddress());
        if (bandwidthLimiter.isEnabled()) {
            logger.info("Локальный лимит полосы пропускания: {} МБ/с", bandwidthLimiter.getLimitMbPerSec());
        }
        logger.info("======================================================================");

        // 1. Регистрация и Heartbeat
        if (config.isEnableDynamicRegistration()) {
            startHeartbeat();
        }

        // 2. gRPC Receiver Server
        if ("all".equalsIgnoreCase(config.getMode()) || "receiver".equalsIgnoreCase(config.getMode())) {
            startReceiverServer();
        }

        // 3. Sender / Worker / Analyzer Loop
        if ("all".equalsIgnoreCase(config.getMode()) || "sender".equalsIgnoreCase(config.getMode())
                || "worker".equalsIgnoreCase(config.getMode()) || "analyzer".equalsIgnoreCase(config.getMode())) {
            startSenderLoop();
        }

        // 4. HMS Metadata Replication Loop (Bootstrap & CDC)
        if (hmsTaskExecutor != null && ("all".equalsIgnoreCase(config.getMode()) || "hms".equalsIgnoreCase(config.getMode()) || "sender".equalsIgnoreCase(config.getMode()))) {
            startHmsLoop();
        }

        // 5. Фоновый сборщик мусора HDFS staging-файлов
        if (config.isStagingCleanupEnabled() && !"streamer".equalsIgnoreCase(config.getMode())) {
            startStagingCleaner();
        }

        // 5. HDFS Inotify Streamer Loop (Near-Zero RPO)
        // Запускается только для специализированных Inotify стримеров (mode="streamer")
        if ("streamer".equalsIgnoreCase(config.getMode())) {
            this.inotifyListener = new org.apache.hadoop.explorer.replicator.inotify.HdfsInotifyListener(
                    config, orchestratorClient, fsManager
            );
            this.inotifyListener.start();
        }
    }

    private void startReceiverServer() throws IOException {
        this.receiverService = new DataTransferServiceImpl(
                config.getStagingDir(),
                fsManager,
                bandwidthLimiter,
                config.getKeytabPath(),
                config.getBundleCommitConcurrency(),
                config.getMaxConcurrentTarStreams(),
                config.getTarPipeBufferBytes(),
                60_000L,
                config.getAgentSecret(),
                config.isEnforceTaskAttestation()
        );

        NettyServerBuilder serverBuilder = NettyServerBuilder.forAddress(new InetSocketAddress(config.getReceiverHost(), config.getReceiverPort()))
                .addService(receiverService)
                .maxInboundMessageSize(64 * 1024 * 1024);

        if (hmsClient != null) {
            serverBuilder.addService(new org.apache.hadoop.explorer.replicator.hms.service.HmsTransferServiceImpl(hmsClient));
            logger.info("gRPC сервис HmsTransferService успешно зарегистрирован на Receiver сервере");
        }

        if (config.getAgentSecret() != null && !config.getAgentSecret().isBlank()) {
            serverBuilder.intercept(new org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor(config.getAgentSecret()));
            logger.info("Включена gRPC L7 аутентификация по X-Agent-Secret на Receiver сервере");
        } else {
            serverBuilder.intercept(new org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor(null, false));
            logger.warn("ВНИМАНИЕ: X-Agent-Secret не задан! gRPC Receiver сервер запущен в режиме Fail-Closed (все запросы будут отклоняться)");
        }

        if (config.isGrpcTlsEnabled()) {
            SslContextBuilder sslBuilder;
            if (config.getGrpcCertChainPath() != null && config.getGrpcPrivateKeyPath() != null) {
                sslBuilder = SslContextBuilder.forServer(
                        new File(config.getGrpcCertChainPath()),
                        new File(config.getGrpcPrivateKeyPath())
                );
            } else {
                logger.info("gRPC TLS включен без сертификатов: генерация SelfSignedCertificate для {}:{}",
                        config.getReceiverHost(), config.getReceiverPort());
                String certHost = ("0.0.0.0".equals(config.getReceiverHost()) || "*".equals(config.getReceiverHost()))
                        ? "localhost" : config.getReceiverHost();
                try {
                    SelfSignedCertificate ssc = new SelfSignedCertificate(certHost);
                    sslBuilder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
                } catch (Exception e) {
                    throw new IOException("Не удалось сгенерировать временный сертификат для gRPC TLS: " + e.getMessage(), e);
                }
            }

            if (config.getGrpcTrustCertCollectionPath() != null && !config.getGrpcTrustCertCollectionPath().isBlank()) {
                sslBuilder.trustManager(new File(config.getGrpcTrustCertCollectionPath()));
            }

            String clientAuth = config.getGrpcClientAuth();
            if ("REQUIRE".equalsIgnoreCase(clientAuth) || "REQUIRED".equalsIgnoreCase(clientAuth) || "NEED".equalsIgnoreCase(clientAuth)) {
                sslBuilder.clientAuth(ClientAuth.REQUIRE);
            } else if ("OPTIONAL".equalsIgnoreCase(clientAuth) || "WANT".equalsIgnoreCase(clientAuth)) {
                sslBuilder.clientAuth(ClientAuth.OPTIONAL);
            } else {
                sslBuilder.clientAuth(ClientAuth.NONE);
            }

            serverBuilder.sslContext(GrpcSslContexts.configure(sslBuilder).build());
            logger.info("gRPC TLS активирован на Receiver сервере (mTLS auth: {})", clientAuth != null ? clientAuth : "NONE");
        }

        this.grpcServer = serverBuilder.build().start();

        logger.info("gRPC Receiver сервер успешно запущен и слушает {}:{} (TLS={})",
                config.getReceiverHost(), config.getReceiverPort(), config.isGrpcTlsEnabled());
    }

    private void startHeartbeat() {
        // Первичная регистрация
        AgentRegisterRequest regReq = new AgentRegisterRequest(
                config.getAgentId(),
                config.getClusterId(),
                config.getMode(),
                ("all".equals(config.getMode()) || "receiver".equals(config.getMode())) ? config.getAdvertisedGrpcAddress() : null,
                null,
                config.getMaxBandwidthMbS()
        );
        orchestratorClient.register(regReq);

        // Периодический keepalive
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "replicator-heartbeat");
            t.setDaemon(true);
            return t;
        });

        long intervalMillis = (long) (Math.max(1.0, config.getPollIntervalSec()) * 1000);
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (!running.get()) return;
            try {
                AgentHeartbeatRequest hbReq = new AgentHeartbeatRequest(
                        config.getAgentId(),
                        config.getClusterId(),
                        config.getAdvertisedGrpcAddress(),
                        activeTransfers.get(),
                        config.getMode()
                );
                hbReq.setTransferredBytes(sender != null ? sender.getTotalBytesTransferred() : 0L);
                hbReq.setTransferErrors(sender != null ? sender.getTotalTransferErrors() : 0L);
                hbReq.setBandwidthLimitMbS(config.getMaxBandwidthMbS());

                AgentMetrics metrics = AgentMetrics.getInstance();
                hbReq.setCompressionRawBytes(metrics.getTotalRawBytes());
                hbReq.setCompressionCompressedBytes(metrics.getTotalCompressedBytes());
                hbReq.setCompressionRatio(metrics.getCompressionRatio());
                hbReq.setChunkLatencyAvgMs(metrics.getChunkLatencyAvgMs());
                hbReq.setChunkLatencyMaxMs(metrics.getChunkLatencyMaxMs());
                hbReq.setChunkTransferCount(metrics.getChunkTransferCount());
                hbReq.setStagingActiveFiles(metrics.getActiveStagingFiles());
                hbReq.setStagingActiveBytes(metrics.getActiveStagingBytes());
                hbReq.setStagingOrphansCleaned(metrics.getTotalOrphansCleaned());

                orchestratorClient.heartbeat(hbReq);
            } catch (Exception e) {
                logger.debug("Ошибка отправки heartbeat: {}", e.getMessage());
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void startSenderLoop() {
        senderThread = new Thread(this::runSenderLoop, "replicator-sender-loop");
        senderThread.setUncaughtExceptionHandler((t, e) -> {
            logger.error("КРИТИЧЕСКИЙ СБОЙ потока {}: {}", t.getName(), e.getMessage(), e);
        });
        senderThread.setDaemon(true);
        senderThread.start();
    }

    private void startHmsLoop() {
        hmsThread = new Thread(this::runHmsLoop, "replicator-hms-loop");
        hmsThread.setUncaughtExceptionHandler((t, e) -> {
            logger.error("КРИТИЧЕСКИЙ СБОЙ потока {}: {}", t.getName(), e.getMessage(), e);
        });
        hmsThread.setDaemon(true);
        hmsThread.start();
    }

    private void runHmsLoop() {
        logger.info("Выделенный цикл репликации Hive Metastore (Bootstrap & CDC) запущен: cluster='{}'", config.getClusterId());
        while (running.get()) {
            boolean didWork = false;
            try {
                if (hmsTaskExecutor != null) {
                    List<org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto> hmsJobs =
                            orchestratorClient.getPendingHmsJobs(config.getClusterId(), config.getAgentId());
                    for (var hJob : hmsJobs) {
                        if (!running.get()) break;
                        String jobId = hJob.id();
                        if (activeRunningHmsJobs.add(jobId)) {
                            didWork = true;
                            hmsTaskPool.submit(() -> {
                                try {
                                    if ("QUEUED".equalsIgnoreCase(hJob.status()) || "BOOTSTRAPPING".equalsIgnoreCase(hJob.status())) {
                                        hmsTaskExecutor.runBootstrap(hJob);
                                    } else if ("ACTIVE".equalsIgnoreCase(hJob.status())) {
                                        hmsTaskExecutor.pollAndSyncCdc(hJob);
                                    }
                                } catch (Throwable t) {
                                    logger.error("[HMS Worker] Ошибка обработки задачи {}: {}", jobId, t.getMessage(), t);
                                } finally {
                                    activeRunningHmsJobs.remove(jobId);
                                }
                            });
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("Ошибка в цикле HMS репликации: {}", t.getMessage(), t);
            }

            if (!didWork) {
                try {
                    Thread.sleep(1500L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } else {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void runSenderLoop() {
        logger.info("Цикл Sender/Worker запущен: mode='{}' (опрос очереди каждые {}с)",
                config.getMode(), config.getPollIntervalSec());

        boolean canAnalyze = "all".equalsIgnoreCase(config.getMode()) || "analyzer".equalsIgnoreCase(config.getMode());
        boolean canWork = "all".equalsIgnoreCase(config.getMode()) || "sender".equalsIgnoreCase(config.getMode()) || "worker".equalsIgnoreCase(config.getMode());

        while (running.get()) {
            boolean didWork = false;
            try {
                // 1. Фаза анализатора: поиск заданий в QUEUED и создание пула подзадач
                if (canAnalyze) {
                    List<JobDto> jobs = orchestratorClient.getJobs();
                    for (JobDto job : jobs) {
                        if (!running.get()) break;

                        if (!"QUEUED".equalsIgnoreCase(job.getStatus())) {
                            continue;
                        }

                        if (config.getClusterId() != null && job.getSourceClusterId() != null
                                && !matchesCluster(config.getClusterId(), job.getSourceClusterId())) {
                            continue;
                        }

                        boolean claimed = orchestratorClient.updateJobProgress(job.getId(), new UpdateJobRequest("ANALYZING", 0L, 0L,
                                "Анализ каталога и вычисление diff деревьев"));
                        if (!claimed) {
                            logger.info("Агент '{}' пропустил задачу '{}': задача уже взята на анализ другим агентом (CAS)",
                                    config.getAgentId(), job.getId());
                            continue;
                        }

                        logger.info("Агент '{}' успешно захватил задачу '{}' на анализ каталога и формирование пула задач",
                                config.getAgentId(), job.getId());

                        String targetAddress = resolveTargetAddress(job.getTargetClusterId());
                        boolean ok = sender.analyzeAndCreateTaskPool(job, targetAddress);
                        if (ok) {
                            didWork = true;
                        }
                    }
                }

                // 2. Фаза воркера: забор и параллельное исполнение задач из распределенного пула
                if (canWork) {
                    int maxConcurrency = Math.max(2, config.getBundleCommitConcurrency());
                    int availableSlots = maxConcurrency - activeTransfers.get();
                    if (availableSlots > 0) {
                        int claimBatch = Math.min(5, availableSlots);
                        List<TaskItemDto> claimedTasks = orchestratorClient.claimTasks(
                                new ClaimTasksRequest(config.getAgentId(), config.getClusterId(), claimBatch)
                        );

                        if (!claimedTasks.isEmpty()) {
                            didWork = true;
                            for (TaskItemDto task : claimedTasks) {
                                if (!running.get()) break;

                                activeTransfers.incrementAndGet();
                                CompletableFuture.runAsync(() -> {
                                    try {
                                        sender.transferTask(task);
                                    } catch (Throwable t) {
                                        logger.error("Ошибка при параллельной передаче задачи {}: {}", task.getId(), t.getMessage(), t);
                                    } finally {
                                        activeTransfers.decrementAndGet();
                                    }
                                }, workerTaskPool);
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("Ошибка в цикле Sender/Worker: {}", t.getMessage(), t);
            }

            if (!didWork) {
                try {
                    Thread.sleep((long) (config.getPollIntervalSec() * 1000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public static boolean matchesCluster(String agentCluster, String jobCluster) {
        if (agentCluster == null || jobCluster == null) return true;
        if (agentCluster.equalsIgnoreCase(jobCluster)) return true;
        String aNorm = agentCluster.toLowerCase().replace("-", "").replace("_", "");
        String jNorm = jobCluster.toLowerCase().replace("-", "").replace("_", "");
        return aNorm.equals(jNorm);
    }

    /**
     * Разрешение gRPC адреса целевого узла репликации.
     */
    public String resolveTargetAddress(String targetClusterId) {
        if (targetClusterId == null || targetClusterId.isBlank()) {
            return config.getFallbackTargetAddress();
        }

        // 1. Проверка системной переменной AGENT_TARGET_<CLUSTER_ID>
        String envKey = "AGENT_TARGET_" + targetClusterId.toUpperCase().replace("-", "_");
        String envTarget = System.getenv(envKey);
        if (envTarget != null && !envTarget.isBlank()) {
            return envTarget.trim();
        }

        // 2. Опрос топологии Оркестратора (/api/v1/clusters)
        long now = System.currentTimeMillis();
        if (now - lastTopologySyncTime > 10_000 || !clusterAddressCache.containsKey(targetClusterId)) {
            try {
                List<ClusterDto> clusters = orchestratorClient.getClusters();
                for (ClusterDto c : clusters) {
                    if (c.getId() != null && c.getGrpcAddress() != null) {
                        clusterAddressCache.put(c.getId(), c.getGrpcAddress());
                    }
                }
                lastTopologySyncTime = now;
            } catch (Exception e) {
                logger.debug("Ошибка обновления топологии кластеров: {}", e.getMessage());
            }
        }

        String targetAddr = clusterAddressCache.get(targetClusterId);
        if (targetAddr != null && !targetAddr.isBlank()) {
            return targetAddr;
        }

        return config.getFallbackTargetAddress();
    }

    private void startStagingCleaner() {
        this.stagingCleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "hdfs-staging-cleaner");
            t.setDaemon(true);
            return t;
        });

        long intervalMin = config.getStagingCleanupIntervalMinutes();
        long ttlMillis = config.getStagingTtlMinutes() * 60 * 1000L;

        stagingCleanupExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (receiverService != null) {
                    int cleaned = receiverService.cleanOrphanedStaging(ttlMillis);
                    if (cleaned > 0) {
                        logger.info("[HdfsStagingCleaner] Периодическая очистка удалила {} осиротевших staging-файлов (TTL: {} мин)",
                                cleaned, config.getStagingTtlMinutes());
                    }
                }
            } catch (Exception e) {
                logger.warn("[HdfsStagingCleaner] Ошибка во время периодической очистки: {}", e.getMessage());
            }
        }, 1, intervalMin, TimeUnit.MINUTES);

        logger.info("Фоновый сборщик мусора HDFS Staging запущен (интервал: {} мин, TTL: {} мин)",
                intervalMin, config.getStagingTtlMinutes());
    }

    /**
     * Корректная остановка агента (Graceful Shutdown).
     */
    public synchronized void stop() {
        if (!running.get()) return;
        running.set(false);

        logger.info("Остановка Replicator Agent '{}'...", config.getAgentId());

        if (senderThread != null) {
            senderThread.interrupt();
        }

        if (hmsThread != null) {
            hmsThread.interrupt();
        }

        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }

        if (stagingCleanupExecutor != null) {
            stagingCleanupExecutor.shutdownNow();
        }

        if (hmsTaskExecutor != null) {
            hmsTaskExecutor.close();
        }

        if (config.isEnableDynamicRegistration()) {
            orchestratorClient.unregister(config.getAgentId());
        }

        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                grpcServer.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
            logger.info("gRPC Receiver сервер остановлен");
        }

        if (sender != null) {
            sender.close();
        }

        if (inotifyListener != null) {
            inotifyListener.stop();
        }

        if (workerTaskPool != null) {
            workerTaskPool.shutdown();
            try {
                workerTaskPool.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
        }

        if (hmsTaskPool != null) {
            hmsTaskPool.shutdown();
            try {
                hmsTaskPool.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
        }

        if (orchestratorClient != null) {
            orchestratorClient.close();
        }

        logger.info("Replicator Agent '{}' успешно остановлен", config.getAgentId());
    }

    public OrchestratorGrpcClient getOrchestratorClient() {
        return orchestratorClient;
    }

    public boolean isRunning() {
        return running.get();
    }

    public org.apache.hadoop.explorer.replicator.inotify.HdfsInotifyListener getInotifyListener() {
        return inotifyListener;
    }

    public ReplicatorAgentConfig getConfig() {
        return config;
    }
}
