package org.apache.hadoop.explorer.replicator.agent;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.sender.ReplicationSender;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
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
    private final OrchestratorClient orchestratorClient;
    private final ReplicationSender sender;

    private Server grpcServer;
    private ScheduledExecutorService heartbeatExecutor;
    private Thread senderThread;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeTransfers = new AtomicInteger(0);

    // Кэш gRPC адресов целевых кластеров
    private final Map<String, String> clusterAddressCache = new ConcurrentHashMap<>();
    private volatile long lastTopologySyncTime = 0;

    public ReplicatorAgent(ReplicatorAgentConfig config) {
        this.config = config;
        this.bandwidthLimiter = new LocalBandwidthLimiter(config.getMaxBandwidthMbS() != null ? config.getMaxBandwidthMbS() : 0.0);
        this.fsManager = new HadoopFsManager(config.getDefaultFsUri(), config.getKeytabPath(), config.getPrincipal());
        this.orchestratorClient = new OrchestratorClient(
                config.getOrchestratorUrl(),
                config.getAgentSecret(),
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
                config.isGrpcInsecureSkipVerify()
        );
    }

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
    }

    private void startReceiverServer() throws IOException {
        DataTransferServiceImpl service = new DataTransferServiceImpl(
                config.getStagingDir(),
                fsManager,
                bandwidthLimiter,
                config.getKeytabPath()
        );

        NettyServerBuilder serverBuilder = NettyServerBuilder.forAddress(new InetSocketAddress(config.getReceiverHost(), config.getReceiverPort()))
                .addService(service)
                .maxInboundMessageSize(64 * 1024 * 1024);

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
                        activeTransfers.get()
                );
                orchestratorClient.heartbeat(hbReq);
            } catch (Exception e) {
                logger.debug("Ошибка отправки heartbeat: {}", e.getMessage());
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void startSenderLoop() {
        senderThread = new Thread(this::runSenderLoop, "replicator-sender-loop");
        senderThread.setDaemon(true);
        senderThread.start();
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

                        logger.info("Агент '{}' взял задачу '{}' на анализ каталога и формирование пула задач",
                                config.getAgentId(), job.getId());
                        orchestratorClient.updateJobProgress(job.getId(), new UpdateJobRequest("ANALYZING", 0L, 0L,
                                "Анализ каталога и вычисление diff деревьев"));

                        String targetAddress = resolveTargetAddress(job.getTargetClusterId());
                        boolean ok = sender.analyzeAndCreateTaskPool(job, targetAddress);
                        if (ok) {
                            didWork = true;
                        }
                    }
                }

                // 2. Фаза воркера: забор и параллельное исполнение задач из распределенного пула
                if (canWork) {
                    List<TaskItemDto> claimedTasks = orchestratorClient.claimTasks(
                            new ClaimTasksRequest(config.getAgentId(), config.getClusterId(), 5)
                    );

                    if (!claimedTasks.isEmpty()) {
                        didWork = true;
                        for (TaskItemDto task : claimedTasks) {
                            if (!running.get()) break;

                            activeTransfers.incrementAndGet();
                            try {
                                sender.transferTask(task);
                            } finally {
                                activeTransfers.decrementAndGet();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Ошибка в цикле Sender/Worker: {}", e.getMessage(), e);
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
        if (jobCluster.toLowerCase().startsWith(agentCluster.toLowerCase() + "-")) return true;
        if (("dc1".equalsIgnoreCase(agentCluster) || "demo-cluster".equalsIgnoreCase(agentCluster))
                && (jobCluster.toLowerCase().contains("dc1") || jobCluster.toLowerCase().contains("demo-cluster"))) {
            return true;
        }
        if (("dc2".equalsIgnoreCase(agentCluster) || "backup-cluster".equalsIgnoreCase(agentCluster))
                && (jobCluster.toLowerCase().contains("dc2") || jobCluster.toLowerCase().contains("backup-cluster"))) {
            return true;
        }
        return false;
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
                        if ("dc1".equalsIgnoreCase(c.getId())) clusterAddressCache.put("demo-cluster", c.getGrpcAddress());
                        if ("dc2".equalsIgnoreCase(c.getId())) clusterAddressCache.put("backup-cluster", c.getGrpcAddress());
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
        if (("dc1".equalsIgnoreCase(targetClusterId) || "demo-cluster".equalsIgnoreCase(targetClusterId))
                && clusterAddressCache.containsKey("dc1")) {
            return clusterAddressCache.get("dc1");
        }
        if (("dc2".equalsIgnoreCase(targetClusterId) || "backup-cluster".equalsIgnoreCase(targetClusterId))
                && clusterAddressCache.containsKey("dc2")) {
            return clusterAddressCache.get("dc2");
        }

        // 3. Fallback по умолчанию для известных пар в Docker
        if ("dc1".equalsIgnoreCase(targetClusterId) || "demo-cluster".equalsIgnoreCase(targetClusterId)) {
            return "agent-dc1:50051";
        }
        if ("dc2".equalsIgnoreCase(targetClusterId) || "backup-cluster".equalsIgnoreCase(targetClusterId)) {
            return "agent-dc2:50051";
        }

        return config.getFallbackTargetAddress();
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

        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
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

        logger.info("Replicator Agent '{}' успешно остановлен", config.getAgentId());
    }

    public boolean isRunning() {
        return running.get();
    }

    public ReplicatorAgentConfig getConfig() {
        return config;
    }
}
