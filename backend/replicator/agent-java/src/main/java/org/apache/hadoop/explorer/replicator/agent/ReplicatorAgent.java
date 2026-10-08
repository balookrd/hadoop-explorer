package org.apache.hadoop.explorer.replicator.agent;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.model.ClusterDto;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.sender.ReplicationSender;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        this.orchestratorClient = new OrchestratorClient(config.getOrchestratorUrl(), config.getAgentSecret());
        this.sender = new ReplicationSender(
                config.getAgentId(),
                orchestratorClient,
                fsManager,
                bandwidthLimiter,
                config.getChunkSize()
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
        if ("all".equals(config.getMode()) || "receiver".equals(config.getMode())) {
            startReceiverServer();
        }

        // 3. Sender Loop
        if ("all".equals(config.getMode()) || "sender".equals(config.getMode())) {
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

        this.grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(config.getReceiverHost(), config.getReceiverPort()))
                .addService(service)
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build()
                .start();

        logger.info("gRPC Receiver сервер успешно запущен и слушает {}:{}",
                config.getReceiverHost(), config.getReceiverPort());
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
        logger.info("Цикл Sender запущен (опрос очереди каждые {}с)", config.getPollIntervalSec());

        while (running.get()) {
            try {
                List<JobDto> jobs = orchestratorClient.getJobs();
                for (JobDto job : jobs) {
                    if (!running.get()) break;

                    if (!"QUEUED".equalsIgnoreCase(job.getStatus())) {
                        continue;
                    }

                    // Если агент привязан к конкретному кластеру, берем только задачи этого источника
                    if (config.getClusterId() != null && job.getSourceClusterId() != null
                            && !config.getClusterId().equalsIgnoreCase(job.getSourceClusterId())) {
                        continue;
                    }

                    String targetAddress = resolveTargetAddress(job.getTargetClusterId());

                    activeTransfers.incrementAndGet();
                    try {
                        sender.transferFile(job, targetAddress);
                    } finally {
                        activeTransfers.decrementAndGet();
                    }
                }
            } catch (Exception e) {
                logger.error("Ошибка в цикле Sender: {}", e.getMessage(), e);
            }

            try {
                Thread.sleep((long) (config.getPollIntervalSec() * 1000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
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
        if (now - lastTopologySyncTime > 30_000 || !clusterAddressCache.containsKey(targetClusterId)) {
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

        // 3. Резервный адрес
        logger.warn("Кластер '{}' не найден в топологии Оркестратора. Используется fallback адрес: {}",
                targetClusterId, config.getFallbackTargetAddress());
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
