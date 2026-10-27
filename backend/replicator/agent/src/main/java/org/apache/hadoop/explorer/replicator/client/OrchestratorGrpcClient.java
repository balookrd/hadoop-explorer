package org.apache.hadoop.explorer.replicator.client;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.hadoop.explorer.replicator.generated.control.*;
import org.apache.hadoop.explorer.replicator.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Высокопроизводительный gRPC-клиент для взаимодействия агента с Оркестратором (Control Plane).
 *
 * <p>Поддерживает:
 * <ul>
 *   <li><b>Active-Active Multi-Endpoint (ORCHESTRATOR_GRPC_ADDRESSES)</b>: список узлов через запятую;</li>
 *   <li><b>Client-Side Random Load Balancing</b>: случайный выбор стартового узла для равномерного
 *       распределения сотен воркеров без промежуточного L7 LB;</li>
 *   <li><b>Автоматический прозрачный Failover</b>: при недоступности текущего узла запрос повторяется
 *       на следующем живом оркестраторе;</li>
 *   <li>HTTP/2 мультиплексирование в рамках постоянного TCP-соединения;</li>
 *   <li>L7-аутентификацию по заголовку X-Agent-Secret и TLS/mTLS.</li>
 * </ul>
 */
public class OrchestratorGrpcClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorGrpcClient.class);

    private final List<String> endpoints;
    private final AtomicInteger currentEndpointIndex;
    /**
     * Замок канала: {@link java.util.concurrent.locks.ReentrantLock}, а не монитор.
     *
     * <p>Под замком создается канал (чтение сертификатов TLS, разрешение имени) и ожидается
     * завершение старого канала до трех секунд. Монитор на этих местах пиннит несущий поток
     * виртуального потока Java 21, а переключение узла вызывается из обычного пути RPC.
     */
    private final java.util.concurrent.locks.ReentrantLock channelLock =
            new java.util.concurrent.locks.ReentrantLock();
    private final String agentSecret;
    private final boolean tlsEnabled;
    private final String trustCertCollectionPath;
    private final boolean insecureSkipVerify;

    private volatile String targetAddress;
    private volatile ManagedChannel channel;
    private volatile OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceBlockingStub blockingStub;

    /**
     * Конструктор для юнит-тестов и тестовых моков.
     */
    protected OrchestratorGrpcClient() {
        this.endpoints = List.of("test-in-memory");
        this.currentEndpointIndex = new AtomicInteger(0);
        this.targetAddress = "test-in-memory";
        this.agentSecret = null;
        this.tlsEnabled = false;
        this.trustCertCollectionPath = null;
        this.insecureSkipVerify = false;
        this.channel = null;
        this.blockingStub = null;
    }

    public OrchestratorGrpcClient(String orchestratorGrpcAddress, String agentSecret) {
        this(orchestratorGrpcAddress, agentSecret, false, null);
    }

    public OrchestratorGrpcClient(String orchestratorGrpcAddress, String agentSecret,
                                  boolean tlsEnabled, String trustCertCollectionPath) {
        this(orchestratorGrpcAddress, agentSecret, tlsEnabled, trustCertCollectionPath, false);
    }

    public OrchestratorGrpcClient(String orchestratorGrpcAddress, String agentSecret,
                                  boolean tlsEnabled, String trustCertCollectionPath,
                                  boolean insecureSkipVerify) {
        this(parseEndpoints(orchestratorGrpcAddress), agentSecret, tlsEnabled, trustCertCollectionPath, insecureSkipVerify);
    }

    public OrchestratorGrpcClient(List<String> rawEndpoints, String agentSecret,
                                  boolean tlsEnabled, String trustCertCollectionPath,
                                  boolean insecureSkipVerify) {
        this(rawEndpoints, agentSecret, tlsEnabled, trustCertCollectionPath, insecureSkipVerify, true);
    }

    public OrchestratorGrpcClient(List<String> rawEndpoints, String agentSecret,
                                  boolean tlsEnabled, String trustCertCollectionPath,
                                  boolean insecureSkipVerify, boolean shuffleEndpoints) {
        List<String> list = new ArrayList<>(rawEndpoints != null && !rawEndpoints.isEmpty()
                ? rawEndpoints
                : List.of("localhost:50050"));

        // Client-Side Random Shuffle для балансировки нагрузки между узлами Active-Active без L7 LB
        if (shuffleEndpoints && list.size() > 1) {
            Collections.shuffle(list, ThreadLocalRandom.current());
        }

        this.endpoints = Collections.unmodifiableList(list);
        this.currentEndpointIndex = new AtomicInteger(0);
        this.agentSecret = agentSecret;
        this.tlsEnabled = tlsEnabled;
        this.trustCertCollectionPath = trustCertCollectionPath;
        this.insecureSkipVerify = insecureSkipVerify;

        this.targetAddress = this.endpoints.get(0);
        this.channel = createChannel(this.targetAddress);
        this.blockingStub = OrchestratorControlPlaneServiceGrpc.newBlockingStub(this.channel);

        if (this.endpoints.size() > 1) {
            log.info("OrchestratorGrpcClient: инициализирован пул адресов {} (выбран стартовый узел: '{}', TLS={})",
                    this.endpoints, this.targetAddress, tlsEnabled);
        } else {
            log.info("OrchestratorGrpcClient инициализирован: target='{}' (TLS={})", this.targetAddress, tlsEnabled);
        }
    }

    /**
     * Конструктор для in-process тестирования
     */
    public OrchestratorGrpcClient(ManagedChannel channel, String agentSecret) {
        this.endpoints = List.of("in-process");
        this.currentEndpointIndex = new AtomicInteger(0);
        this.targetAddress = "in-process";
        this.agentSecret = agentSecret;
        this.tlsEnabled = false;
        this.trustCertCollectionPath = null;
        this.insecureSkipVerify = false;
        this.channel = channel;
        this.blockingStub = OrchestratorControlPlaneServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Парсинг строки адресов (одиночный адрес или список через запятую / точку с запятой / пробел).
     */
    public static List<String> parseEndpoints(String rawAddresses) {
        if (rawAddresses == null || rawAddresses.isBlank()) {
            return List.of("localhost:50050");
        }
        String[] parts = rawAddresses.split("[,;\\s]+");
        List<String> list = new ArrayList<>();
        for (String part : parts) {
            String addr = part.trim();
            if (addr.isEmpty()) continue;
            if (addr.startsWith("grpc://")) {
                addr = addr.substring("grpc://".length());
            } else if (addr.startsWith("http://")) {
                addr = addr.substring("http://".length());
            } else if (addr.startsWith("https://")) {
                addr = addr.substring("https://".length());
            }
            if (!addr.contains(":")) {
                addr = addr + ":50050";
            }
            if (!list.contains(addr)) {
                list.add(addr);
            }
        }
        return list.isEmpty() ? List.of("localhost:50050") : list;
    }

    private ManagedChannel createChannel(String address) {
        NettyChannelBuilder builder = NettyChannelBuilder.forTarget(address)
                .maxInboundMessageSize(16 * 1024 * 1024)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true);

        if (tlsEnabled) {
            try {
                var sslBuilder = GrpcSslContexts.forClient();
                if (trustCertCollectionPath != null && !trustCertCollectionPath.isBlank()) {
                    File trustFile = new File(trustCertCollectionPath);
                    if (!trustFile.isFile() || !trustFile.canRead()) {
                        throw new IllegalStateException("Файл доверенных сертификатов недоступен для чтения: "
                                + trustCertCollectionPath);
                    }
                    sslBuilder.trustManager(trustFile);
                } else if (insecureSkipVerify) {
                    sslBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                }
                builder.sslContext(sslBuilder.build());
            } catch (Exception e) {
                // Откат в plaintext недопустим: канал control plane несет x-agent-secret,
                // и молчаливое ослабление защиты оператор не заметит.
                throw new IllegalStateException("Сбой настройки TLS для OrchestratorGrpcClient к '"
                        + address + "': " + e.getMessage(), e);
            }
        } else {
            builder.usePlaintext();
        }

        // Трассировка ставится всегда: вызовы агента к оркестратору — начало трассы операции
        builder.intercept(org.apache.hadoop.explorer.replicator.telemetry.GrpcTracing.clientInterceptor());

        // L7 Client Interceptor для передачи x-agent-secret
        if (agentSecret != null && !agentSecret.isBlank()) {
            builder.intercept(new ClientInterceptor() {
                private final Metadata.Key<String> SECRET_HEADER =
                        Metadata.Key.of("x-agent-secret", Metadata.ASCII_STRING_MARSHALLER);

                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                    return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                        @Override
                        public void start(Listener<RespT> responseListener, Metadata headers) {
                            headers.put(SECRET_HEADER, agentSecret.trim());
                            super.start(responseListener, headers);
                        }
                    };
                }
            });
        }

        return builder.build();
    }

    private OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceBlockingStub stub() {
        return blockingStub != null ? blockingStub.withDeadlineAfter(15, TimeUnit.SECONDS) : null;
    }

    @FunctionalInterface
    public interface GrpcCall<T> {
        T execute(OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceBlockingStub stub);
    }

    private <T> T executeWithFailover(String operationName, GrpcCall<T> call) {
        if (channel == null && blockingStub == null) {
            // Для мок-клиентов в тестовом окружении
            return null;
        }
        int maxAttempts = Math.max(1, endpoints.size());
        Exception lastException = null;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            String activeTarget = this.targetAddress;
            try {
                var s = stub();
                if (s == null) {
                    throw new IllegalStateException("gRPC stub не инициализирован для " + activeTarget);
                }
                return call.execute(s);
            } catch (io.grpc.StatusRuntimeException e) {
                lastException = e;
                io.grpc.Status.Code code = e.getStatus().getCode();
                boolean isFailoverEligible = (
                        code == io.grpc.Status.Code.UNAVAILABLE ||
                        code == io.grpc.Status.Code.DEADLINE_EXCEEDED ||
                        code == io.grpc.Status.Code.RESOURCE_EXHAUSTED
                );
                if (isFailoverEligible && endpoints.size() > 1 && attempt + 1 < maxAttempts) {
                    log.warn("[OrchestratorFailover] Сбой '{}' на узле '{}' ({}: {}). Переключение на следующий узел (попытка {} из {})...",
                            operationName, activeTarget, code, e.getMessage(), attempt + 1, maxAttempts);
                    switchToNextChannel(activeTarget, e);
                    continue;
                }
                throw e;
            } catch (Exception e) {
                lastException = e;
                if (endpoints.size() > 1 && attempt + 1 < maxAttempts) {
                    log.warn("[OrchestratorFailover] Ошибка '{}' на узле '{}' ({}). Переключение на следующий узел (попытка {} из {})...",
                            operationName, activeTarget, e.getMessage(), attempt + 1, maxAttempts);
                    switchToNextChannel(activeTarget, e);
                    continue;
                }
                throw (e instanceof RuntimeException re) ? re : new RuntimeException(e);
            }
        }
        throw (lastException instanceof RuntimeException re) ? re : new RuntimeException(lastException);
    }

    private void switchToNextChannel(String failedEndpoint, Throwable cause) {
        if (endpoints.size() <= 1) return;
        channelLock.lock();
        try {
            // Если другой поток уже переключил узел
            if (this.targetAddress != null && !this.targetAddress.equals(failedEndpoint)) {
                return;
            }
            int oldIdx = currentEndpointIndex.get();
            int newIdx = (oldIdx + 1) % endpoints.size();
            currentEndpointIndex.set(newIdx);
            String newTarget = endpoints.get(newIdx);

            log.info("[OrchestratorFailover] Активный узел оркестратора изменен: '{}' ➔ '{}' (всего узлов в пуле: {})",
                    failedEndpoint, newTarget, endpoints.size());

            ManagedChannel oldChannel = this.channel;
            this.targetAddress = newTarget;
            this.channel = createChannel(newTarget);
            this.blockingStub = OrchestratorControlPlaneServiceGrpc.newBlockingStub(this.channel);

            if (oldChannel != null) {
                try {
                    oldChannel.shutdownNow();
                } catch (Exception ignored) {}
            }
        } finally {
            channelLock.unlock();
        }
    }

    /**
     * Первичная регистрация агента в реестре Оркестратора.
     */
    public boolean register(org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest request) {
        try {
            org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterRequest protoReq =
                    org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterRequest.newBuilder()
                            .setAgentId(request.getAgentId() != null ? request.getAgentId() : "")
                            .setClusterId(request.getClusterId() != null ? request.getClusterId() : "default")
                            .setMode(request.getMode() != null ? request.getMode() : "all")
                            .setAdvertisedGrpcAddress(request.getGrpcAddress() != null ? request.getGrpcAddress() : "")
                            .setSourceAddress(request.getHostname() != null ? request.getHostname() : "")
                            .setMaxBandwidthMbS(request.getMaxBandwidthMbS() != null ? request.getMaxBandwidthMbS() : 0.0)
                            .build();

            var resp = executeWithFailover("registerAgent", s -> s.registerAgent(protoReq));
            if (resp == null) return false;
            log.info("Агент '{}' успешно зарегистрирован в Оркестраторе по gRPC (status: {})",
                    request.getAgentId(), resp.getStatus());
            return resp.getSuccess();
        } catch (Exception e) {
            log.warn("Ошибка gRPC регистрации агента '{}': {}", request.getAgentId(), e.getMessage());
            return false;
        }
    }

    /**
     * Периодический Heartbeat / Keepalive пинг агента.
     */
    public boolean heartbeat(org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest request) {
        try {
            var builder = org.apache.hadoop.explorer.replicator.generated.control.AgentHeartbeatRequest.newBuilder()
                    .setAgentId(request.getAgentId() != null ? request.getAgentId() : "")
                    .setClusterId(request.getClusterId() != null ? request.getClusterId() : "")
                    .setAdvertisedGrpcAddress(request.getGrpcAddress() != null ? request.getGrpcAddress() : "")
                    .setActiveTransfers(request.getActiveTransfers())
                    .setMode(request.getMode() != null ? request.getMode() : "")
                    .setTransferredBytes(request.getTransferredBytes())
                    .setTransferErrors(request.getTransferErrors())
                    .setBandwidthLimitMbS(request.getBandwidthLimitMbS() != null ? request.getBandwidthLimitMbS() : 0.0)
                    .setCompressionRawBytes(request.getCompressionRawBytes())
                    .setCompressionCompressedBytes(request.getCompressionCompressedBytes())
                    .setCompressionRatio(request.getCompressionRatio() != null ? request.getCompressionRatio() : 1.0)
                    .setChunkLatencyAvgMs(request.getChunkLatencyAvgMs() != null ? request.getChunkLatencyAvgMs() : 0.0)
                    .setChunkLatencyMaxMs(request.getChunkLatencyMaxMs() != null ? request.getChunkLatencyMaxMs() : 0.0)
                    .setChunkTransferCount(request.getChunkTransferCount())
                    .setStagingActiveFiles(request.getStagingActiveFiles())
                    .setStagingActiveBytes(request.getStagingActiveBytes())
                    .setStagingOrphansCleaned(request.getStagingOrphansCleaned())
                    .setKafkaMessagesReplicated(request.getKafkaMessagesReplicated())
                    .setKafkaBytesReplicated(request.getKafkaBytesReplicated())
                    .setKafkaCurrentLagMs(request.getKafkaCurrentLagMs());

            var resp = executeWithFailover("heartbeat", s -> s.heartbeat(builder.build()));
            return resp != null && resp.getAcknowledged();
        } catch (Exception e) {
            log.debug("Ошибка отправки gRPC heartbeat агента '{}': {}", request.getAgentId(), e.getMessage());
            return false;
        }
    }

    /**
     * Снятие агента с регистрации при штатной остановке.
     */
    public boolean unregister(String agentId) {
        try {
            var protoReq = AgentUnregisterRequest.newBuilder().setAgentId(agentId).build();
            var resp = executeWithFailover("unregisterAgent", s -> s.unregisterAgent(protoReq));
            return resp != null && resp.getSuccess();
        } catch (Exception e) {
            log.debug("Ошибка gRPC unregister агента '{}': {}", agentId, e.getMessage());
            return false;
        }
    }

    /**
     * Запрос квоты сетевой полосы (Hierarchical Token Bucket).
     */
    public double requestNetworkTokens(org.apache.hadoop.explorer.replicator.model.TokenRequest request) {
        try {
            var protoReq = org.apache.hadoop.explorer.replicator.generated.control.TokenRequest.newBuilder()
                    .setRequestedBytes(request.getRequestedBytes())
                    .setSourceClusterId(request.getSourceClusterId() != null ? request.getSourceClusterId() : "")
                    .setTargetClusterId(request.getTargetClusterId() != null ? request.getTargetClusterId() : "")
                    .build();

            var resp = executeWithFailover("requestTokens", s -> s.requestTokens(protoReq));
            return resp != null ? resp.getWaitSeconds() : 0.0;
        } catch (Exception e) {
            log.debug("Исключение при gRPC запросе квоты у Оркестратора: {}", e.getMessage());
            return 0.0;
        }
    }

    /**
     * Получение всех задач репликации из Оркестратора.
     */
    public List<JobDto> getJobs() {
        return getJobs(null, false);
    }

    /**
     * Задания в статусе {@code status} (все при {@code null}), старые первыми по {@code oldestFirst}.
     * Отбор идет на стороне оркестратора: выборка ограничена, и без него нужные задания
     * терялись среди тысяч завершенных подзадач.
     */
    public List<JobDto> getJobs(String status, boolean oldestFirst) {
        try {
            var builder = GetJobsRequest.newBuilder().setIncludeSubjobs(true).setOldestFirst(oldestFirst);
            if (status != null) {
                builder.setStatus(status);
            }
            var protoReq = builder.build();
            var resp = executeWithFailover("getJobs", s -> s.getJobs(protoReq));
            if (resp == null) return Collections.emptyList();

            List<JobDto> result = new ArrayList<>(resp.getJobsCount());
            for (JobItemProto job : resp.getJobsList()) {
                result.add(toJobDto(job));
            }
            return result;
        } catch (Exception e) {
            log.warn("Ошибка gRPC получения списка задач из Оркестратора: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private static JobDto toJobDto(JobItemProto job) {
        JobDto dto = new JobDto(
                job.getId(),
                job.getSourceClusterId(),
                job.getTargetClusterId(),
                job.getSourcePath(),
                job.getTargetPath(),
                job.getStatus(),
                job.getExecutionPrincipal(),
                job.getRunAsServiceAccount()
        );
        dto.setTotalBytes(job.getTotalBytes());
        dto.setCopiedBytes(job.getProcessedBytes());
        dto.setMessage(job.getMessage());
        dto.setSyncMode(job.getSyncMode());
        dto.setSyncDeletes(job.getSyncDeletes());
        dto.setJobType(job.getJobType());
        dto.setParentJobId(job.getParentJobId());
        dto.setDeleteTableDirs(job.getDeleteTableDirs());
        dto.setDeleteInsideTables(job.getDeleteInsideTables());
        return dto;
    }

    /** Предел идентификаторов в одном точечном запросе статусов. */
    private static final int JOB_STATUS_BATCH = 1000;

    /**
     * Статусы конкретных заданий точечным запросом.
     *
     * <p>Сбой связи пробрасывается, а не превращается в пустой ответ: для ожидания подзадач
     * данных «не удалось спросить» и «спросили, задач нет» — разные вещи.
     */
    public Map<String, JobDto> getJobsByIds(Collection<String> jobIds) throws Exception {
        Map<String, JobDto> result = new HashMap<>();
        List<String> ids = new ArrayList<>(jobIds);
        for (int from = 0; from < ids.size(); from += JOB_STATUS_BATCH) {
            var protoReq = GetJobsRequest.newBuilder()
                    .setIncludeSubjobs(true)
                    .addAllJobIds(ids.subList(from, Math.min(from + JOB_STATUS_BATCH, ids.size())))
                    .build();
            var resp = executeWithFailover("getJobs", s -> s.getJobs(protoReq));
            if (resp == null) {
                throw new IllegalStateException("Оркестратор не вернул статусы заданий");
            }
            for (JobItemProto job : resp.getJobsList()) {
                result.put(job.getId(), toJobDto(job));
            }
        }
        return result;
    }

    /**
     * Получение конкретной задачи по идентификатору.
     */
    public Optional<JobDto> getJobById(String jobId) {
        if (jobId == null || jobId.isBlank()) return Optional.empty();
        try {
            return Optional.ofNullable(getJobsByIds(List.of(jobId)).get(jobId));
        } catch (Exception e) {
            log.warn("Ошибка gRPC получения задачи '{}': {}", jobId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Ожидание успешного завершения HDFS саб-джобы (Data-First Consistency Barrier).
     */
    public boolean awaitSubjobCompletion(String subjobId, long timeoutSeconds) {
        if (subjobId == null || subjobId.isBlank()) {
            return true;
        }
        return awaitSubjobsCompletion(List.of(subjobId), timeoutSeconds);
    }

    /**
     * Ожидание завершения группы подзадач данных (Data-First Barrier).
     *
     * <p>Барьер открывается только по подтвержденному успеху каждой подзадачи. Сбой связи с
     * оркестратором — не повод пропустить DDL без данных: опрос повторяется до таймаута.
     * Интервал растет от секунды до 15 с — сотни агентов, ждущих многочасовое копирование,
     * не должны опрашивать оркестратор дважды в секунду.
     *
     * @return {@code true}, если все подзадачи завершились успешно до истечения таймаута
     */
    public boolean awaitSubjobsCompletion(Collection<String> subjobIds, long timeoutSeconds) {
        Set<String> pending = new HashSet<>();
        for (String id : subjobIds) {
            if (id != null && !id.isBlank()) {
                pending.add(id);
            }
        }
        long deadline = System.currentTimeMillis() + Math.max(timeoutSeconds, 1) * 1000L;
        long sleepMs = 1000L;
        while (!pending.isEmpty()) {
            try {
                Map<String, JobDto> statuses = getJobsByIds(pending);
                for (Iterator<String> it = pending.iterator(); it.hasNext(); ) {
                    String id = it.next();
                    JobDto job = statuses.get(id);
                    if (job == null) {
                        log.warn("Data-First Barrier: HDFS подзадача {} не найдена в оркестраторе", id);
                        return false;
                    }
                    String status = job.getStatus();
                    if ("COMPLETED".equalsIgnoreCase(status) || "SUCCESS".equalsIgnoreCase(status)) {
                        it.remove();
                    } else if ("FAILED".equalsIgnoreCase(status) || "ERROR".equalsIgnoreCase(status)
                            || "CANCELLED".equalsIgnoreCase(status) || "STOPPED".equalsIgnoreCase(status)) {
                        log.warn("Data-First Barrier: HDFS подзадача {} завершилась без успеха ({}): {}",
                                id, status, job.getMessage());
                        return false;
                    }
                }
            } catch (Exception e) {
                log.warn("Data-First Barrier: не удалось получить статусы подзадач, повтор: {}", e.getMessage());
            }
            if (pending.isEmpty()) {
                break;
            }
            if (System.currentTimeMillis() + sleepMs > deadline) {
                log.warn("Data-First Barrier: истек таймаут ожидания HDFS подзадач {} ({} с)", pending, timeoutSeconds);
                return false;
            }
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            sleepMs = Math.min(sleepMs * 2, 15_000L);
        }
        return true;
    }

    /**
     * Обновление статуса и прогресса выполнения задачи.
     */
    public boolean updateJobProgress(String jobId, UpdateJobRequest request) {
        try {
            var protoReq = UpdateJobProgressRequest.newBuilder()
                    .setJobId(jobId != null ? jobId : "")
                    .setStatus(request.getStatus() != null ? request.getStatus() : "")
                    .setCurrentBytes(request.getCopiedBytes() != null ? request.getCopiedBytes() : 0L)
                    .setTotalBytes(request.getTotalBytes() != null ? request.getTotalBytes() : 0L)
                    .setMessage(request.getMessage() != null ? request.getMessage() : "")
                    .build();

            var resp = executeWithFailover("updateJobProgress", s -> s.updateJobProgress(protoReq));
            return resp != null && resp.getSuccess();
        } catch (Exception e) {
            log.warn("Ошибка gRPC обновления прогресса задачи '{}': {}", jobId, e.getMessage());
            return false;
        }
    }

    /**
     * Обновление стриминг txid и lag для Inotify.
     */
    public boolean updateJobStreamingTxid(String jobId, long txid, long lag) {
        try {
            var protoReq = UpdateJobProgressRequest.newBuilder()
                    .setJobId(jobId)
                    .setStatus("STREAMING")
                    .setLastProcessedTxid(txid)
                    .setTxidLag(lag)
                    .setMessage("Inotify поток активен: txid=" + txid + " (лаг=" + lag + ")")
                    .build();

            var resp = executeWithFailover("updateJobStreamingTxid", s -> s.updateJobProgress(protoReq));
            return resp != null && resp.getSuccess();
        } catch (Exception e) {
            log.debug("Ошибка gRPC обновления txid для задачи '{}': {}", jobId, e.getMessage());
            return false;
        }
    }

    /**
     * Получение топологии кластеров.
     */
    public List<ClusterDto> getClusters() {
        try {
            var resp = executeWithFailover("getClusters", s -> s.getClusters(GetClustersRequest.getDefaultInstance()));
            if (resp == null) return Collections.emptyList();
            List<ClusterDto> result = new ArrayList<>(resp.getClustersCount());
            for (ClusterItemProto cl : resp.getClustersList()) {
                ClusterDto dto = new ClusterDto();
                dto.setId(cl.getId());
                dto.setName(cl.getName());
                dto.setDcId(cl.getDcId());
                dto.setGrpcAddress(cl.getGrpcAddress());
                result.add(dto);
            }
            return result;
        } catch (Exception e) {
            log.debug("Ошибка gRPC получения топологии кластеров: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Пакетная регистрация пофайловых задач в пуле Оркестратора.
     */
    public boolean batchCreateTasks(org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest request) {
        try {
            var builder = org.apache.hadoop.explorer.replicator.generated.control.BatchCreateTasksRequest.newBuilder()
                    .setJobId(request.getJobId() != null ? request.getJobId() : "");

            if (request.getTasks() != null) {
                for (TaskCreateItem item : request.getTasks()) {
                    builder.addTasks(TaskCreateItemProto.newBuilder()
                            .setSourcePath(item.getSourcePath() != null ? item.getSourcePath() : "")
                            .setTargetPath(item.getTargetPath() != null ? item.getTargetPath() : "")
                            .setFileSize(item.getFileSize())
                            .setTaskType(item.getTaskType() != null ? item.getTaskType() : "FILE")
                            .setManifestJson(item.getBundleManifest() != null ? item.getBundleManifest() : "")
                            .build());
                }
            }

            var resp = executeWithFailover("batchCreateTasks", s -> s.batchCreateTasks(builder.build()));
            return resp != null && resp.getSuccess();
        } catch (Exception e) {
            log.warn("Ошибка gRPC пакетной регистрации задач для job '{}': {}", request.getJobId(), e.getMessage());
            return false;
        }
    }

    public boolean batchCreateTasks(String jobId, org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest request) {
        if (request != null && jobId != null) {
            request.setJobId(jobId);
        }
        return batchCreateTasks(request);
    }

    /**
     * Забор задач из распределенного пула Оркестратора свободным воркером.
     */
    public List<TaskItemDto> claimTasks(org.apache.hadoop.explorer.replicator.model.ClaimTasksRequest request) {
        try {
            var protoReq = org.apache.hadoop.explorer.replicator.generated.control.ClaimTasksRequest.newBuilder()
                    .setWorkerId(request.getAgentId() != null ? request.getAgentId() : "")
                    .setClusterId(request.getClusterId() != null ? request.getClusterId() : "")
                    .setBatchSize(request.getLimit() > 0 ? request.getLimit() : 5)
                    .build();

            var resp = executeWithFailover("claimTasks", s -> s.claimTasks(protoReq));
            if (resp == null) return Collections.emptyList();
            List<TaskItemDto> result = new ArrayList<>(resp.getTasksCount());
            for (TaskItemProto t : resp.getTasksList()) {
                TaskItemDto dto = new TaskItemDto(
                        t.getTaskId(),
                        t.getJobId(),
                        null,
                        t.getSourcePath(),
                        t.getTargetPath(),
                        t.getFileSize(),
                        t.getStatus(),
                        request.getAgentId(),
                        null,
                        t.getTargetGrpcAddress(),
                        t.getExecutionPrincipal(),
                        t.getRunAsServiceAccount(),
                        t.getRetryCount(),
                        3,
                        null,
                        t.getTaskType(),
                        1,
                        t.getManifestJson(),
                        t.getSourceClusterId(),
                        t.getTargetClusterId()
                );
                dto.setTaskToken(t.getTaskToken());
                result.add(dto);
            }
            return result;
        } catch (Exception e) {
            log.debug("Ошибка gRPC получения задач из пула Оркестратора: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Фиксация успешного завершения пофайловой задачи воркером.
     */
    public boolean completeTask(org.apache.hadoop.explorer.replicator.model.CompleteTaskRequest request) {
        try {
            var protoReq = org.apache.hadoop.explorer.replicator.generated.control.CompleteTaskRequest.newBuilder()
                    .setTaskId(request.getTaskId() != null ? request.getTaskId() : "")
                    .setWorkerId(request.getAgentId() != null ? request.getAgentId() : "")
                    .setBytesWritten(request.getBytesTransferred())
                    .setMessage("OK")
                    .setCommittedFiles(request.getFilesCount())
                    .build();

            var resp = executeWithFailover("completeTask", s -> s.completeTask(protoReq));
            return resp != null && resp.getSuccess();
        } catch (Exception e) {
            log.warn("Ошибка gRPC фиксации завершения задачи '{}': {}", request.getTaskId(), e.getMessage());
            return false;
        }
    }

    /**
     * Фиксация ошибки передачи файла воркером.
     */
    public boolean failTask(org.apache.hadoop.explorer.replicator.model.FailTaskRequest request) {
        try {
            var protoReq = org.apache.hadoop.explorer.replicator.generated.control.FailTaskRequest.newBuilder()
                    .setTaskId(request.getTaskId() != null ? request.getTaskId() : "")
                    .setWorkerId(request.getAgentId() != null ? request.getAgentId() : "")
                    .setErrorMessage(request.getErrorMessage() != null ? request.getErrorMessage() : "Error")
                    .build();

            var resp = executeWithFailover("failTask", s -> s.failTask(protoReq));
            return resp != null && resp.getSuccess();
        } catch (Exception e) {
            log.warn("Ошибка gRPC фиксации сбоя задачи '{}': {}", request.getTaskId(), e.getMessage());
            return false;
        }
    }

    /**
     * Продление распределенной аренды Inotify стримера (Active-Standby).
     */
    public org.apache.hadoop.explorer.replicator.model.StreamingLeaseRenewResponse renewStreamingLease(
            org.apache.hadoop.explorer.replicator.model.StreamingLeaseRenewRequest req) {
        try {
            var protoReq = org.apache.hadoop.explorer.replicator.generated.control.StreamingLeaseRenewRequest.newBuilder()
                    .setClusterId(req.getClusterId() != null ? req.getClusterId() : "")
                    .setStreamerId(req.getAgentId() != null ? req.getAgentId() : "")
                    .build();

            var resp = executeWithFailover("renewStreamingLease", s -> s.renewStreamingLease(protoReq));
            if (resp == null) return null;
            var modelResp = new org.apache.hadoop.explorer.replicator.model.StreamingLeaseRenewResponse();
            modelResp.setStatus(resp.getGranted() ? "ACTIVE" : "STANDBY");
            modelResp.setEpoch(1L);
            modelResp.setActiveAgentId(resp.getLeaderId());
            modelResp.setLastCommittedTxid(resp.getMaxActiveTxid());
            modelResp.setQueueSaturated(resp.getQueueSaturated());
            modelResp.setRegisteredStreamersCount(1);
            modelResp.setRedundancyWarning(false);
            return modelResp;
        } catch (Exception e) {
            log.warn("Исключение при gRPC продлении аренды стримера для кластера '{}': {}",
                    req.getClusterId(), e.getMessage());
            return null;
        }
    }

    /**
     * Получение ожидающих задач HMS.
     */
    /** Агент, от имени которого заводятся подзадачи данных (fencing на оркестраторе). */
    private volatile String agentId;

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    /** Оркестратор отказал в подзадаче: HMS-задача удалена, остановлена или отобрана. */
    public static final class SubjobRejectedException extends RuntimeException {
        public SubjobRejectedException(String hmsJobId) {
            super("Оркестратор отклонил подзадачу данных для HMS-задачи " + hmsJobId + " (fencing)");
        }
    }

    public List<HmsPendingJobDto> getPendingHmsJobs(String clusterId, String agentId) {
        try {
            var protoReq = GetPendingHmsJobsRequest.newBuilder()
                    .setClusterId(clusterId != null ? clusterId : "")
                    .setAgentId(agentId != null ? agentId : "")
                    .build();

            var resp = executeWithFailover("getPendingHmsJobs", s -> s.getPendingHmsJobs(protoReq));
            if (resp == null) return Collections.emptyList();
            List<HmsPendingJobDto> list = new ArrayList<>(resp.getJobsCount());
            for (HmsPendingJobItemProto j : resp.getJobsList()) {
                list.add(new HmsPendingJobDto(
                        j.getJobId(),
                        j.getSourceCluster(),
                        j.getTargetCluster(),
                        j.getSourceDb(),
                        j.getTargetDb(),
                        j.getTablePattern(),
                        j.getDropExtraneousTables(),
                        j.getDropExtraneousPartitions(),
                        j.getTargetAgentGrpcAddress(),
                        j.getStatus(),
                        j.getLastProcessedEventId(),
                        j.getAssignedAgentId().isEmpty() ? null : j.getAssignedAgentId(),
                        j.getExecutionPrincipal().isEmpty() ? null : j.getExecutionPrincipal(),
                        j.getEnableHdfsStreaming(),
                        j.getCompanionHdfsJobId().isEmpty() ? null : j.getCompanionHdfsJobId(),
                        j.getDbWarehouseLocation().isEmpty() ? null : j.getDbWarehouseLocation(),
                        j.getBootstrapEventId() > 0 ? j.getBootstrapEventId() : null,
                        j.getBootstrapCheckpoint().isEmpty() ? null : j.getBootstrapCheckpoint()
                ));
            }
            return list;
        } catch (Exception e) {
            log.warn("Ошибка gRPC получения ожидающих HMS задач: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public List<HmsPendingJobDto> getPendingHmsJobs(String clusterId) {
        return getPendingHmsJobs(clusterId, null);
    }

    /**
     * Отчет о прогрессе HMS задачи.
     */
    /**
     * Отправка прогресса HMS-задачи.
     *
     * @return {@code false} только когда оркестратор явно отклонил отчет (fencing: задачу
     *         отобрали у этого агента); сбой связи отказом не считается
     */
    public boolean reportHmsProgress(String jobId, HmsProgressReportRequest report) {
        try {
            var protoReqBuilder = ReportHmsProgressRequest.newBuilder()
                    .setJobId(jobId != null ? jobId : "")
                    .setStatus(report.status() != null ? report.status() : "")
                    .setMessage(report.message() != null ? report.message() : "")
                    .setTablesSynced(report.replicatedTables())
                    .setPartitionsSynced(report.replicatedPartitions())
                    .setLastEventId(report.lastProcessedEventId() != null ? report.lastProcessedEventId() : 0L)
                    .setBootstrapEventId(report.bootstrapEventId() != null ? report.bootstrapEventId() : 0L)
                    .setEventLag(report.eventLag() != null ? report.eventLag() : 0L)
                    .setTotalTables(report.totalTables())
                    .setTotalPartitions(report.totalPartitions())
                    .setAgentId(report.agentId() != null ? report.agentId() : "")
                    .setBootstrapCheckpoint(report.bootstrapCheckpoint() != null ? report.bootstrapCheckpoint() : "");

            if (report.events() != null && !report.events().isEmpty()) {
                for (var ev : report.events()) {
                    protoReqBuilder.addEvents(HmsEventReportProto.newBuilder()
                            .setEventId(ev.eventId())
                            .setEventType(ev.eventType() != null ? ev.eventType() : "")
                            .setTableName(ev.tableName() != null ? ev.tableName() : "")
                            .setPartitionName(ev.partitionName() != null ? ev.partitionName() : "")
                            .setSourceUri(ev.sourceUri() != null ? ev.sourceUri() : "")
                            .setTargetUri(ev.targetUri() != null ? ev.targetUri() : "")
                            .setSubjobId(ev.subjobId() != null ? ev.subjobId() : "")
                            .setStatus(ev.status() != null ? ev.status() : "")
                            .setErrorMessage(ev.errorMessage() != null ? ev.errorMessage() : "")
                            .build());
                }
            }

            var resp = executeWithFailover("reportHmsProgress", s -> s.reportHmsProgress(protoReqBuilder.build()));
            return resp == null || resp.getSuccess();
        } catch (Exception e) {
            // Сбой связи — не отказ: если оркестратор недоступен долго, аренда истечет сама,
            // а прерывать перенос из-за кратковременного обрыва незачем
            log.warn("Ошибка gRPC отправки прогресса HMS задачи '{}': {}", jobId, e.getMessage());
            return true;
        }
    }

    /**
     * Создание HDFS саб-джобы для HMS.
     */
    public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl, String executionPrincipal) {
        return createHdfsSubjob(hmsJobId, srcPath, dstPath, srcCl, dstCl, executionPrincipal, null, null);
    }

    public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl, String executionPrincipal, String syncMode, String jobType) {
        return createHdfsSubjob(hmsJobId, srcPath, dstPath, srcCl, dstCl, executionPrincipal, syncMode, jobType, false);
    }

    /**
     * Свежая подзадача копирования для события INSERT: оркестратор не переиспользует идущую или
     * недавно завершенную копию того же каталога — она новых файлов не видела.
     */
    public String createFreshHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl, String executionPrincipal) {
        return createHdfsSubjob(hmsJobId, srcPath, dstPath, srcCl, dstCl, executionPrincipal, null, null, true);
    }

    private String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl,
                                    String executionPrincipal, String syncMode, String jobType, boolean forceNew) {
        try {
            var protoReq = CreateHdfsSubjobRequest.newBuilder()
                    .setParentJobId(hmsJobId != null ? hmsJobId : "")
                    .setSourcePath(srcPath != null ? srcPath : "")
                    .setTargetPath(dstPath != null ? dstPath : "")
                    .setSourceCluster(srcCl != null ? srcCl : "default")
                    .setTargetCluster(dstCl != null ? dstCl : "default")
                    .setExecutionPrincipal(executionPrincipal != null ? executionPrincipal : "")
                    .setSyncMode(syncMode != null ? syncMode : "")
                    .setJobType(jobType != null ? jobType : "")
                    .setAgentId(agentId != null ? agentId : "")
                    .setForceNew(forceNew)
                    .build();

            var resp = executeWithFailover("createHdfsSubjob", s -> s.createHdfsSubjob(protoReq));
            if (resp != null && resp.getFenced()) {
                throw new SubjobRejectedException(hmsJobId);
            }
            return (resp != null && resp.getSuccess()) ? resp.getJobId() : null;
        } catch (SubjobRejectedException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Ошибка gRPC создания HDFS саб-джобы для HMS '{}': {}", hmsJobId, e.getMessage());
            return null;
        }
    }

    public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl) {
        return createHdfsSubjob(hmsJobId, srcPath, dstPath, srcCl, dstCl, null);
    }

    /**
     * Получение активных задач репликации Kafka для дата-центра.
     */
    public List<KafkaJobItemProto> getActiveKafkaJobs(String dcId) {
        try {
            var protoReq = GetActiveKafkaJobsRequest.newBuilder()
                    .setDcId(dcId != null ? dcId : "")
                    .build();

            var resp = executeWithFailover("getActiveKafkaJobs", s -> s.getActiveKafkaJobs(protoReq));
            return resp != null ? resp.getJobsList() : Collections.emptyList();
        } catch (Exception e) {
            log.warn("Ошибка gRPC получения активных Kafka задач для ДЦ '{}': {}", dcId, e.getMessage());
            return Collections.emptyList();
        }
    }

    public String getTargetAddress() {
        return targetAddress;
    }

    public List<String> getEndpoints() {
        return endpoints;
    }

    public int getCurrentEndpointIndex() {
        return currentEndpointIndex.get();
    }

    @Override
    public void close() {
        channelLock.lock();
        try {
            if (channel != null && !channel.isShutdown()) {
                log.info("Закрытие OrchestratorGrpcClient канала к '{}'...", targetAddress);
                channel.shutdown();
                try {
                    if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    channel.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            channelLock.unlock();
        }
    }
}
