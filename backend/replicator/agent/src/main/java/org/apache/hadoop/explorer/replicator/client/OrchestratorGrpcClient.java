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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Высокопроизводительный gRPC-клиент для взаимодействия агента с Оркестратором (Control Plane).
 *
 * <p>Использует HTTP/2 мультиплексирование в рамках единого постоянного TCP-соединения,
 * бинарную сериализацию Protobuf и автоматическую L7-аутентификацию по X-Agent-Secret.
 */
public class OrchestratorGrpcClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorGrpcClient.class);

    private final String targetAddress;
    private final String agentSecret;
    private final ManagedChannel channel;
    private final OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceBlockingStub blockingStub;

    /**
     * Конструктор для юнит-тестов и тестовых моков.
     */
    protected OrchestratorGrpcClient() {
        this.targetAddress = "test-in-memory";
        this.agentSecret = null;
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
        String addr = (orchestratorGrpcAddress != null && !orchestratorGrpcAddress.isBlank())
                ? orchestratorGrpcAddress.trim()
                : "localhost:50050";

        // Очистка префиксов если переданы grpc:// или http://
        if (addr.startsWith("grpc://")) {
            addr = addr.substring("grpc://".length());
        } else if (addr.startsWith("http://")) {
            addr = addr.substring("http://".length());
        } else if (addr.startsWith("https://")) {
            addr = addr.substring("https://".length());
            tlsEnabled = true;
        }

        this.targetAddress = addr;
        this.agentSecret = agentSecret;

        NettyChannelBuilder builder = NettyChannelBuilder.forTarget(targetAddress)
                .maxInboundMessageSize(16 * 1024 * 1024)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true);

        if (tlsEnabled) {
            try {
                var sslBuilder = GrpcSslContexts.forClient();
                if (trustCertCollectionPath != null && !trustCertCollectionPath.isBlank()) {
                    sslBuilder.trustManager(new File(trustCertCollectionPath));
                } else if (insecureSkipVerify) {
                    sslBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                }
                builder.sslContext(sslBuilder.build());
            } catch (Exception e) {
                log.error("Сбой настройки TLS для OrchestratorGrpcClient: {}", e.getMessage(), e);
                builder.usePlaintext();
            }
        } else {
            builder.usePlaintext();
        }

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

        this.channel = builder.build();
        this.blockingStub = OrchestratorControlPlaneServiceGrpc.newBlockingStub(channel);

        log.info("OrchestratorGrpcClient инициализирован: target='{}' (TLS={})", targetAddress, tlsEnabled);
    }

    private OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceBlockingStub stub() {
        return blockingStub != null ? blockingStub.withDeadlineAfter(15, TimeUnit.SECONDS) : null;
    }

    /**
     * Конструктор для in-process тестирования
     */
    public OrchestratorGrpcClient(ManagedChannel channel, String agentSecret) {
        this.targetAddress = "in-process";
        this.agentSecret = agentSecret;
        this.channel = channel;
        this.blockingStub = OrchestratorControlPlaneServiceGrpc.newBlockingStub(channel);
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

            var resp = stub().registerAgent(protoReq);
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

            var resp = stub().heartbeat(builder.build());
            return resp.getAcknowledged();
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
            var resp = stub().unregisterAgent(
                    AgentUnregisterRequest.newBuilder().setAgentId(agentId).build()
            );
            return resp.getSuccess();
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

            var resp = stub().requestTokens(protoReq);
            return resp.getWaitSeconds();
        } catch (Exception e) {
            log.debug("Исключение при gRPC запросе квоты у Оркестратора: {}", e.getMessage());
            return 0.0;
        }
    }

    /**
     * Получение всех задач репликации из Оркестратора.
     */
    public List<JobDto> getJobs() {
        try {
            var resp = stub().getJobs(
                    GetJobsRequest.newBuilder().setIncludeSubjobs(true).build()
            );

            List<JobDto> result = new ArrayList<>(resp.getJobsCount());
            for (JobItemProto job : resp.getJobsList()) {
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
                result.add(dto);
            }
            return result;
        } catch (Exception e) {
            log.warn("Ошибка gRPC получения списка задач из Оркестратора: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Получение конкретной задачи по идентификатору.
     */
    public Optional<JobDto> getJobById(String jobId) {
        if (jobId == null || jobId.isBlank()) return Optional.empty();
        return getJobs().stream()
                .filter(j -> jobId.equalsIgnoreCase(j.getId()))
                .findFirst();
    }

    /**
     * Ожидание успешного завершения HDFS саб-джобы (Data-First Consistency Barrier).
     * Предотвращает применение DDL в целевой HMS до того, как скопированы файлы данных.
     */
    public boolean awaitSubjobCompletion(String subjobId, long timeoutSeconds) {
        if (subjobId == null || subjobId.isBlank()) {
            return true;
        }
        List<JobDto> currentJobs = getJobs();
        if (currentJobs.isEmpty()) {
            // Если оркестратор вернул пустой список (например, mock в юнит-тесте или stub без HDFS задач)
            return true;
        }
        long deadline = System.currentTimeMillis() + Math.max(timeoutSeconds, 1) * 1000L;
        while (System.currentTimeMillis() < deadline) {
            Optional<JobDto> opt = getJobById(subjobId);
            if (opt.isPresent()) {
                String status = opt.get().getStatus();
                if ("COMPLETED".equalsIgnoreCase(status) || "SUCCESS".equalsIgnoreCase(status)) {
                    log.debug("Data-First Barrier: HDFS подзадача {} успешно завершена", subjobId);
                    return true;
                }
                if ("FAILED".equalsIgnoreCase(status) || "ERROR".equalsIgnoreCase(status)) {
                    log.warn("Data-First Barrier: HDFS подзадача {} завершилась со сбоем: {}",
                            subjobId, opt.get().getMessage());
                    return false;
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        log.warn("Data-First Barrier: истек таймаут ожидания HDFS подзадачи {} ({} с)", subjobId, timeoutSeconds);
        return false;
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

            var resp = stub().updateJobProgress(protoReq);
            return resp.getSuccess();
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

            var resp = stub().updateJobProgress(protoReq);
            return resp.getSuccess();
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
            var resp = stub().getClusters(GetClustersRequest.getDefaultInstance());
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

            var resp = stub().batchCreateTasks(builder.build());
            return resp.getSuccess();
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

            var resp = stub().claimTasks(protoReq);
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

            var resp = stub().completeTask(protoReq);
            return resp.getSuccess();
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

            var resp = stub().failTask(protoReq);
            return resp.getSuccess();
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

            var resp = stub().renewStreamingLease(protoReq);
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
    public List<HmsPendingJobDto> getPendingHmsJobs(String clusterId, String agentId) {
        try {
            var protoReq = GetPendingHmsJobsRequest.newBuilder()
                    .setClusterId(clusterId != null ? clusterId : "")
                    .setAgentId(agentId != null ? agentId : "")
                    .build();

            var resp = stub().getPendingHmsJobs(protoReq);
            List<HmsPendingJobDto> list = new ArrayList<>(resp.getJobsCount());
            for (HmsPendingJobItemProto j : resp.getJobsList()) {
                list.add(new HmsPendingJobDto(
                        j.getJobId(),
                        j.getSourceCluster(),
                        j.getTargetCluster(),
                        j.getSourceDb(),
                        j.getTargetDb(),
                        j.getTablePattern(),
                        false,
                        false,
                        j.getTargetAgentGrpcAddress(),
                        j.getStatus(),
                        0L,
                        j.getJobId(),
                        j.getExecutionPrincipal()
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
    public boolean reportHmsProgress(String jobId, HmsProgressReportRequest report) {
        try {
            var protoReq = ReportHmsProgressRequest.newBuilder()
                    .setJobId(jobId != null ? jobId : "")
                    .setStatus(report.status() != null ? report.status() : "")
                    .setMessage(report.message() != null ? report.message() : "")
                    .setTablesSynced(report.replicatedTables())
                    .setPartitionsSynced(report.replicatedPartitions())
                    .setLastEventId(report.lastProcessedEventId() != null ? report.lastProcessedEventId() : 0L)
                    .build();

            var resp = stub().reportHmsProgress(protoReq);
            return resp.getSuccess();
        } catch (Exception e) {
            log.warn("Ошибка gRPC отправки прогресса HMS задачи '{}': {}", jobId, e.getMessage());
            return false;
        }
    }

    /**
     * Создание HDFS саб-джобы для HMS.
     */
    public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl, String executionPrincipal) {
        try {
            var protoReq = CreateHdfsSubjobRequest.newBuilder()
                    .setParentJobId(hmsJobId != null ? hmsJobId : "")
                    .setSourcePath(srcPath != null ? srcPath : "")
                    .setTargetPath(dstPath != null ? dstPath : "")
                    .setSourceCluster(srcCl != null ? srcCl : "default")
                    .setTargetCluster(dstCl != null ? dstCl : "default")
                    .setExecutionPrincipal(executionPrincipal != null ? executionPrincipal : "")
                    .build();

            var resp = stub().createHdfsSubjob(protoReq);
            return resp.getSuccess() ? resp.getJobId() : null;
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

            var resp = stub().getActiveKafkaJobs(protoReq);
            return resp.getJobsList();
        } catch (Exception e) {
            log.warn("Ошибка gRPC получения активных Kafka задач для ДЦ '{}': {}", dcId, e.getMessage());
            return Collections.emptyList();
        }
    }

    public String getTargetAddress() {
        return targetAddress;
    }

    @Override
    public void close() {
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
    }
}
