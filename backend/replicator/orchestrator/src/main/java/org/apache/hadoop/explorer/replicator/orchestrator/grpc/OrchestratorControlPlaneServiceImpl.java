package org.apache.hadoop.explorer.replicator.orchestrator.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.generated.control.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.generated.control.AgentHeartbeatResponse;
import org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterResponse;
import org.apache.hadoop.explorer.replicator.generated.control.AgentUnregisterRequest;
import org.apache.hadoop.explorer.replicator.generated.control.AgentUnregisterResponse;
import org.apache.hadoop.explorer.replicator.generated.control.BatchCreateTasksRequest;
import org.apache.hadoop.explorer.replicator.generated.control.BatchCreateTasksResponse;
import org.apache.hadoop.explorer.replicator.generated.control.ClaimTasksRequest;
import org.apache.hadoop.explorer.replicator.generated.control.ClaimTasksResponse;
import org.apache.hadoop.explorer.replicator.generated.control.ClusterItemProto;
import org.apache.hadoop.explorer.replicator.generated.control.CompleteTaskRequest;
import org.apache.hadoop.explorer.replicator.generated.control.CompleteTaskResponse;
import org.apache.hadoop.explorer.replicator.generated.control.CreateHdfsSubjobRequest;
import org.apache.hadoop.explorer.replicator.generated.control.CreateHdfsSubjobResponse;
import org.apache.hadoop.explorer.replicator.generated.control.FailTaskRequest;
import org.apache.hadoop.explorer.replicator.generated.control.FailTaskResponse;
import org.apache.hadoop.explorer.replicator.generated.control.GetClustersRequest;
import org.apache.hadoop.explorer.replicator.generated.control.GetClustersResponse;
import org.apache.hadoop.explorer.replicator.generated.control.GetJobsRequest;
import org.apache.hadoop.explorer.replicator.generated.control.GetJobsResponse;
import org.apache.hadoop.explorer.replicator.generated.control.GetPendingHmsJobsRequest;
import org.apache.hadoop.explorer.replicator.generated.control.GetPendingHmsJobsResponse;
import org.apache.hadoop.explorer.replicator.generated.control.HmsPendingJobItemProto;
import org.apache.hadoop.explorer.replicator.generated.control.JobItemProto;
import org.apache.hadoop.explorer.replicator.generated.control.OrchestratorControlPlaneServiceGrpc;
import org.apache.hadoop.explorer.replicator.generated.control.ReportHmsProgressRequest;
import org.apache.hadoop.explorer.replicator.generated.control.ReportHmsProgressResponse;
import org.apache.hadoop.explorer.replicator.generated.control.StreamingLeaseRenewRequest;
import org.apache.hadoop.explorer.replicator.generated.control.StreamingLeaseRenewResponse;
import org.apache.hadoop.explorer.replicator.generated.control.TaskCreateItemProto;
import org.apache.hadoop.explorer.replicator.generated.control.TaskItemProto;
import org.apache.hadoop.explorer.replicator.generated.control.TokenRequest;
import org.apache.hadoop.explorer.replicator.generated.control.TokenResponse;
import org.apache.hadoop.explorer.replicator.generated.control.UpdateJobProgressRequest;
import org.apache.hadoop.explorer.replicator.generated.control.UpdateJobProgressResponse;
import org.apache.hadoop.explorer.replicator.model.ClusterDto;
import org.apache.hadoop.explorer.replicator.model.HmsEventReportDto;
import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
import org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest;
import org.apache.hadoop.explorer.replicator.model.TaskCreateItem;
import org.apache.hadoop.explorer.replicator.model.TaskItemDto;
import org.apache.hadoop.explorer.replicator.model.UpdateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.apache.hadoop.explorer.replicator.orchestrator.service.StreamingLeaseCoordinator;
import org.apache.hadoop.explorer.replicator.orchestrator.service.TaskService;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.generated.control.GetActiveKafkaJobsRequest;
import org.apache.hadoop.explorer.replicator.generated.control.GetActiveKafkaJobsResponse;
import org.apache.hadoop.explorer.replicator.generated.control.KafkaJobItemProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Высокопроизводительная реализация gRPC Control Plane сервиса Оркестратора.
 *
 * <p>Обрабатывает все служебные вызовы агентов (регистрация, heartbeat, распределенный пул задач,
 * сетевой шейпинг Token Bucket, лизинг Inotify и координация HMS) по бинарному протоколу Protobuf (HTTP/2).
 */
@Service
public class OrchestratorControlPlaneServiceImpl extends OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorControlPlaneServiceImpl.class);

    private final AgentRegistry agentRegistry;
    private final TaskService taskService;
    private final TokenBucketThrottler throttler;
    private final StreamingLeaseCoordinator streamingLeaseCoordinator;
    private final JobService jobService;
    private final TopologyRegistry topologyRegistry;
    private final HmsCoordinatorService hmsCoordinatorService;
    private final KafkaReplicationOrchestratorService kafkaOrchestratorService;

    @Autowired
    public OrchestratorControlPlaneServiceImpl(
            AgentRegistry agentRegistry,
            TaskService taskService,
            TokenBucketThrottler throttler,
            StreamingLeaseCoordinator streamingLeaseCoordinator,
            JobService jobService,
            TopologyRegistry topologyRegistry,
            @Autowired(required = false) HmsCoordinatorService hmsCoordinatorService,
            @Autowired(required = false) KafkaReplicationOrchestratorService kafkaOrchestratorService
    ) {
        this.agentRegistry = agentRegistry;
        this.taskService = taskService;
        this.throttler = throttler;
        this.streamingLeaseCoordinator = streamingLeaseCoordinator;
        this.jobService = jobService;
        this.topologyRegistry = topologyRegistry;
        this.hmsCoordinatorService = hmsCoordinatorService;
        this.kafkaOrchestratorService = kafkaOrchestratorService;
    }

    @Override
    public void registerAgent(AgentRegisterRequest request, StreamObserver<AgentRegisterResponse> responseObserver) {
        try {
            org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest domainReq =
                    new org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest(
                            request.getAgentId(),
                            request.getClusterId(),
                            request.getMode(),
                            request.getAdvertisedGrpcAddress(),
                            request.getSourceAddress(),
                            request.getMaxBandwidthMbS() > 0 ? request.getMaxBandwidthMbS() : null
                    );

            String secret = AgentGrpcAuthInterceptor.getAuthenticatedSecret();
            boolean ok = agentRegistry.register(domainReq, secret);
            AgentRegisterResponse resp = AgentRegisterResponse.newBuilder()
                    .setStatus(ok ? "registered" : "failed")
                    .setAgentId(request.getAgentId())
                    .setSuccess(ok)
                    .setMessage(ok ? "Агент успешно зарегистрирован" : "Ошибка регистрации")
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC регистрации агента '{}': {}", request.getAgentId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void heartbeat(AgentHeartbeatRequest request, StreamObserver<AgentHeartbeatResponse> responseObserver) {
        try {
            org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest domainReq =
                    new org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest(
                            request.getAgentId(),
                            request.getClusterId(),
                            request.getAdvertisedGrpcAddress(),
                            request.getActiveTransfers(),
                            request.getMode()
                    );
            domainReq.setTransferredBytes(request.getTransferredBytes());
            domainReq.setTransferErrors(request.getTransferErrors());
            if (request.getBandwidthLimitMbS() > 0) {
                domainReq.setBandwidthLimitMbS(request.getBandwidthLimitMbS());
            }
            domainReq.setCompressionRawBytes(request.getCompressionRawBytes());
            domainReq.setCompressionCompressedBytes(request.getCompressionCompressedBytes());
            domainReq.setCompressionRatio(request.getCompressionRatio());
            domainReq.setChunkLatencyAvgMs(request.getChunkLatencyAvgMs());
            domainReq.setChunkLatencyMaxMs(request.getChunkLatencyMaxMs());
            domainReq.setChunkTransferCount(request.getChunkTransferCount());
            domainReq.setStagingActiveFiles(request.getStagingActiveFiles());
            domainReq.setStagingActiveBytes(request.getStagingActiveBytes());
            domainReq.setStagingOrphansCleaned(request.getStagingOrphansCleaned());
            domainReq.setKafkaMessagesReplicated(request.getKafkaMessagesReplicated());
            domainReq.setKafkaBytesReplicated(request.getKafkaBytesReplicated());
            domainReq.setKafkaCurrentLagMs(request.getKafkaCurrentLagMs());

            String secret = AgentGrpcAuthInterceptor.getAuthenticatedSecret();
            boolean ok = agentRegistry.heartbeat(domainReq, secret);

            boolean isKafkaAgent = (request.getMode() != null &&
                    (request.getMode().equalsIgnoreCase("consumer") ||
                     request.getMode().equalsIgnoreCase("producer") ||
                     request.getMode().equalsIgnoreCase("dual") ||
                     request.getMode().toLowerCase().contains("kafka"))) ||
                    request.getKafkaMessagesReplicated() > 0 ||
                    request.getKafkaBytesReplicated() > 0;

            if (ok && kafkaOrchestratorService != null && isKafkaAgent) {
                try {
                    kafkaOrchestratorService.updateMetricsFromHeartbeat(
                            request.getClusterId(),
                            request.getAgentId(),
                            request.getKafkaMessagesReplicated(),
                            request.getKafkaBytesReplicated(),
                            request.getKafkaCurrentLagMs()
                    );
                } catch (Exception ex) {
                    log.warn("Не удалось обновить метрики Kafka из heartbeat агента {}: {}", request.getAgentId(), ex.getMessage());
                }
            }
            AgentHeartbeatResponse resp = AgentHeartbeatResponse.newBuilder()
                    .setStatus(ok ? "acknowledged" : "unknown_agent")
                    .setAcknowledged(ok)
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC heartbeat агента '{}': {}", request.getAgentId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void unregisterAgent(AgentUnregisterRequest request, StreamObserver<AgentUnregisterResponse> responseObserver) {
        try {
            String secret = AgentGrpcAuthInterceptor.getAuthenticatedSecret();
            boolean ok = agentRegistry.unregister(request.getAgentId(), secret);
            if (ok) {
                taskService.failoverTasksForAgent(request.getAgentId(), "Агент дерегистрирован (Graceful stop via gRPC)");
            }
            AgentUnregisterResponse resp = AgentUnregisterResponse.newBuilder()
                    .setStatus(ok ? "unregistered" : "not_found")
                    .setSuccess(ok)
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC unregister агента '{}': {}", request.getAgentId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void requestTokens(TokenRequest request, StreamObserver<TokenResponse> responseObserver) {
        try {
            double waitSec = throttler.requestTokens(
                    request.getRequestedBytes(),
                    request.getSourceClusterId(),
                    request.getTargetClusterId()
            );

            TokenResponse resp = TokenResponse.newBuilder()
                    .setWaitSeconds(waitSec)
                    .setGranted(waitSec <= 0.0)
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC запроса токенов: {}", e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void batchCreateTasks(BatchCreateTasksRequest request, StreamObserver<BatchCreateTasksResponse> responseObserver) {
        try {
            List<TaskCreateItem> items = new ArrayList<>(request.getTasksCount());
            for (TaskCreateItemProto protoItem : request.getTasksList()) {
                items.add(new TaskCreateItem(
                        java.util.UUID.randomUUID().toString(),
                        protoItem.getSourcePath(),
                        protoItem.getTargetPath(),
                        protoItem.getFileSize(),
                        false,
                        3,
                        protoItem.getTaskType().isBlank() ? "FILE" : protoItem.getTaskType(),
                        1,
                        protoItem.getManifestJson()
                ));
            }

            Long epoch = request.getEpoch() > 0 ? request.getEpoch() : null;
            Long lastTxid = request.getLastProcessedTxid() > 0 ? request.getLastProcessedTxid() : null;
            org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest domainReq =
                    new org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest(request.getJobId(), items, epoch, lastTxid);
            boolean ok = taskService.batchCreateTasks(request.getJobId(), domainReq);

            boolean fenced = false;
            String errorMsg = "";
            if (!ok) {
                fenced = taskService.isEpochFenced(request.getJobId(), epoch);
                errorMsg = fenced ? "Стример отгорожен (Epoch fencing: более свежая аренда)" : "Задание не найдено или отклонено";
            }

            long queuedCount = taskService.getQueuedTasksCount(request.getJobId());
            boolean saturated = taskService.isQueueSaturated(request.getJobId(), null);

            BatchCreateTasksResponse resp = BatchCreateTasksResponse.newBuilder()
                    .setSuccess(ok)
                    .setJobId(request.getJobId())
                    .setTasksCount(items.size())
                    .setQueuedCount(queuedCount)
                    .setSaturated(saturated)
                    .setFenced(fenced)
                    .setError(errorMsg)
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC пакетного создания задач для job '{}': {}", request.getJobId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void claimTasks(ClaimTasksRequest request, StreamObserver<ClaimTasksResponse> responseObserver) {
        try {
            org.apache.hadoop.explorer.replicator.model.ClaimTasksRequest domainReq =
                    new org.apache.hadoop.explorer.replicator.model.ClaimTasksRequest(
                            request.getWorkerId(),
                            request.getClusterId(),
                            request.getBatchSize() > 0 ? request.getBatchSize() : 10
                    );

            List<TaskItemDto> claimed = taskService.claimTasks(domainReq);
            ClaimTasksResponse.Builder builder = ClaimTasksResponse.newBuilder();

            for (TaskItemDto item : claimed) {
                builder.addTasks(TaskItemProto.newBuilder()
                        .setTaskId(item.getId() != null ? item.getId() : "")
                        .setJobId(item.getJobId() != null ? item.getJobId() : "")
                        .setSourcePath(item.getSourcePath() != null ? item.getSourcePath() : "")
                        .setTargetPath(item.getTargetPath() != null ? item.getTargetPath() : "")
                        .setFileSize(item.getFileSize())
                        .setStatus(item.getStatus() != null ? item.getStatus() : "")
                        .setRetryCount(item.getRetryCount())
                        .setSourceClusterId(item.getSourceClusterId() != null ? item.getSourceClusterId() : "")
                        .setTargetClusterId(item.getTargetClusterId() != null ? item.getTargetClusterId() : "")
                        .setTargetGrpcAddress(item.getTargetAddress() != null ? item.getTargetAddress() : "")
                        .setTaskType(item.getTaskType() != null ? item.getTaskType() : "FILE")
                        .setManifestJson(item.getBundleManifest() != null ? item.getBundleManifest() : "")
                        .setRunAsServiceAccount(Boolean.TRUE.equals(item.getRunAsServiceAccount()))
                        .setExecutionPrincipal(item.getExecutionPrincipal() != null ? item.getExecutionPrincipal() : "")
                        .setTaskToken(item.getTaskToken() != null ? item.getTaskToken() : "")
                        .build());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC забора задач воркером '{}': {}", request.getWorkerId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void completeTask(CompleteTaskRequest request, StreamObserver<CompleteTaskResponse> responseObserver) {
        try {
            org.apache.hadoop.explorer.replicator.model.CompleteTaskRequest domainReq =
                    new org.apache.hadoop.explorer.replicator.model.CompleteTaskRequest(
                            request.getTaskId(),
                            request.getWorkerId(),
                            request.getBytesWritten(),
                            request.getMessage(),
                            request.getCommittedFiles()
                    );

            boolean ok = taskService.completeTask(domainReq);
            CompleteTaskResponse resp = CompleteTaskResponse.newBuilder()
                    .setSuccess(ok)
                    .setMessage(ok ? "OK" : "Task not found or update error")
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC завершения задачи '{}': {}", request.getTaskId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void failTask(FailTaskRequest request, StreamObserver<FailTaskResponse> responseObserver) {
        try {
            org.apache.hadoop.explorer.replicator.model.FailTaskRequest domainReq =
                    new org.apache.hadoop.explorer.replicator.model.FailTaskRequest(
                            request.getTaskId(),
                            request.getWorkerId(),
                            request.getErrorMessage()
                    );

            boolean ok = taskService.failTask(domainReq);
            FailTaskResponse resp = FailTaskResponse.newBuilder()
                    .setSuccess(ok)
                    .setMessage(ok ? "Failed recorded" : "Task not found")
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC сбоя задачи '{}': {}", request.getTaskId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void updateJobProgress(UpdateJobProgressRequest request, StreamObserver<UpdateJobProgressResponse> responseObserver) {
        try {
            UpdateJobRequest domainReq = new UpdateJobRequest(
                    request.getStatus(),
                    request.getCurrentBytes(),
                    request.getTotalBytes(),
                    request.getMessage()
            );

            var opt = jobService.updateJobProgress(request.getJobId(), domainReq);
            boolean ok = opt.isPresent();

            UpdateJobProgressResponse resp = UpdateJobProgressResponse.newBuilder()
                    .setSuccess(ok)
                    .setMessage(ok ? "Progress updated" : "Job not found")
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC обновления прогресса job '{}': {}", request.getJobId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getJobs(GetJobsRequest request, StreamObserver<GetJobsResponse> responseObserver) {
        try {
            List<JobResponse> jobs = jobService.listJobs(null, null, true, false, request.getIncludeSubjobs());
            GetJobsResponse.Builder builder = GetJobsResponse.newBuilder();

            for (JobResponse job : jobs) {
                if (request.getClusterId() != null && !request.getClusterId().isBlank()) {
                    if (job.sourceClusterId() != null && !job.sourceClusterId().equalsIgnoreCase(request.getClusterId())) {
                        continue;
                    }
                }

                builder.addJobs(JobItemProto.newBuilder()
                        .setId(job.id() != null ? job.id() : "")
                        .setSourceClusterId(job.sourceClusterId() != null ? job.sourceClusterId() : "")
                        .setTargetClusterId(job.targetClusterId() != null ? job.targetClusterId() : "")
                        .setSourcePath(job.sourcePath() != null ? job.sourcePath() : "")
                        .setTargetPath(job.targetPath() != null ? job.targetPath() : "")
                        .setStatus(job.status() != null ? job.status() : "")
                        .setTotalBytes(job.totalBytes())
                        .setProcessedBytes(job.copiedBytes())
                        .setMessage(job.message() != null ? job.message() : "")
                        .setSyncMode(job.syncMode() != null ? job.syncMode() : "MANUAL")
                        .setCronExpression(job.cronExpression() != null ? job.cronExpression() : "")
                        .setExecutionPrincipal(job.executionPrincipal() != null ? job.executionPrincipal() : "")
                        .setRunAsServiceAccount(job.runAsServiceAccount())
                        .setJobType(job.jobType() != null ? job.jobType() : "HDFS")
                        .setParentJobId(job.parentJobId() != null ? job.parentJobId() : "")
                        .build());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC getJobs: {}", e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getClusters(GetClustersRequest request, StreamObserver<GetClustersResponse> responseObserver) {
        try {
            List<ClusterDto> clusters = topologyRegistry.getClusters();
            GetClustersResponse.Builder builder = GetClustersResponse.newBuilder();

            for (ClusterDto cl : clusters) {
                builder.addClusters(ClusterItemProto.newBuilder()
                        .setId(cl.getId() != null ? cl.getId() : "")
                        .setName(cl.getName() != null ? cl.getName() : "")
                        .setDcId(cl.getDcId() != null ? cl.getDcId() : "")
                        .setGrpcAddress(cl.getGrpcAddress() != null ? cl.getGrpcAddress() : "")
                        .build());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC getClusters: {}", e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void renewStreamingLease(StreamingLeaseRenewRequest request, StreamObserver<StreamingLeaseRenewResponse> responseObserver) {
        try {
            org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewRequest domainReq =
                    new org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewRequest(
                            request.getClusterId(),
                            request.getStreamerId()
                    );

            org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewResponse domainResp =
                    streamingLeaseCoordinator.renewLease(domainReq);

            boolean granted = domainResp != null && "ACTIVE".equalsIgnoreCase(domainResp.status());
            StreamingLeaseRenewResponse resp = StreamingLeaseRenewResponse.newBuilder()
                    .setGranted(granted)
                    .setLeaderId(domainResp != null && domainResp.activeAgentId() != null ? domainResp.activeAgentId() : "")
                    .setLeaderAddress("")
                    .setLeaseDurationMs(10000L)
                    .setClusterId(request.getClusterId() != null ? request.getClusterId() : "")
                    .setMaxActiveTxid(domainResp != null ? domainResp.lastCommittedTxid() : 0L)
                    .setQueueSaturated(domainResp != null && domainResp.queueSaturated())
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC renewStreamingLease для cluster '{}': {}", request.getClusterId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getPendingHmsJobs(GetPendingHmsJobsRequest request, StreamObserver<GetPendingHmsJobsResponse> responseObserver) {
        try {
            GetPendingHmsJobsResponse.Builder builder = GetPendingHmsJobsResponse.newBuilder();
            if (hmsCoordinatorService != null) {
                List<HmsPendingJobDto> pendingList = hmsCoordinatorService.getPendingJobsForCluster(request.getClusterId(), request.getAgentId());
                for (HmsPendingJobDto job : pendingList) {
                    builder.addJobs(HmsPendingJobItemProto.newBuilder()
                            .setJobId(job.id() != null ? job.id() : "")
                            .setSourceCluster(job.sourceClusterId() != null ? job.sourceClusterId() : "")
                            .setTargetCluster(job.targetClusterId() != null ? job.targetClusterId() : "")
                            .setTargetAgentGrpcAddress(job.targetAgentGrpcAddress() != null ? job.targetAgentGrpcAddress() : "")
                            .setSourceDb(job.sourceDbName() != null ? job.sourceDbName() : "")
                            .setTargetDb(job.targetDbName() != null ? job.targetDbName() : "")
                            .setTablePattern(job.tableIncludePattern() != null ? job.tableIncludePattern() : "")
                            .setStatus(job.status() != null ? job.status() : "")
                            .setSyncData(true)
                            .setExecutionPrincipal(job.executionPrincipal() != null ? job.executionPrincipal() : "")
                            .setLastProcessedEventId(job.lastProcessedEventId() != null ? job.lastProcessedEventId() : 0L)
                            .setDropExtraneousTables(job.dropExtraneousTables())
                            .setDropExtraneousPartitions(job.dropExtraneousPartitions())
                            .setAssignedAgentId(job.assignedAgentId() != null ? job.assignedAgentId() : "")
                            .setEnableHdfsStreaming(job.enableHdfsStreaming())
                            .setCompanionHdfsJobId(job.companionHdfsJobId() != null ? job.companionHdfsJobId() : "")
                            .setDbWarehouseLocation(job.dbWarehouseLocation() != null ? job.dbWarehouseLocation() : "")
                            .build());
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC getPendingHmsJobs: {}", e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void reportHmsProgress(ReportHmsProgressRequest request, StreamObserver<ReportHmsProgressResponse> responseObserver) {
        try {
            if (hmsCoordinatorService != null) {
                List<HmsEventReportDto> domainEvents = null;
                if (request.getEventsCount() > 0) {
                    domainEvents = new ArrayList<>(request.getEventsCount());
                    for (var ev : request.getEventsList()) {
                        domainEvents.add(new HmsEventReportDto(
                                ev.getEventId(),
                                ev.getEventType(),
                                ev.getTableName(),
                                ev.getPartitionName().isEmpty() ? null : ev.getPartitionName(),
                                ev.getSourceUri().isEmpty() ? null : ev.getSourceUri(),
                                ev.getTargetUri().isEmpty() ? null : ev.getTargetUri(),
                                ev.getSubjobId().isEmpty() ? null : ev.getSubjobId(),
                                ev.getStatus(),
                                ev.getErrorMessage().isEmpty() ? null : ev.getErrorMessage()
                        ));
                    }
                }

                HmsProgressReportRequest domainReq = new HmsProgressReportRequest(
                        request.getStatus(),
                        request.getTotalTables(),
                        request.getTablesSynced(),
                        request.getTotalPartitions(),
                        request.getPartitionsSynced(),
                        request.getLastEventId(),
                        request.getBootstrapEventId() != 0L ? request.getBootstrapEventId() : null,
                        request.getEventLag(),
                        request.getMessage(),
                        domainEvents,
                        request.getAgentId().isEmpty() ? null : request.getAgentId()
                );
                hmsCoordinatorService.updateJobProgress(request.getJobId(), domainReq);
            }

            responseObserver.onNext(ReportHmsProgressResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC reportHmsProgress для job '{}': {}", request.getJobId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void createHdfsSubjob(CreateHdfsSubjobRequest request, StreamObserver<CreateHdfsSubjobResponse> responseObserver) {
        try {
            String syncMode = (request.getSyncMode() != null && !request.getSyncMode().isBlank()) ? request.getSyncMode() : null;
            String jobType = (request.getJobType() != null && !request.getJobType().isBlank()) ? request.getJobType() : "HMS_SUBJOB";

            CreateJobRequest domainReq = new CreateJobRequest(
                    request.getSourcePath(),
                    request.getTargetPath(),
                    request.getSourceCluster() != null && !request.getSourceCluster().isBlank() ? request.getSourceCluster() : "default",
                    request.getTargetCluster() != null && !request.getTargetCluster().isBlank() ? request.getTargetCluster() : "default",
                    0L,
                    request.getExecutionPrincipal() != null && !request.getExecutionPrincipal().isBlank() ? request.getExecutionPrincipal() : "hdfs@EXAMPLE.COM",
                    true,
                    false,
                    null,
                    7,
                    jobType,
                    request.getParentJobId(),
                    syncMode
            );

            // Дедупликация: повторно используем активную или недавно завершенную подзадачу при ретраях CDC
            JobResponse subjob = jobService.findReusableSubjob(request.getParentJobId(), request.getSourcePath(), request.getTargetPath())
                    .orElseGet(() -> jobService.createJob(domainReq, "system_agent"));

            if (subjob != null && "HMS_COMPANION_STREAMING".equalsIgnoreCase(jobType) && hmsCoordinatorService != null) {
                hmsCoordinatorService.linkCompanionHdfsJob(request.getParentJobId(), subjob.id(), request.getSourcePath());
            }

            CreateHdfsSubjobResponse resp = CreateHdfsSubjobResponse.newBuilder()
                    .setJobId(subjob != null ? subjob.id() : "")
                    .setSuccess(subjob != null)
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC createHdfsSubjob для parent '{}': {}", request.getParentJobId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getActiveKafkaJobs(GetActiveKafkaJobsRequest request, StreamObserver<GetActiveKafkaJobsResponse> responseObserver) {
        try {
            if (kafkaOrchestratorService == null) {
                responseObserver.onNext(GetActiveKafkaJobsResponse.newBuilder().build());
                responseObserver.onCompleted();
                return;
            }

            List<KafkaReplicationJobDto> jobs = kafkaOrchestratorService.getActiveJobsForDc(request.getDcId());
            GetActiveKafkaJobsResponse.Builder respBuilder = GetActiveKafkaJobsResponse.newBuilder();

            for (KafkaReplicationJobDto job : jobs) {
                List<String> topics = job.getTopics() != null
                        ? job.getTopics().stream()
                                .map(CreateKafkaJobRequest.TopicSetting::getTopic)
                                .filter(Objects::nonNull)
                                .toList()
                        : Collections.emptyList();

                List<String> targetEndpoints = job.getTargetEndpoints();
                if ((targetEndpoints == null || targetEndpoints.isEmpty()) && job.getTargetDc() != null) {
                    targetEndpoints = kafkaOrchestratorService.getLiveReceiverEndpoints(job.getTargetDc());
                }

                KafkaJobItemProto item = KafkaJobItemProto.newBuilder()
                        .setJobId(job.getId() != null ? job.getId() : "")
                        .setName(job.getName() != null ? job.getName() : "")
                        .setSourceDc(job.getSourceDc() != null ? job.getSourceDc() : "")
                        .setTargetDc(job.getTargetDc() != null ? job.getTargetDc() : "")
                        .setDirection(job.getDirection() != null ? job.getDirection() : "")
                        .addAllTopics(topics)
                        .addAllConsumerGroups(job.getConsumerGroups() != null ? job.getConsumerGroups() : Collections.emptyList())
                        .setSourceBootstrapServers(job.getSourceBootstrapServers() != null ? job.getSourceBootstrapServers() : "")
                        .setTargetBootstrapServers(job.getTargetBootstrapServers() != null ? job.getTargetBootstrapServers() : "")
                        .addAllTargetEndpoints(targetEndpoints != null ? targetEndpoints : Collections.emptyList())
                        .setBatchSize(job.getBatchSize() != null ? job.getBatchSize() : 2000)
                        .setConsumerThreads(job.getConsumerThreads() != null ? job.getConsumerThreads() : 8)
                        .setBandwidthLimitMb(job.getBandwidthLimitMb() != null ? job.getBandwidthLimitMb() : 0.0)
                        .setStatus(job.getStatus() != null ? job.getStatus() : "")
                        .build();

                respBuilder.addJobs(item);
            }

            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка gRPC getActiveKafkaJobs для DC '{}': {}", request.getDcId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
