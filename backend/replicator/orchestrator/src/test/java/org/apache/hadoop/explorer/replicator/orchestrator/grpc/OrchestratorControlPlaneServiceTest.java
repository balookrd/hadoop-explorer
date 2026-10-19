package org.apache.hadoop.explorer.replicator.orchestrator.grpc;

import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.explorer.replicator.generated.control.*;
import org.apache.hadoop.explorer.replicator.model.TaskItemDto;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.service.StreamingLeaseCoordinator;
import org.apache.hadoop.explorer.replicator.orchestrator.service.TaskService;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class OrchestratorControlPlaneServiceTest {

    private int port;
    private Server server;
    private ManagedChannel channel;
    private OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceBlockingStub stub;

    private AgentRegistry agentRegistry;
    private TaskService taskService;
    private TokenBucketThrottler throttler;
    private StreamingLeaseCoordinator streamingLeaseCoordinator;
    private JobService jobService;
    private TopologyRegistry topologyRegistry;
    private HmsCoordinatorService hmsCoordinatorService;
    private KafkaReplicationOrchestratorService kafkaOrchestratorService;

    private static final String TEST_SECRET = "super-secret-token-xyz";

    @BeforeEach
    void setUp() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        agentRegistry = mock(AgentRegistry.class);
        taskService = mock(TaskService.class);
        throttler = mock(TokenBucketThrottler.class);
        streamingLeaseCoordinator = mock(StreamingLeaseCoordinator.class);
        jobService = mock(JobService.class);
        topologyRegistry = mock(TopologyRegistry.class);
        hmsCoordinatorService = mock(HmsCoordinatorService.class);
        kafkaOrchestratorService = mock(KafkaReplicationOrchestratorService.class);

        OrchestratorControlPlaneServiceImpl service = new OrchestratorControlPlaneServiceImpl(
                agentRegistry,
                taskService,
                throttler,
                streamingLeaseCoordinator,
                jobService,
                topologyRegistry,
                hmsCoordinatorService,
                kafkaOrchestratorService
        );

        AgentGrpcAuthInterceptor authInterceptor = new AgentGrpcAuthInterceptor(TEST_SECRET);

        server = NettyServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(service, authInterceptor))
                .build()
                .start();

        channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
                .usePlaintext()
                .build();

        // Stub с валидным x-agent-secret по умолчанию
        stub = OrchestratorControlPlaneServiceGrpc.newBlockingStub(channel)
                .withInterceptors(new ClientInterceptor() {
                    @Override
                    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                            MethodDescriptor<ReqT, RespT> method,
                            CallOptions callOptions,
                            Channel next) {
                        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                            @Override
                            public void start(Listener<RespT> responseListener, Metadata headers) {
                                headers.put(Metadata.Key.of("x-agent-secret", Metadata.ASCII_STRING_MARSHALLER), TEST_SECRET);
                                super.start(responseListener, headers);
                            }
                        };
                    }
                });
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("gRPC: Отклонение вызова без корректного x-agent-secret (Status.UNAUTHENTICATED)")
    void testAuthInterceptorRejection() {
        // Создаем stub без заголовка аутентификации
        var unauthenticatedStub = OrchestratorControlPlaneServiceGrpc.newBlockingStub(channel);

        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
                unauthenticatedStub.heartbeat(AgentHeartbeatRequest.newBuilder().setAgentId("agent-1").build())
        );

        assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        assertTrue(ex.getMessage().contains("Invalid or missing X-Agent-Secret"));
    }

    @Test
    @DisplayName("gRPC: Успешная регистрация и heartbeat агента")
    void testAgentRegistrationAndHeartbeat() {
        when(agentRegistry.register(any(), any())).thenReturn(true);
        when(agentRegistry.heartbeat(any(), any())).thenReturn(true);

        AgentRegisterRequest regReq = AgentRegisterRequest.newBuilder()
                .setAgentId("agent-dc1")
                .setClusterId("dc1")
                .setMode("all")
                .setAdvertisedGrpcAddress("agent-dc1:50051")
                .setMaxBandwidthMbS(80.0)
                .build();

        AgentRegisterResponse regResp = stub.registerAgent(regReq);
        assertTrue(regResp.getSuccess());
        assertEquals("registered", regResp.getStatus());

        AgentHeartbeatRequest hbReq = AgentHeartbeatRequest.newBuilder()
                .setAgentId("agent-dc1")
                .setActiveTransfers(2)
                .build();

        AgentHeartbeatResponse hbResp = stub.heartbeat(hbReq);
        assertTrue(hbResp.getAcknowledged());
        assertEquals("acknowledged", hbResp.getStatus());
    }

    @Test
    @DisplayName("gRPC: Запрос токенов полосы пропускания (Hierarchical Token Bucket)")
    void testRequestTokens() {
        when(throttler.requestTokens(1048576L, "dc1", "dc2")).thenReturn(0.0);

        TokenRequest req = TokenRequest.newBuilder()
                .setRequestedBytes(1048576L)
                .setSourceClusterId("dc1")
                .setTargetClusterId("dc2")
                .build();

        TokenResponse resp = stub.requestTokens(req);
        assertEquals(0.0, resp.getWaitSeconds());
        assertTrue(resp.getGranted());
    }

    @Test
    @DisplayName("gRPC: Пакетное создание задач и взятие свободным воркером (Claim)")
    void testBatchCreateAndClaimTasks() {
        when(taskService.batchCreateTasks(eq("job-100"), any())).thenReturn(true);
        when(taskService.getQueuedTasksCount("job-100")).thenReturn(2L);
        when(taskService.isQueueSaturated("job-100", null)).thenReturn(false);

        BatchCreateTasksRequest batchReq = BatchCreateTasksRequest.newBuilder()
                .setJobId("job-100")
                .addTasks(TaskCreateItemProto.newBuilder()
                        .setSourcePath("/data/sales1.csv")
                        .setTargetPath("/backup/sales1.csv")
                        .setFileSize(5000L)
                        .build())
                .addTasks(TaskCreateItemProto.newBuilder()
                        .setSourcePath("/data/sales2.csv")
                        .setTargetPath("/backup/sales2.csv")
                        .setFileSize(7000L)
                        .build())
                .build();

        BatchCreateTasksResponse batchResp = stub.batchCreateTasks(batchReq);
        assertTrue(batchResp.getSuccess());
        assertEquals(2, batchResp.getTasksCount());
        assertEquals(2L, batchResp.getQueuedCount());

        // Мокирование взятия задач
        TaskItemDto item1 = new TaskItemDto(
                "task-1", "job-100", "run-1", "/data/sales1.csv", "/backup/sales1.csv",
                5000L, "ASSIGNED", "worker-1", null, "agent-dc2:50051",
                null, false, 0, 3, null, "FILE", 1, null, "dc1", "dc2"
        );
        when(taskService.claimTasks(any())).thenReturn(List.of(item1));

        ClaimTasksRequest claimReq = ClaimTasksRequest.newBuilder()
                .setWorkerId("worker-1")
                .setClusterId("dc1")
                .setBatchSize(5)
                .build();

        ClaimTasksResponse claimResp = stub.claimTasks(claimReq);
        assertEquals(1, claimResp.getTasksCount());
        assertEquals("task-1", claimResp.getTasks(0).getTaskId());
        assertEquals("/data/sales1.csv", claimResp.getTasks(0).getSourcePath());
    }

    @Test
    @DisplayName("gRPC: Фиксация завершения и сбоя задачи воркером")
    void testCompleteAndFailTask() {
        when(taskService.completeTask(any())).thenReturn(true);
        when(taskService.failTask(any())).thenReturn(true);

        CompleteTaskRequest compReq = CompleteTaskRequest.newBuilder()
                .setTaskId("task-1")
                .setWorkerId("worker-1")
                .setBytesWritten(5000L)
                .build();

        CompleteTaskResponse compResp = stub.completeTask(compReq);
        assertTrue(compResp.getSuccess());

        FailTaskRequest failReq = FailTaskRequest.newBuilder()
                .setTaskId("task-2")
                .setWorkerId("worker-1")
                .setErrorMessage("Checksum mismatch")
                .build();

        FailTaskResponse failResp = stub.failTask(failReq);
        assertTrue(failResp.getSuccess());
    }

    @Test
    @DisplayName("gRPC: Получение списка задач (GetJobs)")
    void testGetJobs() {
        JobEntity entity = new JobEntity();
        entity.setId("job-1");
        entity.setSourceClusterId("dc1");
        entity.setTargetClusterId("dc2");
        entity.setSourcePath("/source/dir");
        entity.setTargetPath("/target/dir");
        entity.setStatus("RUNNING");
        entity.setTotalBytes(1000000L);
        entity.setCopiedBytes(500000L);
        entity.setSyncMode("STREAMING_INOTIFY");

        JobResponse job = JobResponse.fromEntity(entity);
        when(jobService.listJobs(null, null, true, false, true)).thenReturn(List.of(job));

        GetJobsRequest req = GetJobsRequest.newBuilder()
                .setIncludeSubjobs(true)
                .build();

        GetJobsResponse resp = stub.getJobs(req);
        assertEquals(1, resp.getJobsCount());
        assertEquals("job-1", resp.getJobs(0).getId());
        assertEquals("STREAMING_INOTIFY", resp.getJobs(0).getSyncMode());
    }

    @Test
    @DisplayName("gRPC: Успешное обновление прогресса задачи")
    void testUpdateJobProgressSuccess() {
        JobEntity entity = new JobEntity();
        entity.setId("job-1");
        entity.setStatus("RUNNING");
        JobResponse mockResp = JobResponse.fromEntity(entity);
        when(jobService.updateJobProgress(eq("job-1"), any())).thenReturn(Optional.of(mockResp));

        UpdateJobProgressRequest req = UpdateJobProgressRequest.newBuilder()
                .setJobId("job-1")
                .setStatus("RUNNING")
                .setCurrentBytes(500L)
                .setTotalBytes(1000L)
                .setMessage("Progress update")
                .build();

        UpdateJobProgressResponse resp = stub.updateJobProgress(req);
        assertTrue(resp.getSuccess());
        assertEquals("Progress updated", resp.getMessage());
    }

    @Test
    @DisplayName("gRPC: Отклонение обновления при CAS-конфликте захвата задачи в ANALYZING")
    void testUpdateJobProgressCasConflict() {
        when(jobService.updateJobProgress(eq("job-cas"), any())).thenReturn(Optional.empty());

        UpdateJobProgressRequest req = UpdateJobProgressRequest.newBuilder()
                .setJobId("job-cas")
                .setStatus("ANALYZING")
                .setMessage("Захват на анализ")
                .build();

        UpdateJobProgressResponse resp = stub.updateJobProgress(req);
        assertFalse(resp.getSuccess());
        assertEquals("Job not found", resp.getMessage());
    }

    @Test
    @DisplayName("gRPC: Получение активных задач Kafka для дата-центра")
    void testGetActiveKafkaJobs() {
        KafkaReplicationJobDto job = new KafkaReplicationJobDto();
        job.setId("job-kafka-1");
        job.setName("Kafka Sync DC1 -> DC2");
        job.setSourceDc("dc1");
        job.setTargetDc("dc2");
        job.setDirection("PUSH");
        job.setStatus("ACTIVE");
        CreateKafkaJobRequest.TopicSetting ts = new CreateKafkaJobRequest.TopicSetting();
        ts.setTopic("telemetry");
        job.setTopics(List.of(ts));
        job.setConsumerGroups(List.of("cg-telemetry"));
        job.setSourceBootstrapServers("dc1-kafka:9092");
        job.setTargetBootstrapServers("dc2-kafka:9092");
        job.setTargetEndpoints(List.of("dc2-agent:50052"));
        job.setBatchSize(5000);
        job.setConsumerThreads(4);
        job.setBandwidthLimitMb(120.0);

        when(kafkaOrchestratorService.getActiveJobsForDc("dc1")).thenReturn(List.of(job));

        GetActiveKafkaJobsRequest req = GetActiveKafkaJobsRequest.newBuilder()
                .setDcId("dc1")
                .build();

        GetActiveKafkaJobsResponse resp = stub.getActiveKafkaJobs(req);
        assertNotNull(resp);
        assertEquals(1, resp.getJobsCount());

        KafkaJobItemProto item = resp.getJobs(0);
        assertEquals("job-kafka-1", item.getJobId());
        assertEquals("Kafka Sync DC1 -> DC2", item.getName());
        assertEquals("dc1", item.getSourceDc());
        assertEquals("dc2", item.getTargetDc());
        assertEquals(List.of("telemetry"), item.getTopicsList());
        assertEquals(List.of("cg-telemetry"), item.getConsumerGroupsList());
        assertEquals("dc1-kafka:9092", item.getSourceBootstrapServers());
        assertEquals(5000, item.getBatchSize());
        assertEquals(4, item.getConsumerThreads());
        assertEquals(120.0, item.getBandwidthLimitMb());
    }
}
