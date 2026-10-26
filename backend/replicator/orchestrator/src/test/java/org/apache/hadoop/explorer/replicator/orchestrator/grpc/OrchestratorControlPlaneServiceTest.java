package org.apache.hadoop.explorer.replicator.orchestrator.grpc;

import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.explorer.replicator.generated.control.*;
import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
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
                .setClusterId("dc1")
                .setMode("dual")
                .setKafkaMessagesReplicated(100L)
                .setKafkaBytesReplicated(4096L)
                .setKafkaCurrentLagMs(15L)
                .setActiveTransfers(2)
                .build();

        AgentHeartbeatResponse hbResp = stub.heartbeat(hbReq);
        assertTrue(hbResp.getAcknowledged());
        assertEquals("acknowledged", hbResp.getStatus());
        verify(kafkaOrchestratorService).updateMetricsFromHeartbeat("dc1", "agent-dc1", 100L, 4096L, 15L);
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
        // Выборка заданий кластера идет запросом с пределом, а не фильтром по всему списку
        when(jobService.listJobsForCluster(eq("dc1"), eq(true), any(org.springframework.data.domain.Pageable.class)))
                .thenReturn(new org.springframework.data.domain.PageImpl<>(List.of(job)));

        GetJobsRequest req = GetJobsRequest.newBuilder()
                .setClusterId("dc1")
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

    @Test
    @DisplayName("Реестр схем берется из топологии выбранных кластеров, а не из задачи")
    void testGetActiveKafkaJobsCarriesSchemaRegistryFromTopology() {
        ReplicatorProperties.ClusterConfig source = new ReplicatorProperties.ClusterConfig();
        source.setId("dc1");
        source.setDcId("dc1");
        source.setSchemaRegistryUrl("http://sr-dc1:8081");
        source.setSchemaRegistryType("HORTONWORKS");
        ReplicatorProperties.ClusterConfig target = new ReplicatorProperties.ClusterConfig();
        target.setId("dc2");
        target.setDcId("dc2");
        target.setSchemaRegistryUrl("http://sr-dc2:8081");
        target.setSchemaRegistryType("CONFLUENT");
        when(topologyRegistry.getCluster("dc1")).thenReturn(Optional.of(source));
        when(topologyRegistry.getCluster("dc2")).thenReturn(Optional.of(target));

        KafkaReplicationJobDto job = new KafkaReplicationJobDto();
        job.setId("job-kafka-sr");
        job.setName("SR from topology");
        job.setSourceDc("dc1");
        job.setTargetDc("dc2");
        job.setDirection("PUSH");
        job.setStatus("ACTIVE");
        job.setSchemaTranscodingEnabled(true);
        when(kafkaOrchestratorService.getActiveJobsForDc("dc1")).thenReturn(List.of(job));

        KafkaJobItemProto item = stub.getActiveKafkaJobs(GetActiveKafkaJobsRequest.newBuilder()
                .setDcId("dc1")
                .build()).getJobs(0);

        assertEquals("http://sr-dc1:8081", item.getSourceSchemaRegistryUrl(),
                "Выбор кластера обязан приносить его реестр схем: адрес задается в конфигурации "
                        + "топологии, а не в задаче");
        assertEquals("HORTONWORKS", item.getSourceSchemaRegistryType());
        assertEquals("http://sr-dc2:8081", item.getTargetSchemaRegistryUrl());
        assertEquals("CONFLUENT", item.getTargetSchemaRegistryType());
    }

    @Test
    @DisplayName("Интервал переноса смещений и транскодинг схем доезжают до агента в задаче")
    void testGetActiveKafkaJobsCarriesPerJobOffsetSyncAndTranscoding() {
        KafkaReplicationJobDto job = new KafkaReplicationJobDto();
        job.setId("job-kafka-per-job");
        job.setName("Per-job settings");
        job.setSourceDc("dc1");
        job.setTargetDc("dc2");
        job.setDirection("PUSH");
        job.setStatus("ACTIVE");
        job.setOffsetSyncIntervalSec(30);
        job.setSchemaTranscodingEnabled(true);

        when(kafkaOrchestratorService.getActiveJobsForDc("dc1")).thenReturn(List.of(job));

        KafkaJobItemProto item = stub.getActiveKafkaJobs(GetActiveKafkaJobsRequest.newBuilder()
                .setDcId("dc1")
                .build()).getJobs(0);

        assertEquals(30, item.getOffsetSyncIntervalSec(),
                "Интерфейс задает интервал переноса смещений на задачу, и агент обязан его получить: "
                        + "иначе поле формы остается декоративным");
        assertTrue(item.getSchemaTranscoding(),
                "Флаг транскодинга схем задается на задачу и обязан доезжать до агента");
    }

    @Test
    @DisplayName("getActiveKafkaJobs корректно инвертирует топологию и эндпоинты для Active-Active (BIDIRECTIONAL) обратного направления")
    void testGetActiveKafkaJobsBidirectionalReverse() {
        KafkaReplicationJobDto job = new KafkaReplicationJobDto();
        job.setId("job-kafka-active-active");
        job.setName("Kafka Active-Active DC1 <-> DC2");
        job.setSourceDc("dc1");
        job.setTargetDc("dc2");
        job.setDirection("BIDIRECTIONAL");
        job.setStatus("ACTIVE");
        CreateKafkaJobRequest.TopicSetting ts = new CreateKafkaJobRequest.TopicSetting();
        ts.setTopic("orders-active");
        job.setTopics(List.of(ts));
        job.setSourceBootstrapServers("dc1-kafka:9092");
        job.setTargetBootstrapServers("dc2-kafka:9092");

        when(kafkaOrchestratorService.getActiveJobsForDc("dc2")).thenReturn(List.of(job));
        when(kafkaOrchestratorService.getLiveReceiverEndpoints("dc1")).thenReturn(List.of("dc1-agent:50052"));

        GetActiveKafkaJobsRequest req = GetActiveKafkaJobsRequest.newBuilder()
                .setDcId("dc2")
                .build();

        GetActiveKafkaJobsResponse resp = stub.getActiveKafkaJobs(req);
        assertNotNull(resp);
        assertEquals(1, resp.getJobsCount());

        KafkaJobItemProto item = resp.getJobs(0);
        assertEquals("job-kafka-active-active", item.getJobId());
        assertEquals("dc2", item.getSourceDc(), "Для обратного агента в DC2 источником должен быть DC2");
        assertEquals("dc1", item.getTargetDc(), "Для обратного агента в DC2 целью должен быть DC1");
        assertEquals("dc2-kafka:9092", item.getSourceBootstrapServers());
        assertEquals("dc1-kafka:9092", item.getTargetBootstrapServers());
        assertEquals(List.of("dc1-agent:50052"), item.getTargetEndpointsList(), "Эндпоинты должны указывать на приемники в DC1");
        assertEquals("BIDIRECTIONAL", item.getDirection());
    }

    @Test
    @DisplayName("reportHmsProgress: отказ координатора доходит до агента как success=false")
    void testReportHmsProgressRejectionReachesAgent() {
        when(hmsCoordinatorService.updateJobProgress(eq("hms-job-fenced"), any())).thenReturn(false);

        ReportHmsProgressResponse resp = stub.reportHmsProgress(ReportHmsProgressRequest.newBuilder()
                .setJobId("hms-job-fenced").setStatus("BOOTSTRAPPING").setAgentId("agent-zombie").build());

        assertFalse(resp.getSuccess(), "агент, у которого отобрали задачу, обязан узнать об отказе");
    }

    @Test
    @DisplayName("reportHmsProgress: незаданная позиция (0 в proto3) передается как null")
    void testReportHmsProgressZeroPositionIsAbsent() {
        when(hmsCoordinatorService.updateJobProgress(eq("hms-job-zero"), any())).thenReturn(true);

        stub.reportHmsProgress(ReportHmsProgressRequest.newBuilder()
                .setJobId("hms-job-zero").setStatus("BOOTSTRAPPING").setAgentId("agent-dc1").build());

        verify(hmsCoordinatorService).updateJobProgress(eq("hms-job-zero"),
                argThat(report -> report.lastProcessedEventId() == null));
    }

    @Test
    @DisplayName("reportHmsProgress корректно передает DDL события в hmsCoordinatorService")
    void testReportHmsProgressWithEvents() {
        ReportHmsProgressRequest req = ReportHmsProgressRequest.newBuilder()
                .setJobId("hms-job-test-123")
                .setStatus("ACTIVE")
                .setMessage("Синхронизировано 2 событий CDC")
                .setTablesSynced(5)
                .setPartitionsSynced(10)
                .setLastEventId(1052L)
                .setBootstrapEventId(1000L)
                .setEventLag(0L)
                .setTotalTables(5)
                .setTotalPartitions(10)
                .setAgentId("agent-dc1")
                .addEvents(HmsEventReportProto.newBuilder()
                        .setEventId(1051L)
                        .setEventType("ADD_PARTITION")
                        .setTableName("sales")
                        .setPartitionName("dt=2026-10-22")
                        .setStatus("APPLIED")
                        .build())
                .addEvents(HmsEventReportProto.newBuilder()
                        .setEventId(1052L)
                        .setEventType("CREATE_TABLE")
                        .setTableName("orders")
                        .setStatus("APPLIED")
                        .build())
                .build();

        when(hmsCoordinatorService.updateJobProgress(eq("hms-job-test-123"), any())).thenReturn(true);
        ReportHmsProgressResponse resp = stub.reportHmsProgress(req);
        assertNotNull(resp);
        assertTrue(resp.getSuccess());

        verify(hmsCoordinatorService).updateJobProgress(eq("hms-job-test-123"), argThat(report -> {
            assertNotNull(report);
            assertEquals("ACTIVE", report.status());
            assertEquals(5, report.replicatedTables());
            assertEquals(10, report.replicatedPartitions());
            assertEquals(1052L, report.lastProcessedEventId());
            assertEquals(1000L, report.bootstrapEventId());
            assertEquals(0L, report.eventLag());
            assertEquals("agent-dc1", report.agentId());
            assertNotNull(report.events());
            assertEquals(2, report.events().size());
            assertEquals(1051L, report.events().get(0).eventId());
            assertEquals("ADD_PARTITION", report.events().get(0).eventType());
            assertEquals("sales", report.events().get(0).tableName());
            assertEquals("dt=2026-10-22", report.events().get(0).partitionName());
            assertEquals("APPLIED", report.events().get(0).status());
            assertEquals(1052L, report.events().get(1).eventId());
            assertEquals("CREATE_TABLE", report.events().get(1).eventType());
            assertEquals("orders", report.events().get(1).tableName());
            return true;
        }));
    }

    @Test
    @DisplayName("getPendingHmsJobs: корректная передача lastProcessedEventId, drop-флагов и assignedAgentId")
    void testGetPendingHmsJobs_ReturnsAllFieldsIncludingLastProcessedEventId() {
        HmsPendingJobDto jobDto = new HmsPendingJobDto(
                "hms-job-xyz",
                "dc1",
                "dc2",
                "analytics_dw",
                "analytics_dw",
                "sales.*",
                true,
                true,
                "replicator-agent-dc2:50051",
                "ACTIVE",
                4242L,
                "agent-dc1-worker",
                "hdfs/hadoop@REALM"
        );

        when(hmsCoordinatorService.getPendingJobsForCluster("dc1", "agent-dc1-worker"))
                .thenReturn(List.of(jobDto));

        GetPendingHmsJobsRequest req = GetPendingHmsJobsRequest.newBuilder()
                .setClusterId("dc1")
                .setAgentId("agent-dc1-worker")
                .build();

        GetPendingHmsJobsResponse resp = stub.getPendingHmsJobs(req);
        assertNotNull(resp);
        assertEquals(1, resp.getJobsCount());

        HmsPendingJobItemProto jobProto = resp.getJobs(0);
        assertEquals("hms-job-xyz", jobProto.getJobId());
        assertEquals("dc1", jobProto.getSourceCluster());
        assertEquals("dc2", jobProto.getTargetCluster());
        assertEquals("analytics_dw", jobProto.getSourceDb());
        assertEquals("analytics_dw", jobProto.getTargetDb());
        assertEquals("sales.*", jobProto.getTablePattern());
        assertEquals("ACTIVE", jobProto.getStatus());
        assertEquals("replicator-agent-dc2:50051", jobProto.getTargetAgentGrpcAddress());
        assertEquals(4242L, jobProto.getLastProcessedEventId());
        assertTrue(jobProto.getDropExtraneousTables());
        assertTrue(jobProto.getDropExtraneousPartitions());
        assertEquals("agent-dc1-worker", jobProto.getAssignedAgentId());
        assertEquals("hdfs/hadoop@REALM", jobProto.getExecutionPrincipal());
    }

    @Test
    @DisplayName("createHdfsSubjob: данные HMS-задачи пишутся от ее пользователя, без пользователя — от служебной учетки")
    void hmsSubjobRunsAsExecutionPrincipal() {
        when(hmsCoordinatorService.acceptsSubjobFrom(any(), any())).thenReturn(true);
        when(jobService.findReusableSubjob(any(), any(), any())).thenReturn(Optional.empty());
        when(jobService.createJob(any(), any())).thenReturn(null);

        stub.createHdfsSubjob(CreateHdfsSubjobRequest.newBuilder().setParentJobId("hms-job-a").setSourcePath("/s")
                .setTargetPath("/t").setExecutionPrincipal("analyst@COMPANY.LOCAL").setAgentId("agent-dc1").build());
        stub.createHdfsSubjob(CreateHdfsSubjobRequest.newBuilder().setParentJobId("hms-job-b").setSourcePath("/s2")
                .setTargetPath("/t2").setAgentId("agent-dc1").build());

        verify(jobService).createJob(argThat(r -> r != null && "analyst@COMPANY.LOCAL".equals(r.executionPrincipal())
                && Boolean.FALSE.equals(r.runAsServiceAccount())), any());
        verify(jobService).createJob(argThat(r -> r != null && "/s2".equals(r.sourcePath())
                && Boolean.TRUE.equals(r.runAsServiceAccount())), any());
    }
}
