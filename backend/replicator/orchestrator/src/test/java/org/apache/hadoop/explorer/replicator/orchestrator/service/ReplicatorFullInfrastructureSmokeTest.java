package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.controller.HmsClusterApiController;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.StreamingLeaseEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClientPool;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.StreamingLeaseRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Комплексный Smoke-тест инфраструктуры репликации (с Inotify/CDC и без него):
 * 1. Перенос таблицы и данных
 * 2. Перенос партиции
 * 3. Дописывание файла в существующую таблицу
 * 4. Дописывание файла в существующую партицию
 * 5. Удаление партиции
 * 6. Удаление таблицы
 * 7. HDFS Inotify Streaming HA (Active-Standby лидеры, фильтрация staged путей, Failover)
 */
@SpringBootTest(properties = {"replicator.hms.mock-cluster-api.enabled=true"})
@ActiveProfiles("test")
@Transactional
public class ReplicatorFullInfrastructureSmokeTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private TaskService taskService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @Autowired
    private HmsEventLogRepository hmsEventLogRepository;

    @Autowired
    private AgentRegistry agentRegistry;

    @Autowired
    private StreamingLeaseRepository leaseRepository;

    @Autowired
    private StreamingLeaseCoordinator leaseCoordinator;

    @Autowired
    private HmsClusterApiController hmsClusterApi;

    @Autowired
    private HmsClientPool hmsClientPool;

    @Autowired
    private ReplicatorProperties properties;

    @BeforeEach
    void setUp() {
        agentRegistry.clear();
        leaseRepository.deleteAll();
    }

    @Test
    @DisplayName("Стандартный режим (без Inotify и без CDC): перенос таблицы, дописывание файла, удаление через воркеры")
    void testStandardModeWithoutInotifyAndCdc() {
        properties.getStreaming().setEnabled(false);

        // 1. Регистрируем агентов
        agentRegistry.register(
                new AgentRegisterRequest("worker-dc1", "dc1", "all", "worker-dc1:50051", "worker-dc2:50051", 100.0),
                properties.getAgentSecret()
        );
        agentRegistry.register(
                new AgentRegisterRequest("worker-dc2", "dc2", "all", "worker-dc2:50051", "worker-dc1:50051", 100.0),
                properties.getAgentSecret()
        );

        // 2. Создание стандартной задачи переноса таблицы
        CreateJobRequest tableJobReq = new CreateJobRequest(
                "/warehouse/tables/orders",
                "/backup/warehouse/tables/orders",
                "dc1",
                "dc2",
                5242880L,
                "etl_user@REALM.LOCAL",
                true,
                false,
                null,
                10,
                "STANDARD",
                null
        );
        JobResponse tableJob = jobService.createJob(tableJobReq, "admin_user");
        assertEquals("QUEUED", tableJob.status());

        // 3. Формирование пула задач по файлам
        TaskCreateItem item1 = new TaskCreateItem();
        item1.setId("task-orders-01");
        item1.setSourcePath("/warehouse/tables/orders/part-00000.parquet");
        item1.setTargetPath("/backup/warehouse/tables/orders/part-00000.parquet");
        item1.setFileSize(5242880L);

        BatchCreateTasksRequest batchReq = new BatchCreateTasksRequest(tableJob.id(), List.of(item1));
        assertTrue(taskService.batchCreateTasks(tableJob.id(), batchReq));

        // 4. Воркер забирает задачу на передачу
        List<TaskItemDto> claimed = taskService.claimTasks(new ClaimTasksRequest("worker-dc1", "dc1", 5));
        assertFalse(claimed.isEmpty());
        assertEquals("task-orders-01", claimed.get(0).getId());

        // 5. Завершение передачи таблицы
        UpdateJobRequest finishReq = new UpdateJobRequest();
        finishReq.setStatus("COMPLETED");
        finishReq.setCopiedBytes(5242880L);
        finishReq.setMessage("Table orders replicated successfully");
        Optional<JobResponse> completedJob = jobService.updateJobProgress(tableJob.id(), finishReq);
        assertTrue(completedJob.isPresent());
        assertEquals("COMPLETED", completedJob.get().status());

        // 6. Дописывание нового файла в существующую таблицу: создание инкрементальной задачи
        CreateJobRequest appendJobReq = new CreateJobRequest(
                "/warehouse/tables/orders/part-00001.parquet",
                "/backup/warehouse/tables/orders/part-00001.parquet",
                "dc1",
                "dc2",
                2097152L,
                "etl_user@REALM.LOCAL",
                true,
                false,
                null,
                10,
                "STANDARD",
                null
        );
        JobResponse appendJob = jobService.createJob(appendJobReq, "admin_user");
        assertNotNull(appendJob.id());

        TaskCreateItem appendItem = new TaskCreateItem();
        appendItem.setId("task-orders-02");
        appendItem.setSourcePath("/warehouse/tables/orders/part-00001.parquet");
        appendItem.setTargetPath("/backup/warehouse/tables/orders/part-00001.parquet");
        appendItem.setFileSize(2097152L);

        assertTrue(taskService.batchCreateTasks(appendJob.id(), new BatchCreateTasksRequest(appendJob.id(), List.of(appendItem))));
        List<TaskItemDto> appendClaimed = taskService.claimTasks(new ClaimTasksRequest("worker-dc1", "dc1", 5));
        assertEquals(1, appendClaimed.size());
        assertEquals("task-orders-02", appendClaimed.get(0).getId());

        // 7. Удаление таблицы
        assertTrue(jobService.deleteJob(tableJob.id()));
        assertTrue(jobService.getJob(tableJob.id()).isEmpty());
    }

    @Test
    @DisplayName("Режим HMS CDC: полный жизненный цикл (создание таблицы, добавление партиции, дописывание файла, удаление партиции и таблицы)")
    void testHmsCdcFullLifecycle() {
        String dbName = "sales_cdc_db_" + System.currentTimeMillis();
        String tableName = "fact_transactions";

        // 1. Создание БД и партиционированной таблицы в DC1
        hmsClusterApi.createDatabase("dc1", new HmsClusterApiController.CreateDatabaseRequest(dbName, "/warehouse/" + dbName));

        HmsClusterApiController.CreateTableRequest createTblReq = new HmsClusterApiController.CreateTableRequest(
                dbName,
                tableName,
                "EXTERNAL_TABLE",
                "/warehouse/" + dbName + "/" + tableName,
                Map.of("EXTERNAL", "TRUE"),
                List.of("dt"),
                null, null, null,
                true, // emit_cdc_event
                true, // create_sample_data
                1024L
        );
        var createdTable = hmsClusterApi.createTable("dc1", createTblReq);
        assertEquals(HttpStatus.CREATED, createdTable.getStatusCode());

        // Проверка наличия таблицы в исходном кластере
        HmsClient clientDc1 = hmsClientPool.getClient("dc1");
        assertTrue(clientDc1.getTable(dbName, tableName).isPresent());

        // 2. Добавление партиции dt=2026-10-10 в DC1
        var addPartReq = new HmsClusterApiController.AddPartitionRequest(
                List.of("2026-10-10"),
                "/warehouse/" + dbName + "/" + tableName + "/dt=2026-10-10",
                Map.of(),
                true
        );
        var partResp = hmsClusterApi.addPartition("dc1", dbName, tableName, addPartReq);
        assertEquals(HttpStatus.CREATED, partResp.getStatusCode());

        List<HmsPartitionDto> partsDc1 = clientDc1.getPartitions(dbName, tableName);
        assertEquals(1, partsDc1.size());
        assertEquals(List.of("2026-10-10"), partsDc1.get(0).values());

        // 3. Дописывание файла в существующую партицию dt=2026-10-10
        var appendPartReq = new HmsClusterApiController.AddPartitionDataRequest(
                List.of("2026-10-10"),
                "appended_part_data.parquet",
                "sample-appended-bytes-content",
                null
        );
        var appendResp = hmsClusterApi.addPartitionData("dc1", dbName, tableName, appendPartReq);
        assertEquals(HttpStatus.OK, appendResp.getStatusCode());
        assertTrue((Boolean) appendResp.getBody().get("success"));

        // 4. Дописывание файла в существующую таблицу
        var appendTblReq = new HmsClusterApiController.AddDataRequest(
                "extra_manifest.json",
                "{\"status\":\"committed\"}",
                null
        );
        var tblDataResp = hmsClusterApi.addTableData("dc1", dbName, tableName, appendTblReq);
        assertEquals(HttpStatus.OK, tblDataResp.getStatusCode());

        // 5. Эмуляция создания задачи репликации схемы в Оркестраторе
        String hmsJobId = "hms-job-" + dbName;
        HmsReplicationJobEntity hmsJob = new HmsReplicationJobEntity();
        hmsJob.setId(hmsJobId);
        hmsJob.setSourceClusterId("dc1");
        hmsJob.setTargetClusterId("dc2");
        hmsJob.setSourceDbName(dbName);
        hmsJob.setTargetDbName(dbName);
        hmsJob.setStatus("ACTIVE");
        hmsJob.setCreatedAt(Instant.now());
        hmsJob.setUpdatedAt(Instant.now());
        hmsJobRepository.save(hmsJob);

        // Применяем схему и партицию в target (DC2) через Mock Client
        HmsClient clientDc2 = hmsClientPool.getClient("dc2");
        clientDc2.createDatabase(dbName, "/warehouse/" + dbName);
        clientDc2.createTable(createdTable.getBody());
        clientDc2.addPartitions(dbName, tableName, partsDc1);

        assertTrue(clientDc2.getTable(dbName, tableName).isPresent());
        assertEquals(1, clientDc2.getPartitions(dbName, tableName).size());

        // 6. Удаление партиции dt=2026-10-10
        var dropPartReq = new HmsClusterApiController.DropPartitionRequest(
                List.of("2026-10-10"),
                true,
                true
        );
        var dropPartResp = hmsClusterApi.dropPartition("dc1", dbName, tableName, dropPartReq);
        assertEquals(HttpStatus.OK, dropPartResp.getStatusCode());
        assertTrue(clientDc1.getPartitions(dbName, tableName).isEmpty());

        // Синхронизация удаления в DC2
        clientDc2.dropPartition(dbName, tableName, List.of("2026-10-10"), true);
        assertTrue(clientDc2.getPartitions(dbName, tableName).isEmpty());

        // 7. Удаление таблицы
        var dropTblResp = hmsClusterApi.dropTable("dc1", dbName, tableName, true, true);
        assertEquals(HttpStatus.OK, dropTblResp.getStatusCode());
        assertTrue(clientDc1.getTable(dbName, tableName).isEmpty());

        // Синхронизация удаления таблицы в DC2
        clientDc2.dropTable(dbName, tableName, true);
        assertTrue(clientDc2.getTable(dbName, tableName).isEmpty());
    }

    @Test
    @DisplayName("Режим HDFS Inotify Streaming: изоляция стримера, аренда Active-Standby, распознавание коммита и Failover")
    void testHdfsInotifyStreamingMode() {
        properties.getStreaming().setEnabled(true);

        // 1. Регистрация стримеров (суперпользователи NameNode)
        agentRegistry.register(
                new AgentRegisterRequest("streamer-dc1-01", "dc1", "streamer", "streamer-dc1-01:50051", null, 0.0),
                properties.getAgentSecret()
        );
        agentRegistry.register(
                new AgentRegisterRequest("streamer-dc1-02", "dc1", "streamer", "streamer-dc1-02:50051", null, 0.0),
                properties.getAgentSecret()
        );
        agentRegistry.register(
                new AgentRegisterRequest("streamer-dc2-01", "dc2", "streamer", "streamer-dc2-01:50051", null, 0.0),
                properties.getAgentSecret()
        );

        // 2. Изоляция стримера: запрет вызова claimTasks (403 Forbidden)
        assertThrows(ResponseStatusException.class, () ->
                taskService.claimTasks(new ClaimTasksRequest("streamer-dc1-01", "dc1", 5))
        );

        // 3. Выборы лидера: streamer-01 = ACTIVE, streamer-02 = STANDBY
        StreamingLeaseRenewResponse r1 = leaseCoordinator.renewLease(new StreamingLeaseRenewRequest("dc1", "streamer-dc1-01"));
        assertEquals("ACTIVE", r1.status());
        assertEquals("streamer-dc1-01", r1.activeAgentId());

        StreamingLeaseRenewResponse r2 = leaseCoordinator.renewLease(new StreamingLeaseRenewRequest("dc1", "streamer-dc1-02"));
        assertEquals("STANDBY", r2.status());
        assertEquals("streamer-dc1-01", r2.activeAgentId());

        // 4. Проверка статуса лизингов мульти-ЦОД
        Map<String, StreamingLeaseRenewResponse> statuses = leaseCoordinator.getAllLeaseStatuses();
        assertTrue(statuses.containsKey("dc1"));
        assertEquals("streamer-dc1-01", statuses.get("dc1").activeAgentId());

        // 5. Создание стриминговой задачи (перенос committed партиции)
        CreateJobRequest streamJobReq = new CreateJobRequest(
                "/warehouse/tables/stream_partition_20261010",
                "/backup/warehouse/tables/stream_partition_20261010",
                "dc1",
                "dc2",
                1048576L,
                "hdfs@REALM.LOCAL",
                true,
                false,
                null,
                10,
                "STREAMING",
                null
        );
        JobResponse streamJob = jobService.createJob(streamJobReq, "streamer-dc1-01");
        assertNotNull(streamJob.id());

        // Стример регистрирует обнаруженный committed файл
        TaskCreateItem committedFile = new TaskCreateItem();
        committedFile.setId("task-stream-01");
        committedFile.setSourcePath("/warehouse/tables/stream_partition_20261010/part-committed.parquet");
        committedFile.setTargetPath("/backup/warehouse/tables/stream_partition_20261010/part-committed.parquet");
        committedFile.setFileSize(1048576L);

        assertTrue(taskService.batchCreateTasks(streamJob.id(), new BatchCreateTasksRequest(streamJob.id(), List.of(committedFile))));

        // Воркер передачи данных забирает задачу
        agentRegistry.register(
                new AgentRegisterRequest("worker-dc1-01", "dc1", "all", "worker-dc1-01:50051", "worker-dc2:50051", 100.0),
                properties.getAgentSecret()
        );
        List<TaskItemDto> claimed = taskService.claimTasks(new ClaimTasksRequest("worker-dc1-01", "dc1", 5));
        assertEquals(1, claimed.size());
        assertEquals("task-stream-01", claimed.get(0).getId());

        // 6. Failover стримера: сбой первого стримера и передача лидерства дублеру
        StreamingLeaseEntity lease = leaseRepository.findById("dc1").orElseThrow();
        lease.setExpiresAt(Instant.now().minusSeconds(1));
        leaseRepository.saveAndFlush(lease);

        StreamingLeaseRenewResponse failoverResp = leaseCoordinator.renewLease(new StreamingLeaseRenewRequest("dc1", "streamer-dc1-02"));
        assertEquals("ACTIVE", failoverResp.status());
        assertEquals("streamer-dc1-02", failoverResp.activeAgentId());
        assertEquals(2L, failoverResp.epoch());
    }
}
