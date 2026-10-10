package org.apache.hadoop.explorer.replicator.hms;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.explorer.replicator.generated.GetMetadataSnapshotResponse;
import org.apache.hadoop.explorer.replicator.generated.HmsOperationResponse;
import org.apache.hadoop.explorer.replicator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.hms.service.HmsPipelineSender;
import org.apache.hadoop.explorer.replicator.hms.service.HmsTransferServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class HmsAgentPipeTest {

    private Server grpcServer;
    private int grpcPort;
    private MockHmsClient targetHmsClient;
    private HmsPipelineSender pipelineSender;

    @BeforeEach
    void setup() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            grpcPort = socket.getLocalPort();
        }

        targetHmsClient = new MockHmsClient("dc2", "APACHE_3.1.3");
        HmsTransferServiceImpl transferService = new HmsTransferServiceImpl(targetHmsClient);

        grpcServer = NettyServerBuilder.forPort(grpcPort)
                .addService(transferService)
                .build()
                .start();

        pipelineSender = new HmsPipelineSender("127.0.0.1:" + grpcPort, false, false);
    }

    @AfterEach
    void tearDown() {
        if (pipelineSender != null) {
            pipelineSender.close();
        }
        if (grpcServer != null) {
            grpcServer.shutdownNow();
        }
    }

    @Test
    @DisplayName("Прямой gRPC конвейер: Создание БД, идемпотентный ApplyTable, пакетные партиции и Reconciliation")
    void testDirectAgentPipelineEndToEnd() {
        String jobId = "job-pipe-" + UUID.randomUUID().toString().substring(0, 8);
        String dbName = "telemetry_db";

        // 1. ApplyDatabase
        HmsOperationResponse dbResp = pipelineSender.applyDatabase(jobId, dbName, "hdfs://ns-target:8020/warehouse/telemetry.db");
        assertTrue(dbResp.getSuccess());
        assertTrue(targetHmsClient.getAllDatabases().contains(dbName));

        // 2. ApplyTable (Первичное создание)
        HmsTableDto tableDto = new HmsTableDto(
                "hive",
                dbName,
                "sensor_metrics",
                "EXTERNAL_TABLE",
                "hdfs://ns-target:8020/warehouse/telemetry.db/sensor_metrics",
                Map.of("EXTERNAL", "TRUE"),
                List.of("dt", "region"),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        );
        HmsOperationResponse tblResp = pipelineSender.applyTable(jobId, tableDto);
        assertTrue(tblResp.getSuccess());

        Optional<HmsTableDto> createdTable = targetHmsClient.getTable(dbName, "sensor_metrics");
        assertTrue(createdTable.isPresent());
        assertEquals(2, createdTable.get().partitionKeys().size());

        // 3. ApplyTable (Идемпотентный alterTable при повторном вызове)
        HmsTableDto updatedDto = new HmsTableDto(
                "hive",
                dbName,
                "sensor_metrics",
                "EXTERNAL_TABLE",
                "hdfs://ns-target:8020/warehouse/telemetry.db/sensor_metrics",
                Map.of("EXTERNAL", "TRUE", "comment", "v2-updated"),
                List.of("dt", "region"),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        );
        HmsOperationResponse alterResp = pipelineSender.applyTable(jobId, updatedDto);
        assertTrue(alterResp.getSuccess());
        assertEquals("v2-updated", targetHmsClient.getTable(dbName, "sensor_metrics").orElseThrow().parameters().get("comment"));

        // 4. ApplyPartitionBatch (Пакетная вставка партиций)
        HmsPartitionDto p1 = new HmsPartitionDto("hive", dbName, "sensor_metrics", List.of("2026-10-10", "eu-west"),
                "hdfs://ns-target:8020/warehouse/telemetry.db/sensor_metrics/dt=2026-10-10/region=eu-west", Map.of("numRows", "100"));
        HmsPartitionDto p2 = new HmsPartitionDto("hive", dbName, "sensor_metrics", List.of("2026-10-10", "us-east"),
                "hdfs://ns-target:8020/warehouse/telemetry.db/sensor_metrics/dt=2026-10-10/region=us-east", Map.of("numRows", "200"));
        HmsPartitionDto pExtra = new HmsPartitionDto("hive", dbName, "sensor_metrics", List.of("2026-10-09", "obsolete"),
                "hdfs://ns-target:8020/warehouse/telemetry.db/sensor_metrics/dt=2026-10-09/region=obsolete", Map.of());

        HmsOperationResponse partBatchResp = pipelineSender.applyPartitionBatch(jobId, dbName, "sensor_metrics", List.of(p1, p2, pExtra));
        assertTrue(partBatchResp.getSuccess());
        assertEquals(3, targetHmsClient.getPartitions(dbName, "sensor_metrics").size());

        // 5. GetMetadataSnapshot
        GetMetadataSnapshotResponse snapshot = pipelineSender.getMetadataSnapshot(jobId, dbName, "sensor_metrics");
        assertTrue(snapshot.getDbExists());
        assertEquals(3, snapshot.getExistingPartitionsList().size());

        // 6. ReconcileExtraneous (Удаление лишней партиции)
        HmsOperationResponse dropPartResp = pipelineSender.reconcileExtraneousPartitions(jobId, dbName, "sensor_metrics",
                List.of(List.of("2026-10-09", "obsolete")));
        assertTrue(dropPartResp.getSuccess());
        assertEquals(2, targetHmsClient.getPartitions(dbName, "sensor_metrics").size());

        // 7. ReconcileExtraneous (Удаление лишней таблицы)
        targetHmsClient.createTable(new HmsTableDto("hive", dbName, "garbage_table", "EXTERNAL_TABLE", "/loc", Map.of(), List.of(), "i", "o", "s"));
        assertTrue(targetHmsClient.getAllTables(dbName).contains("garbage_table"));

        HmsOperationResponse dropTblResp = pipelineSender.reconcileExtraneousTables(jobId, dbName, List.of("garbage_table"));
        assertTrue(dropTblResp.getSuccess());
        assertFalse(targetHmsClient.getAllTables(dbName).contains("garbage_table"));
    }
}
