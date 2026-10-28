package org.apache.hadoop.explorer.replicator.hms;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsFieldSchemaDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.hms.service.HmsTaskExecutor;
import org.apache.hadoop.explorer.replicator.hms.service.HmsTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.model.HmsEventReportDto;
import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
import org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Покрытие типов событий CDC.
 *
 * <p>Обрабатывались пять типов событий, а остальные молча отмечались обработанными: позиция
 * в журнале двигалась дальше, в журнале задачи не оставалось следа. `ALTER_TABLE` — добавление
 * колонки, переименование, смена локации — не доезжал до приемника никогда, и схема расходилась
 * навсегда, без единой записи об этом.
 */
@DisplayName("HMS CDC: события не пропускаются молча")
class CdcEventCoverageTest {

    private static final String SRC_DB = "analytics";
    private static final String DST_DB = "analytics_replica";

    private Server targetGrpcServer;
    private int targetGrpcPort;
    private MockHmsClient srcHmsClient;
    private MockHmsClient dstHmsClient;
    private HmsTaskExecutor executor;

    private final List<HmsProgressReportRequest> reports = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            targetGrpcPort = socket.getLocalPort();
        }
        srcHmsClient = new MockHmsClient("dc1", "HDP_3.1");
        dstHmsClient = new MockHmsClient("dc2", "APACHE_3.1.3");

        targetGrpcServer = NettyServerBuilder.forPort(targetGrpcPort)
                .addService(new HmsTransferServiceImpl(dstHmsClient))
                .build()
                .start();

        OrchestratorGrpcClient orchestratorClient = new OrchestratorGrpcClient() {
            @Override
            public boolean reportHmsProgress(String jobId, HmsProgressReportRequest report) {
                reports.add(report);
                return true;
            }

            @Override
            public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl) {
                return "subjob-1";
            }

            @Override
            public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath,
                                           String srcCl, String dstCl, String executionPrincipal) {
                return "subjob-1";
            }

            @Override
            public boolean awaitSubjobsCompletion(java.util.Collection<String> subjobIds, long timeoutSeconds,
                                                  java.util.function.BooleanSupplier cancelled) {
                return true;
            }
        };

        executor = new HmsTaskExecutor("agent-src-01", "dc1", srcHmsClient, orchestratorClient,
                false, false, 4, 100);
    }

    @AfterEach
    void tearDown() {
        if (executor != null) executor.close();
        if (targetGrpcServer != null) targetGrpcServer.shutdownNow();
    }

    private HmsTableDto table(String db, List<HmsFieldSchemaDto> columns) {
        return new HmsTableDto("hive", db, "sales_daily", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/" + db + ".db/sales_daily",
                Map.of("EXTERNAL", "TRUE"), Collections.emptyList(),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                columns, Collections.emptyList(), Map.of());
    }

    private HmsPendingJobDto job(long lastEventId) {
        return new HmsPendingJobDto("hms-job-cdc", "dc1", "dc2", SRC_DB, DST_DB, "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "ACTIVE", lastEventId);
    }

    private List<HmsEventReportDto> lastEvents() {
        assertFalse(reports.isEmpty(), "Отчет о прогрессе должен быть отправлен");
        return reports.get(reports.size() - 1).events();
    }

    @Test
    @DisplayName("ALTER_TABLE доезжает до приемника: новая колонка появляется в схеме")
    void alterTableShouldReachTheTarget() {
        srcHmsClient.createDatabase(SRC_DB, "hdfs://ns-hot:8020/warehouse/analytics.db");
        srcHmsClient.createTable(table(SRC_DB, List.of(new HmsFieldSchemaDto("id", "bigint"))));
        dstHmsClient.createDatabase(DST_DB, null);
        dstHmsClient.createTable(table(DST_DB, List.of(new HmsFieldSchemaDto("id", "bigint"))));

        // На источнике добавили колонку
        srcHmsClient.alterTable(table(SRC_DB, List.of(
                new HmsFieldSchemaDto("id", "bigint"),
                new HmsFieldSchemaDto("region", "string"))));
        srcHmsClient.emitEvent("ALTER_TABLE", SRC_DB, "sales_daily", "{}");

        int processed = executor.pollAndSyncCdc(job(0L));

        assertEquals(1, processed, "Событие должно быть обработано");
        HmsTableDto target = dstHmsClient.getTable(DST_DB, "sales_daily").orElseThrow();
        assertEquals(2, target.columns().size(),
                "Колонка, добавленная на источнике, должна появиться на приемнике: " + target.columns());
    }

    @Test
    @DisplayName("Неподдерживаемое событие помечается в журнале, а не исчезает")
    void unsupportedEventShouldBeRecorded() {
        srcHmsClient.createDatabase(SRC_DB, "hdfs://ns-hot:8020/warehouse/analytics.db");
        srcHmsClient.createTable(table(SRC_DB, List.of(new HmsFieldSchemaDto("id", "bigint"))));
        srcHmsClient.emitEvent("ALTER_DATABASE", SRC_DB, null, "{}");

        executor.pollAndSyncCdc(job(0L));

        assertTrue(lastEvents().stream().anyMatch(e -> "SKIPPED_UNSUPPORTED".equalsIgnoreCase(e.status())),
                "Пропущенный тип события должен оставлять след в журнале: " + lastEvents());
    }
}
