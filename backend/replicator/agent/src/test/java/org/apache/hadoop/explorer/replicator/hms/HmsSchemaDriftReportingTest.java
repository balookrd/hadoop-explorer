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
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Фиксация дрейфа схемы при переносе.
 *
 * <p>Детектор расхождений существовал, но его никто не вызывал, а сканирование в оркестраторе
 * возвращало заглушку «дрейфа нет». Сравнивать схемы может только агент: он один видит и
 * источник, и метастор приемника. Сравнение идет перед применением DDL — именно тогда видно,
 * что на приемнике было до нас (ручная правка схемы или отставание от источника). После
 * применения таблицы совпадают по построению, и сравнивать бессмысленно.
 */
@DisplayName("HMS: дрейф схемы фиксируется в журнале задачи")
class HmsSchemaDriftReportingTest {

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
                return createHdfsSubjob(hmsJobId, srcPath, dstPath, srcCl, dstCl, null);
            }

            @Override
            public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath,
                                           String srcCl, String dstCl, String executionPrincipal) {
                return "subjob-1";
            }
        };

        executor = new HmsTaskExecutor("agent-src-01", "dc1", srcHmsClient, orchestratorClient,
                false, false, 4, 2);
    }

    @AfterEach
    void tearDown() {
        if (executor != null) executor.close();
        if (targetGrpcServer != null) targetGrpcServer.shutdownNow();
    }

    private HmsPendingJobDto job() {
        return new HmsPendingJobDto("hms-job-drift", "dc1", "dc2", SRC_DB, DST_DB, "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L);
    }

    private HmsTableDto table(String db, String name, List<HmsFieldSchemaDto> columns, String serdeLib) {
        return new HmsTableDto("hive", db, name, "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/" + db + ".db/" + name,
                Map.of("EXTERNAL", "TRUE"), Collections.emptyList(),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                serdeLib, columns, Collections.emptyList(), Map.of());
    }

    private List<HmsEventReportDto> finalEvents() {
        assertFalse(reports.isEmpty(), "Отчет о прогрессе должен быть отправлен");
        return reports.get(reports.size() - 1).events();
    }

    @Test
    @DisplayName("Расхождение колонок на приемнике попадает в журнал до применения DDL")
    void driftOnExistingTargetTableShouldBeRecorded() {
        srcHmsClient.createDatabase(SRC_DB, "hdfs://ns-hot:8020/warehouse/analytics.db");
        srcHmsClient.createTable(table(SRC_DB, "sales_daily", List.of(
                new HmsFieldSchemaDto("id", "bigint"),
                new HmsFieldSchemaDto("amount", "decimal(10,2)"),
                new HmsFieldSchemaDto("region", "string")),
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));

        // На приемнике таблица уже есть, но со своей схемой: тип колонки изменен, одной колонки нет
        dstHmsClient.createDatabase(DST_DB, null);
        dstHmsClient.createTable(table(DST_DB, "sales_daily", List.of(
                new HmsFieldSchemaDto("id", "bigint"),
                new HmsFieldSchemaDto("amount", "double")),
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));

        executor.runBootstrap(job());

        Optional<HmsEventReportDto> drift = finalEvents().stream()
                .filter(e -> "SCHEMA_DRIFT".equalsIgnoreCase(e.status()))
                .findFirst();
        assertTrue(drift.isPresent(), "Дрейф схемы должен быть зафиксирован в журнале: " + finalEvents());
        assertEquals("sales_daily", drift.get().tableName());
        assertNotNull(drift.get().errorMessage(), "Расхождения должны быть описаны");
        assertTrue(drift.get().errorMessage().contains("amount"),
                "В описании должна быть колонка с несовпадающим типом: " + drift.get().errorMessage());
        assertTrue(drift.get().errorMessage().contains("region"),
                "В описании должна быть отсутствующая колонка: " + drift.get().errorMessage());
    }

    @Test
    @DisplayName("Совпадающая схема дрейфом не считается")
    void matchingSchemaShouldNotBeRecordedAsDrift() {
        List<HmsFieldSchemaDto> columns = List.of(
                new HmsFieldSchemaDto("id", "bigint"),
                new HmsFieldSchemaDto("amount", "decimal(10,2)"));

        srcHmsClient.createDatabase(SRC_DB, "hdfs://ns-hot:8020/warehouse/analytics.db");
        srcHmsClient.createTable(table(SRC_DB, "sales_daily", columns,
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));

        dstHmsClient.createDatabase(DST_DB, null);
        dstHmsClient.createTable(table(DST_DB, "sales_daily", columns,
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));

        executor.runBootstrap(job());

        assertTrue(finalEvents().stream().noneMatch(e -> "SCHEMA_DRIFT".equalsIgnoreCase(e.status())),
                "Ложного дрейфа быть не должно: " + finalEvents());
    }

    @Test
    @DisplayName("Новая таблица на приемнике дрейфом не считается")
    void absentTargetTableShouldNotBeRecordedAsDrift() {
        srcHmsClient.createDatabase(SRC_DB, "hdfs://ns-hot:8020/warehouse/analytics.db");
        srcHmsClient.createTable(table(SRC_DB, "sales_daily", List.of(
                new HmsFieldSchemaDto("id", "bigint")),
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));

        executor.runBootstrap(job());

        assertTrue(finalEvents().stream().noneMatch(e -> "SCHEMA_DRIFT".equalsIgnoreCase(e.status())),
                "Для таблицы, которой на приемнике еще нет, сравнивать нечего: " + finalEvents());
    }
}
