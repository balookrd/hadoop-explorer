package org.apache.hadoop.explorer.replicator.hms;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.hms.rewriter.HmsPathRewriter;
import org.apache.hadoop.explorer.replicator.hms.service.HmsTaskExecutor;
import org.apache.hadoop.explorer.replicator.hms.service.HmsTransferServiceImpl;
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
 * Локация, не покрытая сопоставлениями федерации.
 *
 * <p>Результат перезаписи пути несет признак {@code unmapped}, но его никто не читал: при
 * отсутствии сопоставления перезапись возвращает исходный URI, и таблица создавалась в целевом
 * метасторе с локацией кластера-источника. Запросы к такой таблице читают чужой кластер (или
 * падают), а HDFS-подзадача копирует путь сам в себя. Если оператор задал сопоставления, то
 * путь мимо них — ошибка конфигурации, и переносить такую таблицу нельзя.
 */
@DisplayName("HMS: локация без сопоставления федерации не переносится молча")
class HmsUnmappedLocationTest {

    private static final String SRC_DB = "analytics";
    private static final String DST_DB = "analytics_replica";
    private static final String ORPHAN_LOCATION = "hdfs://ns-orphan:8020/warehouse/analytics.db/orphan_table";

    private Server targetGrpcServer;
    private int targetGrpcPort;
    private MockHmsClient srcHmsClient;
    private MockHmsClient dstHmsClient;
    private HmsTaskExecutor executor;

    private final List<HmsProgressReportRequest> reports = new CopyOnWriteArrayList<>();
    private final List<String> subjobPaths = new CopyOnWriteArrayList<>();

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
            public boolean awaitSubjobsCompletion(java.util.Collection<String> subjobIds, long timeoutSeconds) {
                // Копирование данных в этих тестах считается успешным: проверяется метаданная часть
                return true;
            }

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
                subjobPaths.add(srcPath + " -> " + dstPath);
                return "subjob-" + subjobPaths.size();
            }
        };

        // Оператор задал сопоставление только для ns-hot: всё остальное переносить некуда
        HmsPathRewriter pathRewriter = new HmsPathRewriter(
                List.of(new HmsPathRewriter.FederationMapping("ns-hot", "hdfs://ns-target:8020")));

        executor = new HmsTaskExecutor(
                "agent-src-01", "dc1", srcHmsClient, orchestratorClient, pathRewriter,
                false, false, 4, 2);
    }

    @AfterEach
    void tearDown() {
        if (executor != null) executor.close();
        if (targetGrpcServer != null) targetGrpcServer.shutdownNow();
    }

    private HmsPendingJobDto job() {
        return new HmsPendingJobDto("hms-job-unmapped", "dc1", "dc2", SRC_DB, DST_DB, "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L);
    }

    private HmsTableDto table(String name, String location) {
        return new HmsTableDto("hive", SRC_DB, name, "EXTERNAL_TABLE", location,
                Map.of("EXTERNAL", "TRUE"), Collections.emptyList(),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    }

    @Test
    @DisplayName("Таблица с локацией мимо сопоставлений не создается в целевом метасторе")
    void unmappedTableShouldNotBeCreatedWithSourceLocation() {
        srcHmsClient.createDatabase(SRC_DB, "hdfs://ns-hot:8020/warehouse/analytics.db");
        srcHmsClient.createTable(table("orphan_table", ORPHAN_LOCATION));

        boolean ok = executor.runBootstrap(job());

        assertFalse(ok, "Bootstrap с непереносимой локацией должен сообщить о сбое");
        Optional<HmsTableDto> replicated = dstHmsClient.getTable(DST_DB, "orphan_table");
        assertTrue(replicated.isEmpty(),
                "Таблица без сопоставления локации не должна появиться в целевом метасторе; "
                        + "создана с локацией: " + replicated.map(HmsTableDto::sdLocation).orElse("-"));
        assertTrue(subjobPaths.stream().noneMatch(p -> p.equals(ORPHAN_LOCATION + " -> " + ORPHAN_LOCATION)),
                "HDFS-подзадача не должна копировать путь сам в себя: " + subjobPaths);
    }

    @Test
    @DisplayName("Причина отказа видна в отчете о прогрессе")
    void refusalReasonShouldBeReported() {
        srcHmsClient.createDatabase(SRC_DB, "hdfs://ns-hot:8020/warehouse/analytics.db");
        srcHmsClient.createTable(table("orphan_table", ORPHAN_LOCATION));

        executor.runBootstrap(job());

        assertFalse(reports.isEmpty(), "Отчет о прогрессе должен быть отправлен");
        HmsProgressReportRequest finalReport = reports.get(reports.size() - 1);
        assertEquals("ERROR", finalReport.status(), "Итоговый статус задачи — ошибка");
        assertTrue(finalReport.events().stream()
                        .anyMatch(e -> "FAILED".equalsIgnoreCase(e.status())
                                && e.errorMessage() != null && e.errorMessage().contains("ns-orphan")),
                "В журнале событий должна быть причина с указанием непереносимой локации: "
                        + finalReport.events());
    }

    @Test
    @DisplayName("Покрытая сопоставлением таблица переносится с целевой локацией")
    void mappedTableShouldStillBeReplicated() {
        srcHmsClient.createDatabase(SRC_DB, "hdfs://ns-hot:8020/warehouse/analytics.db");
        srcHmsClient.createTable(table("sales_daily", "hdfs://ns-hot:8020/warehouse/analytics.db/sales_daily"));

        boolean ok = executor.runBootstrap(job());

        assertTrue(ok, "Таблица с сопоставленной локацией должна переноситься без ошибок");
        Optional<HmsTableDto> replicated = dstHmsClient.getTable(DST_DB, "sales_daily");
        assertTrue(replicated.isPresent(), "Таблица должна появиться в целевом метасторе");
        assertEquals("hdfs://ns-target:8020/warehouse/analytics.db/sales_daily", replicated.get().sdLocation(),
                "Локация должна быть переписана на целевой кластер");
    }
}
