package org.apache.hadoop.explorer.replicator.hms;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
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
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class HmsTaskExecutorTest {

    private Server targetGrpcServer;
    private int targetGrpcPort;
    private MockHmsClient srcHmsClient;
    private MockHmsClient dstHmsClient;
    private HmsTaskExecutor executor;

    private final List<HmsProgressReportRequest> reports = new CopyOnWriteArrayList<>();
    private final List<String> createdSubjobs = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setup() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            targetGrpcPort = socket.getLocalPort();
        }

        srcHmsClient = new MockHmsClient("dc1", "HDP_3.1");
        dstHmsClient = new MockHmsClient("dc2", "APACHE_3.1.3");

        // Поднимаем gRPC сервер целевого агента
        HmsTransferServiceImpl transferService = new HmsTransferServiceImpl(dstHmsClient);
        targetGrpcServer = NettyServerBuilder.forPort(targetGrpcPort)
                .addService(transferService)
                .build()
                .start();

        // Тестовый mock-клиент Оркестратора
        OrchestratorClient mockOrchestratorClient = new OrchestratorClient("http://localhost:8005", "secret") {
            @Override
            public boolean reportHmsProgress(String jobId, HmsProgressReportRequest report) {
                reports.add(report);
                return true;
            }

            @Override
            public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl) {
                String subId = "subjob-" + createdSubjobs.size();
                createdSubjobs.add(subId);
                return subId;
            }
        };

        executor = new HmsTaskExecutor(
                "agent-src-01",
                "dc1",
                srcHmsClient,
                mockOrchestratorClient,
                false,
                false,
                4,
                2 // малый размер чанка для проверки батчинга партиций
        );
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.close();
        }
        if (targetGrpcServer != null) {
            targetGrpcServer.shutdownNow();
        }
    }

    @Test
    @DisplayName("Сквозной тест HmsTaskExecutor: Масштабируемый Bootstrap, чанкинг, саб-джобы, Non-ACID шлюз и CDC")
    void testHmsTaskExecutorFullPipeline() {
        String srcDb = "analytics";
        String dstDb = "analytics_replica";

        // 1. Инициализация данных источника
        srcHmsClient.createDatabase(srcDb, "hdfs://ns-hot:8020/warehouse/analytics.db");

        // 1.1 Таблица 1: EXTERNAL с 3 партициями
        srcHmsClient.createTable(new HmsTableDto(
                "hive", srcDb, "sales_daily", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/analytics.db/sales_daily",
                Map.of("EXTERNAL", "TRUE", "hdp.version", "3.1.0"),
                List.of("dt"),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        ));
        srcHmsClient.addPartitions(srcDb, "sales_daily", List.of(
                new HmsPartitionDto("hive", srcDb, "sales_daily", List.of("2026-10-08"), "hdfs://ns-hot:8020/warehouse/analytics.db/sales_daily/dt=2026-10-08", Map.of()),
                new HmsPartitionDto("hive", srcDb, "sales_daily", List.of("2026-10-09"), "hdfs://ns-hot:8020/warehouse/analytics.db/sales_daily/dt=2026-10-09", Map.of()),
                new HmsPartitionDto("hive", srcDb, "sales_daily", List.of("2026-10-10"), "hdfs://ns-cold:8020/warehouse/analytics.db/sales_daily/dt=2026-10-10", Map.of())
        ));

        // 1.2 Таблица 2: MANAGED Non-ACID
        srcHmsClient.createTable(new HmsTableDto(
                "hive", srcDb, "dim_customers", "MANAGED_TABLE",
                "hdfs://ns-hot:8020/warehouse/analytics.db/dim_customers",
                Map.of("EXTERNAL", "FALSE"),
                Collections.emptyList(),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        ));

        // 1.3 Таблица 3: ACID (должна быть отсеяна фильтром)
        srcHmsClient.createTable(new HmsTableDto(
                "hive", srcDb, "acid_orders", "MANAGED_TABLE",
                "hdfs://ns-hot:8020/warehouse/analytics.db/acid_orders",
                Map.of("transactional", "true"),
                Collections.emptyList(),
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
        ));

        // Подготовка лишних объектов на приёмнике для проверки Reconciliation
        dstHmsClient.createDatabase(dstDb, null);
        dstHmsClient.createTable(new HmsTableDto(
                "hive", dstDb, "obsolete_table", "EXTERNAL_TABLE", "/loc", Map.of(), List.of(), "i", "o", "s"
        ));
        dstHmsClient.createTable(new HmsTableDto(
                "hive", dstDb, "sales_daily", "EXTERNAL_TABLE",
                "hdfs://ns-target:8020/warehouse/analytics.db/sales_daily",
                Map.of(), List.of("dt"), "i", "o", "s"
        ));
        dstHmsClient.addPartitions(dstDb, "sales_daily", List.of(
                new HmsPartitionDto("hive", dstDb, "sales_daily", List.of("1999-01-01"), "/loc", Map.of())
        ));

        HmsPendingJobDto job = new HmsPendingJobDto(
                "hms-job-test-1",
                "dc1",
                "dc2",
                srcDb,
                dstDb,
                "*",
                true, // dropExtraneousTables
                true, // dropExtraneousPartitions
                "127.0.0.1:" + targetGrpcPort,
                "QUEUED",
                0L
        );

        // 2. Исполнение Bootstrap
        boolean ok = executor.runBootstrap(job);
        assertTrue(ok, "Bootstrap должен завершиться успешно");

        // 3. Проверка метаданных на целевом HMS
        assertTrue(dstHmsClient.getAllDatabases().contains(dstDb));
        List<String> dstTables = dstHmsClient.getAllTables(dstDb);
        assertTrue(dstTables.contains("sales_daily"));
        assertTrue(dstTables.contains("dim_customers"));
        assertFalse(dstTables.contains("acid_orders"), "ACID таблица не должна быть перенесена");
        assertFalse(dstTables.contains("obsolete_table"), "Лишняя таблица должна быть удалена Reconciliation");

        // Проверка партиций
        List<HmsPartitionDto> dstParts = dstHmsClient.getPartitions(dstDb, "sales_daily");
        assertEquals(3, dstParts.size());
        assertFalse(dstParts.stream().anyMatch(p -> p.values().contains("1999-01-01")), "Лишняя партиция удалена");

        // Проверка саб-джоб HDFS:
        // Созданы корневые саб-джобы для sales_daily и dim_customers, плюс отдельная саб-джоба для холодной партиции ns-cold
        assertTrue(createdSubjobs.size() >= 2);

        // Проверка отчетов о прогрессе: финальный статус ACTIVE
        assertFalse(reports.isEmpty());
        HmsProgressReportRequest finalReport = reports.get(reports.size() - 1);
        assertEquals("ACTIVE", finalReport.status());
        assertEquals(2, finalReport.replicatedTables());
        assertEquals(3, finalReport.replicatedPartitions());

        // 4. Проверка CDC
        srcHmsClient.addPartitions(srcDb, "sales_daily", List.of(
                new HmsPartitionDto("hive", srcDb, "sales_daily", List.of("2026-10-11"),
                        "hdfs://ns-hot:8020/warehouse/analytics.db/sales_daily/dt=2026-10-11", Map.of())
        ));
        srcHmsClient.emitEvent("ADD_PARTITION", srcDb, "sales_daily", "{}");

        int processedCdc = executor.pollAndSyncCdc(new HmsPendingJobDto(
                job.id(), job.sourceClusterId(), job.targetClusterId(), job.sourceDbName(), job.targetDbName(),
                job.tableIncludePattern(), false, false, job.targetAgentGrpcAddress(), "ACTIVE", finalReport.lastProcessedEventId()
        ));

        assertEquals(1, processedCdc, "Должно быть обработано 1 событие CDC");
        assertEquals(4, dstHmsClient.getPartitions(dstDb, "sales_daily").size(), "Новая партиция должна появиться на целевой стороне");
    }
}
