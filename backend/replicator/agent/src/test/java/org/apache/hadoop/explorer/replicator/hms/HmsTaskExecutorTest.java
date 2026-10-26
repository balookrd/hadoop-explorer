package org.apache.hadoop.explorer.replicator.hms;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
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
    private final List<String> awaitedSubjobs = new CopyOnWriteArrayList<>();
    private final List<String> freshSubjobs = new CopyOnWriteArrayList<>();
    /** Сколько подзадач ждал каждый вызов барьера. */
    private final List<Integer> awaitCallSizes = new CopyOnWriteArrayList<>();

    /** Файловая система приемника: каталог -> имена файлов (листинг и удаление по gRPC). */
    private final java.util.Map<String, Set<String>> targetFs = new java.util.concurrent.ConcurrentHashMap<>();
    private final org.apache.hadoop.explorer.replicator.generated.DataTransferServiceGrpc.DataTransferServiceImplBase targetFiles =
            new org.apache.hadoop.explorer.replicator.generated.DataTransferServiceGrpc.DataTransferServiceImplBase() {
                @Override
                public void getDirectoryManifest(org.apache.hadoop.explorer.replicator.generated.DirectoryManifestRequest request,
                                                 io.grpc.stub.StreamObserver<org.apache.hadoop.explorer.replicator.generated.DirectoryManifestResponse> obs) {
                    Set<String> files = targetFs.get(request.getPath());
                    var b = org.apache.hadoop.explorer.replicator.generated.DirectoryManifestResponse.newBuilder().setExists(files != null);
                    if (files != null) {
                        files.forEach(f -> b.addFiles(org.apache.hadoop.explorer.replicator.generated.FileManifestEntry.newBuilder().setRelativePath(f)));
                    }
                    obs.onNext(b.build());
                    obs.onCompleted();
                }

                @Override
                public void deletePath(org.apache.hadoop.explorer.replicator.generated.DeletePathRequest request,
                                       io.grpc.stub.StreamObserver<org.apache.hadoop.explorer.replicator.generated.DeletePathResponse> obs) {
                    String path = request.getPath();
                    int slash = path.lastIndexOf('/');
                    Set<String> files = targetFs.get(path.substring(0, slash));
                    boolean removed = files != null && files.remove(path.substring(slash + 1));
                    obs.onNext(org.apache.hadoop.explorer.replicator.generated.DeletePathResponse.newBuilder().setSuccess(removed).build());
                    obs.onCompleted();
                }
            };
    /** Подзадачи данных, копирование которых завершилось неуспехом. */
    private final Set<String> failedSubjobs = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** Оркестратор отказывает в подзадачах данных: задача удалена. */
    private volatile boolean rejectSubjobs;
    /** Оркестратор отклоняет отчеты: задачу отобрали у агента. */
    private volatile boolean rejectReports;

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
                .addService(targetFiles)
                .build()
                .start();

        // Тестовый mock-клиент Оркестратора
        OrchestratorGrpcClient mockOrchestratorClient = new OrchestratorGrpcClient() {
            @Override
            public boolean reportHmsProgress(String jobId, HmsProgressReportRequest report) {
                reports.add(report);
                return !rejectReports;
            }

            @Override
            public String createFreshHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl, String executionPrincipal) {
                String subId = "fresh-" + freshSubjobs.size();
                freshSubjobs.add(srcPath);
                return subId;
            }

            @Override
            public boolean awaitSubjobsCompletion(java.util.Collection<String> subjobIds, long timeoutSeconds) {
                awaitedSubjobs.addAll(subjobIds);
                awaitCallSizes.add(subjobIds.size());
                return subjobIds.stream().noneMatch(failedSubjobs::contains);
            }

            @Override
            public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl) {
                return createHdfsSubjob(hmsJobId, srcPath, dstPath, srcCl, dstCl, null);
            }

            @Override
            public String createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl, String executionPrincipal) {
                if (rejectSubjobs) {
                    throw new OrchestratorGrpcClient.SubjobRejectedException(hmsJobId);
                }
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

        // 4. Проверка CDC: сообщение Hive 3
        srcHmsClient.addPartitions(srcDb, "sales_daily", List.of(
                new HmsPartitionDto("hive", srcDb, "sales_daily", List.of("2026-10-11"),
                        "hdfs://ns-hot:8020/warehouse/analytics.db/sales_daily/dt=2026-10-11", Map.of())
        ));
        // Сообщение в формате Hive 3 (json-0.2): партиции — карты ключ -> значение
        srcHmsClient.emitEvent("ADD_PARTITION", srcDb, "sales_daily", "{\"partitions\": [{\"dt\": \"2026-10-11\"}]}");

        int processedCdc = executor.pollAndSyncCdc(new HmsPendingJobDto(
                job.id(), job.sourceClusterId(), job.targetClusterId(), job.sourceDbName(), job.targetDbName(),
                job.tableIncludePattern(), false, false, job.targetAgentGrpcAddress(), "ACTIVE", finalReport.lastProcessedEventId()
        ));

        assertEquals(1, processedCdc, "Должно быть обработано 1 событие CDC");
        assertEquals(4, dstHmsClient.getPartitions(dstDb, "sales_daily").size(), "Новая партиция должна появиться на целевой стороне");

        // 5. Проверка целевой CDC дельта-обработки с JSON partitionValues
        srcHmsClient.addPartitions(srcDb, "sales_daily", List.of(
                new HmsPartitionDto("hive", srcDb, "sales_daily", List.of("2026-10-12"),
                        "hdfs://ns-hot:8020/warehouse/analytics.db/sales_daily/dt=2026-10-12", Map.of()),
                new HmsPartitionDto("hive", srcDb, "sales_daily", List.of("2026-10-13"),
                        "hdfs://ns-hot:8020/warehouse/analytics.db/sales_daily/dt=2026-10-13", Map.of())
        ));
        // Эмитируем событие с дельтой только для 2026-10-12
        srcHmsClient.emitEvent("ADD_PARTITION", srcDb, "sales_daily", "{\"partitionValues\": [[\"2026-10-12\"]]}");

        long lastEventIdAfterFirstCdc = reports.get(reports.size() - 1).lastProcessedEventId();
        int processedDelta = executor.pollAndSyncCdc(new HmsPendingJobDto(
                job.id(), job.sourceClusterId(), job.targetClusterId(), job.sourceDbName(), job.targetDbName(),
                job.tableIncludePattern(), false, false, job.targetAgentGrpcAddress(), "ACTIVE", lastEventIdAfterFirstCdc
        ));

        assertEquals(1, processedDelta, "Должно быть обработано дельта CDC событие");
        List<HmsPartitionDto> targetParts = dstHmsClient.getPartitions(dstDb, "sales_daily");
        assertEquals(5, targetParts.size(), "Должна быть добавлена только партиция 2026-10-12 из дельты");
        assertTrue(targetParts.stream().anyMatch(p -> p.values().contains("2026-10-12")));
        assertFalse(targetParts.stream().anyMatch(p -> p.values().contains("2026-10-13")),
                "Партиция 2026-10-13 не была в дельта-событии и не должна пока присутствовать на целевой стороне");
    }

    @Test
    @DisplayName("Bootstrap переносит формат хранения партиций: Parquet не превращается в текст")
    void bootstrapKeepsPartitionStorageFormat() {
        String parquetIn = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
        String parquetOut = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
        String parquetSerde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
        srcHmsClient.createDatabase("fmt", "hdfs://ns-hot:8020/warehouse/fmt.db");
        srcHmsClient.createTable(new HmsTableDto(
                "hive", "fmt", "events", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/fmt.db/events",
                Map.of("EXTERNAL", "TRUE"), List.of("dt"), parquetIn, parquetOut, parquetSerde));
        srcHmsClient.addPartitions("fmt", "events", List.of(new HmsPartitionDto(
                "hive", "fmt", "events", List.of("2026-10-01"),
                "hdfs://ns-hot:8020/warehouse/fmt.db/events/dt=2026-10-01",
                Map.of("COLUMN_STATS_ACCURATE", "{\"BASIC_STATS\":\"true\"}", "numRows", "5"), List.of(), Map.of("serialization.format", "1"),
                parquetIn, parquetOut, parquetSerde)));

        boolean ok = executor.runBootstrap(new HmsPendingJobDto(
                "hms-job-fmt", "dc1", "dc2", "fmt", "fmt_replica", "*", false, false,
                "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L));

        assertTrue(ok, "Bootstrap должен завершиться успешно");
        HmsPartitionDto replicated = dstHmsClient.getPartitions("fmt_replica", "events").get(0);
        assertEquals(parquetIn, replicated.inputFormat());
        assertEquals(parquetOut, replicated.outputFormat());
        assertEquals(parquetSerde, replicated.serdeLib());
        assertEquals("1", replicated.serdeParameters().get("serialization.format"));
        assertFalse(replicated.parameters().containsKey("COLUMN_STATS_ACCURATE"),
                "флаг точной статистики без самой статистики на приемник не переносится");
    }

    @Test
    @DisplayName("Bootstrap отправляет журнал порциями: ни один отчет не несет больше 500 событий")
    void bootstrapReportsEventsInBoundedBatches() {
        srcHmsClient.createDatabase("big", "hdfs://ns-hot:8020/warehouse/big.db");
        srcHmsClient.createTable(new HmsTableDto(
                "hive", "big", "clicks", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/big.db/clicks",
                Map.of("EXTERNAL", "TRUE"), List.of("dt"), "i", "o", "s"));
        List<HmsPartitionDto> parts = new ArrayList<>();
        for (int i = 0; i < 1200; i++) {
            // Партиции вне корня таблицы: на каждую пишется отдельное событие
            parts.add(new HmsPartitionDto("hive", "big", "clicks", List.of("p" + i),
                    "hdfs://ns-cold:8020/archive/clicks/dt=p" + i, Map.of()));
        }
        srcHmsClient.addPartitions("big", "clicks", parts);

        boolean ok = executor.runBootstrap(new HmsPendingJobDto(
                "hms-job-big", "dc1", "dc2", "big", "big_replica", "*", false, false,
                "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L));

        assertTrue(ok, "Bootstrap должен завершиться успешно");
        int maxEvents = reports.stream().mapToInt(r -> r.events().size()).max().orElse(0);
        assertTrue(maxEvents <= 500, "в одном отчете " + maxEvents + " событий");
        long partitionEvents = reports.stream().flatMap(r -> r.events().stream())
                .filter(e -> "BOOTSTRAP_PARTITION".equals(e.eventType())).count();
        assertEquals(1200, partitionEvents, "порционная отправка не должна терять события");
        assertEquals("ACTIVE", reports.get(reports.size() - 1).status());
    }

    @Test
    @DisplayName("Fencing: отказ оркестратора прерывает bootstrap, итоговый ACTIVE не отправляется")
    void rejectedReportStopsBootstrap() {
        srcHmsClient.createDatabase("fenced", "hdfs://ns-hot:8020/warehouse/fenced.db");
        srcHmsClient.createTable(new HmsTableDto(
                "hive", "fenced", "clicks", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/fenced.db/clicks",
                Map.of("EXTERNAL", "TRUE"), List.of("dt"), "i", "o", "s"));
        List<HmsPartitionDto> parts = new ArrayList<>();
        for (int i = 0; i < 1200; i++) {
            parts.add(new HmsPartitionDto("hive", "fenced", "clicks", List.of("p" + i),
                    "hdfs://ns-cold:8020/archive/clicks/dt=p" + i, Map.of()));
        }
        srcHmsClient.addPartitions("fenced", "clicks", parts);
        rejectReports = true;

        boolean ok = executor.runBootstrap(new HmsPendingJobDto(
                "hms-job-fenced", "dc1", "dc2", "fenced", "fenced_replica", "*", false, false,
                "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L));

        assertFalse(ok, "отобранная задача не должна считаться успешно перенесенной");
        assertTrue(reports.stream().noneMatch(r -> "ACTIVE".equals(r.status())),
                "итоговый ACTIVE от агента без аренды недопустим");
        int replicated = dstHmsClient.getPartitions("fenced_replica", "clicks").size();
        assertTrue(replicated < 1200, "перенос должен остановиться после отказа, перенесено " + replicated);
    }

    @Test
    @DisplayName("Data barrier: подзадачи данных порции ждутся разом, неуспех копии отменяет DDL партиций")
    void dataBarrierWaitsForChunkAndBlocksDdlOnFailure() {
        srcHmsClient.createDatabase("bar", "hdfs://ns-hot:8020/warehouse/bar.db");
        srcHmsClient.createTable(new HmsTableDto(
                "hive", "bar", "clicks", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/bar.db/clicks",
                Map.of("EXTERNAL", "TRUE"), List.of("dt"), "i", "o", "s"));
        srcHmsClient.addPartitions("bar", "clicks", List.of(
                new HmsPartitionDto("hive", "bar", "clicks", List.of("a"), "hdfs://ns-cold:8020/archive/clicks/dt=a", Map.of()),
                new HmsPartitionDto("hive", "bar", "clicks", List.of("b"), "hdfs://ns-cold:8020/archive/clicks/dt=b", Map.of())));
        // subjob-0 — корень таблицы, он скопирован; партиции вне корня — нет
        failedSubjobs.addAll(List.of("subjob-1", "subjob-2"));

        boolean ok = executor.runBootstrap(new HmsPendingJobDto(
                "hms-job-bar", "dc1", "dc2", "bar", "bar_replica", "*", false, false,
                "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L));

        assertFalse(ok, "неуспешная копия данных — неуспешный bootstrap");
        assertTrue(awaitedSubjobs.containsAll(List.of("subjob-1", "subjob-2")),
                "обе подзадачи порции должны ждаться вместе: " + awaitedSubjobs);
        assertTrue(dstHmsClient.getPartitions("bar_replica", "clicks").isEmpty(),
                "партиции без скопированных данных не должны появиться на приемнике");
    }

    @Test
    @DisplayName("Разрыв журнала событий: CDC не продолжается молча, задача уходит в ERROR")
    void eventGapMovesJobToError() {
        srcHmsClient.createDatabase("gap", "hdfs://ns-hot:8020/warehouse/gap.db");
        for (int i = 0; i < 5; i++) {
            srcHmsClient.emitEvent("CREATE_TABLE", "gap", "t" + i, "{}");
        }
        long newest = srcHmsClient.getCurrentNotificationEventId();
        // Агент остановился на событии newest-5, а метастор уже удалил newest-4..newest-1 —
        // в журнале осталось только последнее событие
        long lastProcessed = newest - 5;
        srcHmsClient.dropEventsUpTo(newest - 1);

        int processed = executor.pollAndSyncCdc(new HmsPendingJobDto(
                "hms-job-gap", "dc1", "dc2", "gap", "gap_replica", "*", false, false,
                "127.0.0.1:" + targetGrpcPort, "ACTIVE", lastProcessed));

        assertEquals(0, processed, "события за разрывом не применяются");
        HmsProgressReportRequest last = reports.get(reports.size() - 1);
        assertEquals("ERROR", last.status());
        assertTrue(last.message().contains("bootstrap"), last.message());
    }

    @Test
    @DisplayName("Fencing подзадач: удаленная задача не применяет остаток пачки CDC")
    void rejectedSubjobStopsCdcBatch() {
        srcHmsClient.createDatabase("del", "hdfs://ns-hot:8020/warehouse/del.db");
        srcHmsClient.createTable(new HmsTableDto("hive", "del", "t", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/del.db/t", Map.of("EXTERNAL", "TRUE"), List.of("dt"), "i", "o", "s"));
        long before = srcHmsClient.getCurrentNotificationEventId();
        List<HmsPartitionDto> parts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            parts.add(new HmsPartitionDto("hive", "del", "t", List.of("p" + i), "hdfs://ns-cold:8020/x/dt=p" + i, Map.of()));
        }
        srcHmsClient.addPartitions("del", "t", parts);
        for (int i = 0; i < 5; i++) {
            srcHmsClient.emitEvent("ADD_PARTITION", "del", "t", "{\"partitions\": [{\"dt\": \"p" + i + "\"}]}");
        }
        rejectSubjobs = true;

        int processed = executor.pollAndSyncCdc(new HmsPendingJobDto(
                "hms-job-deleted", "dc1", "dc2", "del", "del_replica", "*", false, false,
                "127.0.0.1:" + targetGrpcPort, "ACTIVE", before));

        assertEquals(0, processed);
        assertTrue(dstHmsClient.getAllDatabases().stream().noneMatch("del_replica"::equals)
                        || dstHmsClient.getPartitions("del_replica", "t").isEmpty(),
                "партиции удаленной задачи не должны применяться");
    }

    @Test
    @DisplayName("Имена партиций приемника отдаются постранично, а не одним сообщением")
    void targetPartitionNamesArePaged() {
        dstHmsClient.createDatabase("paged", null);
        dstHmsClient.createTable(new HmsTableDto("hive", "paged", "t", "EXTERNAL_TABLE", "/t",
                Map.of(), List.of("dt"), "i", "o", "s"));
        List<HmsPartitionDto> parts = new ArrayList<>();
        for (int i = 0; i < 12_000; i++) {
            parts.add(new HmsPartitionDto("hive", "paged", "t", List.of("p" + i), "/t/dt=p" + i, Map.of()));
        }
        dstHmsClient.addPartitions("paged", "t", parts);

        try (var sender = new org.apache.hadoop.explorer.replicator.hms.service.HmsPipelineSender(
                "127.0.0.1:" + targetGrpcPort, false, false)) {
            List<String> names = sender.listTargetPartitionNames("job", "paged", "t", null);
            assertEquals(12_000, names.size());
            assertEquals(12_000, new HashSet<>(names).size(), "страницы не должны пересекаться");

            var schemaOnly = sender.getTableSchemaSnapshot("job", "paged", "t", null);
            assertEquals(0, schemaOnly.getExistingPartitionsCount(), "проверке дрейфа партиции не нужны");
            assertEquals(1, schemaOnly.getExistingTableSchemasCount());
        }
    }

    @Test
    @DisplayName("RENAME таблицы: приемник переименовывает таблицу и сохраняет ее партиции")
    void renameKeepsTargetPartitions() {
        srcHmsClient.createDatabase("ren", "hdfs://ns-hot:8020/warehouse/ren.db");
        srcHmsClient.createTable(new HmsTableDto("hive", "ren", "orders", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/ren.db/orders", Map.of("EXTERNAL", "TRUE"), List.of("dt"), "i", "o", "s"));
        srcHmsClient.addPartitions("ren", "orders", List.of(new HmsPartitionDto("hive", "ren", "orders",
                List.of("d1"), "hdfs://ns-hot:8020/warehouse/ren.db/orders/dt=d1", Map.of())));
        assertTrue(executor.runBootstrap(new HmsPendingJobDto("hms-job-ren", "dc1", "dc2", "ren", "ren_replica", "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L)));
        long position = reports.get(reports.size() - 1).lastProcessedEventId();

        // Источник: таблица переименована, событие несет имена до и после
        srcHmsClient.renameTable("ren", "orders", new HmsTableDto("hive", "ren", "orders_v2", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/ren.db/orders", Map.of("EXTERNAL", "TRUE"), List.of("dt"), "i", "o", "s"));
        srcHmsClient.emitEvent("ALTER_TABLE", "ren", "orders", "{\"tableObjBeforeJson\":\"{\\\"1\\\":{\\\"str\\\":\\\"orders\\\"}}\","
                + "\"tableObjAfterJson\":\"{\\\"1\\\":{\\\"str\\\":\\\"orders_v2\\\"}}\"}");

        executor.pollAndSyncCdc(new HmsPendingJobDto("hms-job-ren", "dc1", "dc2", "ren", "ren_replica", "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "ACTIVE", position));

        List<String> tables = dstHmsClient.getAllTables("ren_replica");
        assertTrue(tables.contains("orders_v2"), "новое имя на приемнике: " + tables);
        assertFalse(tables.contains("orders"), "старого имени быть не должно: " + tables);
        assertEquals(1, dstHmsClient.getPartitions("ren_replica", "orders_v2").size(), "партиции сохраняются");
    }

    private void insertSetup() {
        srcHmsClient.createDatabase("ins", "hdfs://dc1-nn:8020/warehouse/ins.db");
        srcHmsClient.createTable(new HmsTableDto("hive", "ins", "orders", "EXTERNAL_TABLE",
                "hdfs://dc1-nn:8020/warehouse/ins.db/orders", Map.of("EXTERNAL", "TRUE"), List.of("dt"), "i", "o", "s"));
        srcHmsClient.addPartitions("ins", "orders", List.of(new HmsPartitionDto("hive", "ins", "orders",
                List.of("d1"), "hdfs://dc1-nn:8020/warehouse/ins.db/orders/dt=d1", Map.of("numRows", "10"))));
        assertTrue(executor.runBootstrap(new HmsPendingJobDto("hms-job-ins", "dc1", "dc2", "ins", "ins_replica", "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L)));
    }

    private static String insertMessage(boolean replace, String... files) {
        StringBuilder f = new StringBuilder();
        for (String file : files) {
            if (f.length() > 0) f.append(',');
            f.append("\"hdfs://dc1-nn:8020/warehouse/ins.db/orders/dt=d1/").append(file).append("#cksum#\"");
        }
        return "{\"replace\":\"" + replace + "\",\"ptnObjJson\":\"{\\\"1\\\":{\\\"lst\\\":[\\\"str\\\",1,\\\"d1\\\"]}}\","
                + "\"files\":[" + f + "]}";
    }

    @Test
    @DisplayName("INSERT INTO: свежая копия каталога партиции и обновленные метаданные на приемнике")
    void insertIntoCopiesPartitionAndRefreshesMetadata() {
        insertSetup();
        long position = reports.get(reports.size() - 1).lastProcessedEventId();
        srcHmsClient.addPartitions("ins", "orders", List.of(new HmsPartitionDto("hive", "ins", "orders",
                List.of("d1"), "hdfs://dc1-nn:8020/warehouse/ins.db/orders/dt=d1", Map.of("numRows", "25"))));
        srcHmsClient.emitEvent("INSERT", "ins", "orders", insertMessage(false, "000000_0", "000001_0"));

        int processed = executor.pollAndSyncCdc(new HmsPendingJobDto("hms-job-ins", "dc1", "dc2", "ins", "ins_replica", "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "ACTIVE", position));

        assertEquals(1, processed);
        assertEquals(List.of("hdfs://dc1-nn:8020/warehouse/ins.db/orders/dt=d1"), freshSubjobs,
                "каталог партиции копируется новой подзадачей, а не переиспользованной");
        assertEquals("25", dstHmsClient.getPartitions("ins_replica", "orders").get(0).parameters().get("numRows"),
                "статистика партиции на приемнике обновлена");
    }

    @Test
    @DisplayName("INSERT OVERWRITE: устаревшие файлы партиции на приемнике удаляются")
    void insertOverwritePrunesStaleTargetFiles() {
        insertSetup();
        long position = reports.get(reports.size() - 1).lastProcessedEventId();
        String targetDir = "hdfs://dc2-nn:8020/warehouse/ins.db/orders/dt=d1";
        targetFs.put(targetDir, java.util.concurrent.ConcurrentHashMap.newKeySet());
        targetFs.get(targetDir).addAll(List.of("000000_0", "000001_0", "000002_0"));
        srcHmsClient.emitEvent("INSERT", "ins", "orders", insertMessage(true, "000000_0"));

        executor.pollAndSyncCdc(new HmsPendingJobDto("hms-job-ins", "dc1", "dc2", "ins", "ins_replica", "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "ACTIVE", position));

        assertEquals(Set.of("000000_0"), targetFs.get(targetDir), "после перезаписи на приемнике только новые файлы");
    }

    @Test
    @DisplayName("CDC с барьером: данные партиций всей пачки ждутся одним ожиданием, а не по одной")
    void cdcBatchWaitsForAllPartitionDataAtOnce() {
        srcHmsClient.createDatabase("cdcb", "hdfs://ns-hot:8020/warehouse/cdcb.db");
        srcHmsClient.createTable(new HmsTableDto("hive", "cdcb", "t", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/cdcb.db/t", Map.of("EXTERNAL", "TRUE"), List.of("dt"), "i", "o", "s"));
        assertTrue(executor.runBootstrap(new HmsPendingJobDto("hms-job-cdcb", "dc1", "dc2", "cdcb", "cdcb_r", "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L)));
        long position = reports.get(reports.size() - 1).lastProcessedEventId();
        for (int i = 0; i < 5; i++) {
            srcHmsClient.addPartitions("cdcb", "t", List.of(new HmsPartitionDto("hive", "cdcb", "t", List.of("p" + i),
                    "hdfs://ns-hot:8020/warehouse/cdcb.db/t/dt=p" + i, Map.of())));
            srcHmsClient.emitEvent("ADD_PARTITION", "cdcb", "t", "{\"partitions\": [{\"dt\": \"p" + i + "\"}]}");
        }
        awaitCallSizes.clear();

        int processed = executor.pollAndSyncCdc(new HmsPendingJobDto("hms-job-cdcb", "dc1", "dc2", "cdcb", "cdcb_r", "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "ACTIVE", position));

        assertEquals(5, processed);
        assertEquals(List.of(5), awaitCallSizes, "одно ожидание на пять копий");
        assertEquals(5, dstHmsClient.getPartitions("cdcb_r", "t").size());
    }

    @Test
    @DisplayName("Продолжение bootstrap по чекпоинту: перенесенное не повторяется, счетчики полные")
    void bootstrapResumesFromCheckpoint() {
        srcHmsClient.createDatabase("res", "hdfs://ns-hot:8020/warehouse/res.db");
        srcHmsClient.createTable(new HmsTableDto("hive", "res", "small", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/res.db/small", Map.of("EXTERNAL", "TRUE"), List.of(), "i", "o", "s"));
        srcHmsClient.createTable(new HmsTableDto("hive", "res", "big", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/res.db/big", Map.of("EXTERNAL", "TRUE"), List.of("dt"), "i", "o", "s"));
        List<HmsPartitionDto> parts = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            String v = String.format("p%04d", i);
            parts.add(new HmsPartitionDto("hive", "res", "big", List.of(v), "hdfs://ns-hot:8020/warehouse/res.db/big/dt=" + v, Map.of()));
        }
        srcHmsClient.addPartitions("res", "big", parts);
        String checkpoint = "{\"done\":{\"small\":0},\"partial\":{\"big\":\"dt=p0599\"}}";

        boolean ok = executor.runBootstrap(new HmsPendingJobDto("hms-job-res", "dc1", "dc2", "res", "res_r", "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "QUEUED", 0L, null, null, true, null, null,
                777L, checkpoint));

        assertTrue(ok);
        assertFalse(dstHmsClient.getAllTables("res_r").contains("small"), "перенесенная таблица не трогается");
        List<HmsPartitionDto> applied = dstHmsClient.getPartitions("res_r", "big");
        assertEquals(400, applied.size(), "применяются только партиции после чекпоинта");
        assertTrue(applied.stream().allMatch(p -> p.values().get(0).compareTo("p0599") > 0));
        HmsProgressReportRequest last = reports.get(reports.size() - 1);
        assertEquals("ACTIVE", last.status());
        assertEquals(777L, last.bootstrapEventId(), "позиция журнала — исходная, а не текущая");
        assertEquals(2, last.replicatedTables());
        assertEquals(1000, last.replicatedPartitions());
    }
}
