package org.apache.hadoop.explorer.replicator.hms.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.generated.GetMetadataSnapshotResponse;
import org.apache.hadoop.explorer.replicator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.hms.filter.TableSupportFilter;
import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.hms.rewriter.HmsPathRewriter;
import org.apache.hadoop.explorer.replicator.model.HmsEventReportDto;
import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
import org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Исполнитель задач репликации схемы и метаданных Hive Metastore на стороне Replicator Agent.
 * Выполняет первичный Bootstrap и потоковую CDC-синхронизацию из NOTIFICATION_LOG.
 */
public class HmsTaskExecutor implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(HmsTaskExecutor.class);

    private final String agentId;
    private final String clusterId;
    private final HmsClient hmsClient;
    private final OrchestratorGrpcClient orchestratorClient;
    private final TableSupportFilter tableFilter;
    private final HmsPathRewriter pathRewriter;
    private final ExecutorService bootstrapExecutor;
    private final boolean grpcTlsEnabled;
    private final boolean insecureSkipVerify;
    private final int partitionBatchSize;
    private final String agentSecret;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, AtomicInteger> syncedCdcEventsPerJob;
    private final ConcurrentMap<String, AtomicInteger> totalPartitionsPerJob;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public HmsTaskExecutor(
            String agentId,
            String clusterId,
            HmsClient hmsClient,
            OrchestratorGrpcClient orchestratorClient,
            HmsPathRewriter pathRewriter
    ) {
        this(agentId, clusterId, hmsClient, orchestratorClient, pathRewriter, false, false, 8, 1000, null);
    }

    public HmsTaskExecutor(
            String agentId,
            String clusterId,
            HmsClient hmsClient,
            OrchestratorGrpcClient orchestratorClient,
            HmsPathRewriter pathRewriter,
            boolean grpcTlsEnabled,
            boolean insecureSkipVerify,
            int bootstrapConcurrency,
            int partitionBatchSize
    ) {
        this(agentId, clusterId, hmsClient, orchestratorClient, pathRewriter, grpcTlsEnabled, insecureSkipVerify,
                bootstrapConcurrency, partitionBatchSize, null);
    }

    public HmsTaskExecutor(
            String agentId,
            String clusterId,
            HmsClient hmsClient,
            OrchestratorGrpcClient orchestratorClient,
            boolean grpcTlsEnabled,
            boolean insecureSkipVerify,
            int bootstrapConcurrency,
            int partitionBatchSize
    ) {
        this(agentId, clusterId, hmsClient, orchestratorClient, new HmsPathRewriter(), grpcTlsEnabled, insecureSkipVerify,
                bootstrapConcurrency, partitionBatchSize, null);
    }

    public HmsTaskExecutor(
            String agentId,
            String clusterId,
            HmsClient hmsClient,
            OrchestratorGrpcClient orchestratorClient,
            HmsPathRewriter pathRewriter,
            boolean grpcTlsEnabled,
            boolean insecureSkipVerify,
            int bootstrapConcurrency,
            int partitionBatchSize,
            String agentSecret
    ) {
        this.agentId = agentId;
        this.clusterId = clusterId;
        this.hmsClient = hmsClient;
        this.orchestratorClient = orchestratorClient;
        this.pathRewriter = pathRewriter != null ? pathRewriter : new HmsPathRewriter();
        this.tableFilter = new TableSupportFilter();
        this.grpcTlsEnabled = grpcTlsEnabled;
        this.insecureSkipVerify = insecureSkipVerify;
        this.partitionBatchSize = partitionBatchSize > 0 ? partitionBatchSize : 1000;
        this.agentSecret = agentSecret;
        this.objectMapper = new ObjectMapper();
        this.syncedCdcEventsPerJob = new ConcurrentHashMap<>();
        this.totalPartitionsPerJob = new ConcurrentHashMap<>();

        int threads = Math.max(1, bootstrapConcurrency);
        this.bootstrapExecutor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "HmsBootstrapWorker-" + UUID.randomUUID().toString().substring(0, 5));
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Выполнение начального создания и переноса таблиц/партиций (Bootstrap).
     */
    public boolean runBootstrap(HmsPendingJobDto job) {
        if (job.targetAgentGrpcAddress() == null || job.targetAgentGrpcAddress().isBlank()) {
            log.error("[HmsTaskExecutor] Задача {}: не указан gRPC адрес целевого агента", job.id());
            return false;
        }

        log.info("[HmsTaskExecutor] Запуск Bootstrap для задачи {} ({}.{} -> {}.{}) через целевой агент {}",
                job.id(), job.sourceClusterId(), job.sourceDbName(), job.targetClusterId(), job.targetDbName(),
                job.targetAgentGrpcAddress());

        orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                "BOOTSTRAPPING", 0, 0, 0, 0, null, null, null,
                "Агент " + agentId + " запустил начальный перенос схемы (Bootstrap)", Collections.emptyList(), agentId
        ));

        try (HmsPipelineSender sender = new HmsPipelineSender(job.targetAgentGrpcAddress(), grpcTlsEnabled, insecureSkipVerify, agentSecret)) {
            long highWaterMark = hmsClient.getCurrentNotificationEventId();

            // 1. Создание БД на целевой стороне и валидация ответа
            var dbResp = sender.applyDatabase(job.id(), job.targetDbName(), null);
            if (!dbResp.getSuccess()) {
                throw new RuntimeException("Целевой агент вернул ошибку при создании БД '" + job.targetDbName() + "': " + dbResp.getMessage());
            }

            List<String> allSourceTables = hmsClient.getAllTables(job.sourceDbName());
            List<String> tables = allSourceTables.stream()
                    .filter(t -> matchesPattern(t, job.tableIncludePattern()))
                    .toList();

            List<HmsEventReportDto> reconciliationEvents = new ArrayList<>();

            // 2. Двусторонний Diff и Reconciliation лишних таблиц
            if (job.dropExtraneousTables()) {
                try {
                    GetMetadataSnapshotResponse snapshot = sender.getMetadataSnapshot(job.id(), job.targetDbName(), "");
                    List<String> targetExistingTables = snapshot.getExistingTablesList();
                    Set<String> sourceTableSet = new HashSet<>(tables);

                    List<String> dropTables = targetExistingTables.stream()
                            .filter(t -> matchesPattern(t, job.tableIncludePattern()) && !sourceTableSet.contains(t))
                            .toList();

                    if (!dropTables.isEmpty()) {
                        log.warn("[HmsTaskExecutor] Reconciliation: обнаружены лишние таблицы на приемнике: {}", dropTables);
                        var dropResp = sender.reconcileExtraneousTables(job.id(), job.targetDbName(), dropTables);
                        if (!dropResp.getSuccess()) {
                            log.error("[HmsTaskExecutor] Ошибка согласования лишних таблиц: {}", dropResp.getMessage());
                        }

                        for (String dt : dropTables) {
                            reconciliationEvents.add(new HmsEventReportDto(
                                     highWaterMark, "BOOTSTRAP_DROP_EXTRA_TABLE", dt, null, null, null, null,
                                    "DROPPED", "Лишняя таблица удалена при начальном согласовании. Данные HDFS сохранены."
                            ));
                        }
                    }
                } catch (Exception e) {
                    log.error("[HmsTaskExecutor] Ошибка Reconciliation таблиц: {}", e.getMessage(), e);
                }
            }

            AtomicInteger replicatedTablesCount = new AtomicInteger(0);
            AtomicInteger replicatedPartitionsCount = new AtomicInteger(0);
            AtomicInteger totalPartitionsCount = new AtomicInteger(0);
            List<HmsEventReportDto> eventLog = Collections.synchronizedList(new ArrayList<>(reconciliationEvents));

            // Фоновый heartbeat-таймер для непрерывного продления аренды (lease renewal) во время тяжелого Bootstrap
            ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "HmsHeartbeat-" + job.id());
                t.setDaemon(true);
                return t;
            });
            heartbeatScheduler.scheduleAtFixedRate(() -> {
                try {
                    orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                            "BOOTSTRAPPING",
                            tables.size(),
                            replicatedTablesCount.get(),
                            totalPartitionsCount.get(),
                            replicatedPartitionsCount.get(),
                            null,
                            highWaterMark,
                            null,
                            String.format("Bootstrap в процессе: перенесено %d/%d таблиц, %d партиций",
                                    replicatedTablesCount.get(), tables.size(), replicatedPartitionsCount.get()),
                            Collections.emptyList(),
                            agentId
                    ));
                } catch (Exception e) {
                    log.warn("[HmsTaskExecutor] Сбой отправки периодического heartbeat для задачи {}: {}", job.id(), e.getMessage());
                }
            }, 5, 5, TimeUnit.SECONDS);

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String tblName : tables) {
                futures.add(CompletableFuture.runAsync(() -> {
                    try {
                        processSingleTable(job, sender, highWaterMark, tblName,
                                replicatedTablesCount, replicatedPartitionsCount, totalPartitionsCount, eventLog);
                    } catch (Exception e) {
                        log.error("[HmsTaskExecutor] Сбой обработки таблицы {}.{}: {}", job.sourceDbName(), tblName, e.getMessage(), e);
                        eventLog.add(new HmsEventReportDto(highWaterMark, "BOOTSTRAP_TABLE", tblName, null,
                                null, null, null, "FAILED", e.getMessage()));
                    }
                }, bootstrapExecutor));
            }

            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            } finally {
                heartbeatScheduler.shutdownNow();
            }

            totalPartitionsPerJob.put(job.id(), totalPartitionsCount);

            boolean hasFailures = eventLog.stream().anyMatch(e -> "FAILED".equalsIgnoreCase(e.status()));
            String finalStatus = hasFailures ? "ERROR" : "ACTIVE";
            String finalMessage = hasFailures
                    ? String.format("Bootstrap завершен с ошибками: успешно %d/%d таблиц, %d партиций. Требуется проверка схем.",
                            replicatedTablesCount.get(), tables.size(), replicatedPartitionsCount.get())
                    : String.format("Первичный Bootstrap успешно завершен (таблиц: %d/%d, партиций: %d). Режим CDC активен.",
                            replicatedTablesCount.get(), tables.size(), replicatedPartitionsCount.get());

            // Итоговый отчет о завершении Bootstrap
            HmsProgressReportRequest finalReport = new HmsProgressReportRequest(
                    finalStatus,
                    tables.size(),
                    replicatedTablesCount.get(),
                    totalPartitionsCount.get(),
                    replicatedPartitionsCount.get(),
                    highWaterMark,
                    highWaterMark,
                    0L,
                    finalMessage,
                    new ArrayList<>(eventLog),
                    agentId
            );
            orchestratorClient.reportHmsProgress(job.id(), finalReport);

            log.info("[HmsTaskExecutor] Bootstrap для задачи {} завершен (status={}): таблиц={}/{}, партиций={}/{}",
                    job.id(), finalStatus, replicatedTablesCount.get(), tables.size(), replicatedPartitionsCount.get(), totalPartitionsCount.get());
            return !hasFailures;
        } catch (Exception e) {
            log.error("[HmsTaskExecutor] Критическая ошибка при исполнении Bootstrap задачи {}: {}", job.id(), e.getMessage(), e);
            orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                    "FAILED", 0, 0, 0, 0, null, null, null,
                    "Сбой выполнения Bootstrap агентом: " + e.getMessage(), Collections.emptyList(), agentId
            ));
            return false;
        }
    }

    private void processSingleTable(
            HmsPendingJobDto job,
            HmsPipelineSender sender,
            long highWaterMark,
            String tblName,
            AtomicInteger repTables,
            AtomicInteger repPartitions,
            AtomicInteger totalPartitions,
            List<HmsEventReportDto> eventLog
    ) {
        if (!running.get()) return;
        Optional<HmsTableDto> optTable = hmsClient.getTable(job.sourceDbName(), tblName);
        if (optTable.isEmpty()) return;
        HmsTableDto table = optTable.get();

        // 1. Проверка поддержки Non-ACID
        var filterResult = tableFilter.evaluate(table);
        if (!filterResult.supported()) {
            log.info("[HmsTaskExecutor] Пропуск таблицы {}.{}: {}", job.sourceDbName(), tblName, filterResult.reason());
            eventLog.add(new HmsEventReportDto(highWaterMark, "BOOTSTRAP_TABLE", tblName, null,
                    table.sdLocation(), null, null, "SKIPPED_ACID", filterResult.reason()));
            return;
        }

        // 2. Трансляция пути корня таблицы с учетом федерации HDFS
        var rewrite = pathRewriter.rewrite(table.sdLocation(), job.sourceClusterId(), job.targetClusterId());

        // 3. Создание 1 корневой саб-джобы HDFS передачи файлов через Оркестратор!
        String tableSubjobId = orchestratorClient.createHdfsSubjob(job.id(), rewrite.sourceUri(), rewrite.targetUri(),
                rewrite.sourceClusterId(), rewrite.targetClusterId(), job.executionPrincipal());

        // 4. Очистка вендорных параметров HDP 3.1
        Map<String, String> cleanParams = tableFilter.sanitizeParameters(table.parameters());

        // 5. Создание/обновление таблицы на целевой стороне через gRPC с полной схемой колонок и SerDe
        HmsTableDto targetTable = new HmsTableDto(
                "hive",
                job.targetDbName(),
                tblName,
                table.tableType(),
                rewrite.targetUri(),
                cleanParams,
                table.partitionKeys(),
                table.inputFormat(),
                table.outputFormat(),
                table.serdeLib(),
                table.columns(),
                table.partitionKeyFields(),
                table.serdeParameters()
        );
        var tableResp = sender.applyTable(job.id(), targetTable);
        if (!tableResp.getSuccess()) {
            throw new RuntimeException("Целевой агент вернул ошибку при создании/обновлении таблицы " + tblName + ": " + tableResp.getMessage());
        }

        // 6. Обработка партиций
        if (table.isPartitioned()) {
            List<HmsPartitionDto> allPartitions = hmsClient.getPartitions(job.sourceDbName(), tblName);
            totalPartitions.addAndGet(allPartitions.size());

            // Reconciliation партиций:
            if (job.dropExtraneousPartitions()) {
                try {
                    GetMetadataSnapshotResponse partSnapshot = sender.getMetadataSnapshot(job.id(), job.targetDbName(), tblName);
                    Set<List<String>> srcPartitionValues = allPartitions.stream()
                            .map(HmsPartitionDto::values)
                            .collect(Collectors.toSet());

                    List<List<String>> dropPartitionVals = new ArrayList<>();
                    for (var targetPart : partSnapshot.getExistingPartitionsList()) {
                        List<String> vals = targetPart.getValuesList();
                        if (!srcPartitionValues.contains(vals)) {
                            dropPartitionVals.add(vals);
                        }
                    }

                    if (!dropPartitionVals.isEmpty()) {
                        var partDropResp = sender.reconcileExtraneousPartitions(job.id(), job.targetDbName(), tblName, dropPartitionVals);
                        if (!partDropResp.getSuccess()) {
                            log.warn("[HmsTaskExecutor] Сбой согласования партиций: {}", partDropResp.getMessage());
                        }
                        for (List<String> dVals : dropPartitionVals) {
                            String partName = String.join("/", dVals);
                            eventLog.add(new HmsEventReportDto(
                                    highWaterMark, "BOOTSTRAP_DROP_EXTRA_PARTITION", tblName, partName,
                                    null, null, null, "DROPPED",
                                    "Лишняя партиция удалена при начальном согласовании. Данные HDFS сохранены."
                            ));
                        }
                    }
                } catch (Exception e) {
                    log.error("[HmsTaskExecutor] Ошибка Reconciliation партиций таблицы {}: {}", tblName, e.getMessage());
                }
            }

            String tableBaseLocation = table.sdLocation();

            // Чанкинг партиций по 1000 шт
            for (int i = 0; i < allPartitions.size(); i += partitionBatchSize) {
                int toIndex = Math.min(i + partitionBatchSize, allPartitions.size());
                List<HmsPartitionDto> chunk = allPartitions.subList(i, toIndex);
                List<HmsPartitionDto> targetChunk = new ArrayList<>(chunk.size());

                for (HmsPartitionDto part : chunk) {
                    var partRewrite = pathRewriter.rewrite(part.location(), job.sourceClusterId(), job.targetClusterId());
                    String partSubjobId = tableSubjobId;

                    if (!isLocationWithinTable(part.location(), tableBaseLocation)) {
                        partSubjobId = orchestratorClient.createHdfsSubjob(job.id(), partRewrite.sourceUri(),
                                partRewrite.targetUri(), partRewrite.sourceClusterId(), partRewrite.targetClusterId(), job.executionPrincipal());
                    }

                    targetChunk.add(new HmsPartitionDto(
                            "hive",
                            job.targetDbName(),
                            tblName,
                            part.values(),
                            partRewrite.targetUri(),
                            part.parameters(),
                            part.columns(),
                            part.serdeParameters()
                    ));

                    if (allPartitions.size() <= 100 || !isLocationWithinTable(part.location(), tableBaseLocation)) {
                        String partName = part.getPartitionName(table.partitionKeys());
                        eventLog.add(new HmsEventReportDto(
                                highWaterMark, "BOOTSTRAP_PARTITION", tblName, partName,
                                partRewrite.sourceUri(), partRewrite.targetUri(), partSubjobId, "APPLIED", null
                        ));
                    }
                }

                if (!targetChunk.isEmpty()) {
                    var chunkResp = sender.applyPartitionBatch(job.id(), job.targetDbName(), tblName, targetChunk);
                    if (!chunkResp.getSuccess()) {
                        throw new RuntimeException("Целевой агент вернул ошибку при пакетной вставке партиций: " + chunkResp.getMessage());
                    }
                }

                if (allPartitions.size() > 100) {
                    eventLog.add(new HmsEventReportDto(
                            highWaterMark, "BOOTSTRAP_PARTITION_CHUNK", tblName,
                            String.format("Батч партиций [%d..%d] из %d", i + 1, toIndex, allPartitions.size()),
                            null, null, tableSubjobId, "APPLIED",
                            String.format("Пакетно реплицировано %d партиций", targetChunk.size())
                    ));
                }

                repPartitions.addAndGet(chunk.size());
            }
        }

        eventLog.add(new HmsEventReportDto(
                highWaterMark, "BOOTSTRAP_TABLE", tblName, null,
                rewrite.sourceUri(), rewrite.targetUri(), tableSubjobId, "APPLIED", null
        ));
        repTables.incrementAndGet();
    }

    /**
     * Потоковая синхронизация CDC событий из локального NOTIFICATION_LOG.
     */
    public int pollAndSyncCdc(HmsPendingJobDto job) {
        if (job.targetAgentGrpcAddress() == null || job.targetAgentGrpcAddress().isBlank()) {
            return 0;
        }

        long lastEventId = job.lastProcessedEventId() != null ? job.lastProcessedEventId() : 0L;
        List<HmsNotificationEventDto> events = hmsClient.getNextNotifications(lastEventId, 50);
        if (events.isEmpty()) {
            return 0;
        }

        List<HmsEventReportDto> eventLog = new ArrayList<>();
        int processed = 0;

        long lastCommittedEventId = lastEventId;
        boolean batchHasFailures = false;

        try (HmsPipelineSender sender = new HmsPipelineSender(job.targetAgentGrpcAddress(), grpcTlsEnabled, insecureSkipVerify, agentSecret)) {
            for (HmsNotificationEventDto event : events) {
                if (!running.get()) break;
                // Фильтрация событий по исходной базе данных (NOTIFICATION_LOG глобален для всего метастора)
                if (event.dbName() != null && !event.dbName().equalsIgnoreCase(job.sourceDbName())) {
                    lastCommittedEventId = event.eventId();
                    continue;
                }

                String tbl = event.tableName();
                String eventType = event.eventType();
                boolean eventSuccess = true;
                String failureReason = null;

                if ("ADD_PARTITION".equalsIgnoreCase(eventType) || "ALTER_PARTITION".equalsIgnoreCase(eventType)) {
                    Optional<HmsTableDto> optTable = hmsClient.getTable(job.sourceDbName(), tbl);
                    if (optTable.isPresent()) {
                        List<List<String>> targetPartVals = extractPartitionValuesFromEvent(event);
                        List<HmsPartitionDto> partsToSync = new ArrayList<>();

                        if (!targetPartVals.isEmpty()) {
                            for (List<String> partVal : targetPartVals) {
                                hmsClient.getPartition(job.sourceDbName(), tbl, partVal).ifPresent(partsToSync::add);
                            }
                        } else {
                            // Если парсинг values не дал результатов, берем только последнюю добавленную партицию (фолбэк-дельта)
                            List<HmsPartitionDto> allParts = hmsClient.getPartitions(job.sourceDbName(), tbl);
                            if (!allParts.isEmpty()) {
                                partsToSync.add(allParts.get(allParts.size() - 1));
                            }
                        }

                        for (HmsPartitionDto part : partsToSync) {
                            var partRewrite = pathRewriter.rewrite(part.location(), job.sourceClusterId(), job.targetClusterId());
                            String subjobId = orchestratorClient.createHdfsSubjob(job.id(), partRewrite.sourceUri(),
                                    partRewrite.targetUri(), partRewrite.sourceClusterId(), partRewrite.targetClusterId(), job.executionPrincipal());

                            var partResp = sender.applyPartitionBatch(job.id(), job.targetDbName(), tbl, List.of(
                                    new HmsPartitionDto("hive", job.targetDbName(), tbl, part.values(), partRewrite.targetUri(),
                                            part.parameters(), part.columns(), part.serdeParameters())
                            ));

                            String partName = part.getPartitionName(optTable.get().partitionKeys());
                            boolean ok = partResp.getSuccess();
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, partName,
                                    partRewrite.sourceUri(), partRewrite.targetUri(), subjobId,
                                    ok ? "APPLIED" : "FAILED", partResp.getMessage()));

                            if (!ok) {
                                eventSuccess = false;
                                failureReason = partResp.getMessage();
                                break;
                            }
                        }
                    }
                } else if ("CREATE_TABLE".equalsIgnoreCase(eventType)) {
                    Optional<HmsTableDto> optTable = hmsClient.getTable(job.sourceDbName(), tbl);
                    if (optTable.isPresent()) {
                        HmsTableDto table = optTable.get();
                        var check = tableFilter.evaluate(table);
                        if (check.supported()) {
                            var rewrite = pathRewriter.rewrite(table.sdLocation(), job.sourceClusterId(), job.targetClusterId());
                            String subjobId = orchestratorClient.createHdfsSubjob(job.id(), rewrite.sourceUri(),
                                    rewrite.targetUri(), job.sourceClusterId(), job.targetClusterId(), job.executionPrincipal());

                            HmsTableDto targetTable = new HmsTableDto(
                                    "hive", job.targetDbName(), tbl, table.tableType(), rewrite.targetUri(),
                                    tableFilter.sanitizeParameters(table.parameters()), table.partitionKeys(),
                                    table.inputFormat(), table.outputFormat(), table.serdeLib(),
                                    table.columns(), table.partitionKeyFields(), table.serdeParameters()
                            );
                            var tblResp = sender.applyTable(job.id(), targetTable);
                            boolean ok = tblResp.getSuccess();
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null,
                                    rewrite.sourceUri(), rewrite.targetUri(), subjobId,
                                    ok ? "APPLIED" : "FAILED", tblResp.getMessage()));

                            if (!ok) {
                                eventSuccess = false;
                                failureReason = tblResp.getMessage();
                            }
                        } else {
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null,
                                    table.sdLocation(), null, null, "SKIPPED_ACID", check.reason()));
                        }
                    }
                } else if ("DROP_TABLE".equalsIgnoreCase(eventType)) {
                    var dropTblResp = sender.reconcileExtraneousTables(job.id(), job.targetDbName(), List.of(tbl));
                    boolean ok = dropTblResp.getSuccess();
                    eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null, null, null, null,
                            ok ? "APPLIED" : "FAILED", "deleteData=false"));

                    if (!ok) {
                        eventSuccess = false;
                        failureReason = dropTblResp.getMessage();
                    }
                } else if ("DROP_PARTITION".equalsIgnoreCase(eventType)) {
                    List<String> partVals = Collections.emptyList();
                    try {
                        if (event.message() != null && event.message().contains("\"values\"")) {
                            Map<?, ?> map = objectMapper.readValue(event.message(), Map.class);
                            Object v = map.get("values");
                            if (v instanceof List<?> l) {
                                partVals = l.stream().map(Object::toString).toList();
                            }
                        }
                    } catch (Exception ignored) {}
                    if (!partVals.isEmpty()) {
                        var dropPartResp = sender.reconcileExtraneousPartitions(job.id(), job.targetDbName(), tbl, List.of(partVals));
                        boolean ok = dropPartResp.getSuccess();
                        eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, String.join(",", partVals), null, null, null,
                                ok ? "APPLIED" : "FAILED", "deleteData=false"));

                        if (!ok) {
                            eventSuccess = false;
                            failureReason = dropPartResp.getMessage();
                        }
                    }
                }

                if (eventSuccess) {
                    lastCommittedEventId = event.eventId();
                    processed++;
                } else {
                    log.error("[HmsTaskExecutor] Сбой применения события CDC id={}, тип={}, таблица={}: {}. Остановка пачки для повтора.",
                            event.eventId(), eventType, tbl, failureReason);
                    batchHasFailures = true;
                    break;
                }
            }

            long currentMax = hmsClient.getCurrentNotificationEventId();
            long lag = Math.max(0, currentMax - lastCommittedEventId);

            List<String> currentTables = Collections.emptyList();
            try {
                currentTables = hmsClient.getAllTables(job.sourceDbName()).stream()
                        .filter(t -> matchesPattern(t, job.tableIncludePattern()))
                        .toList();
            } catch (Exception e) {
                log.warn("[HmsTaskExecutor] Не удалось получить актуальные таблицы схемы {}: {}", job.sourceDbName(), e.getMessage());
            }

            int curPartCount = totalPartitionsPerJob.computeIfAbsent(job.id(), k -> new AtomicInteger(0)).get();

            // Накопительный счетчик обработанных CDC событий схемы
            AtomicInteger counter = syncedCdcEventsPerJob.computeIfAbsent(job.id(), k -> new AtomicInteger(0));
            int cumulativeEvents = (processed > 0) ? counter.addAndGet(processed) : counter.get();

            String statusMessage = batchHasFailures
                    ? String.format("Приостановка CDC на событии %d из-за ошибки. Отставание: %d событий.", lastCommittedEventId, lag)
                    : String.format("Синхронизировано %d событий CDC. Текущее отставание: %d событий.",
                            cumulativeEvents > 0 ? cumulativeEvents : processed, lag);

            orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                    "ACTIVE", currentTables.size(), currentTables.size(), curPartCount, curPartCount, lastCommittedEventId, null, lag,
                    statusMessage,
                    eventLog, agentId
            ));
        } catch (Exception e) {
            log.error("[HmsTaskExecutor] Ошибка при передаче CDC событий: {}", e.getMessage(), e);
        }

        return processed;
    }

    private List<List<String>> extractPartitionValuesFromEvent(HmsNotificationEventDto event) {
        if (event == null || event.message() == null || event.message().isBlank()) {
            return Collections.emptyList();
        }
        try {
            Map<?, ?> map = objectMapper.readValue(event.message(), Map.class);
            List<List<String>> result = new ArrayList<>();
            Object values = map.get("values");
            if (values instanceof List<?> valList) {
                result.add(valList.stream().map(Object::toString).toList());
                return result;
            }
            Object parts = map.get("partitions");
            if (parts instanceof List<?> pList) {
                for (Object item : pList) {
                    if (item instanceof Map<?, ?> pMap) {
                        Object pVals = pMap.get("values");
                        if (pVals instanceof List<?> pvList) {
                            result.add(pvList.stream().map(Object::toString).toList());
                        }
                    }
                }
                if (!result.isEmpty()) return result;
            }
            Object partObj = map.get("partition");
            if (partObj instanceof Map<?, ?> pMap) {
                Object pVals = pMap.get("values");
                if (pVals instanceof List<?> pvList) {
                    result.add(pvList.stream().map(Object::toString).toList());
                    return result;
                }
            }
            Object partVals = map.get("partitionValues");
            if (partVals instanceof List<?> pvList) {
                for (Object item : pvList) {
                    if (item instanceof List<?> l) {
                        result.add(l.stream().map(Object::toString).toList());
                    } else if (item != null) {
                        result.add(List.of(item.toString()));
                    }
                }
                if (!result.isEmpty()) return result;
            }
        } catch (Exception ignored) {}
        return Collections.emptyList();
    }

    private boolean isLocationWithinTable(String partLocation, String tableBaseLocation) {
        if (partLocation == null || tableBaseLocation == null) return false;
        String cleanTable = tableBaseLocation.replaceAll("/+$", "");
        String cleanPart = partLocation.replaceAll("/+$", "");
        return cleanPart.startsWith(cleanTable + "/");
    }

    private boolean matchesPattern(String tableName, String pattern) {
        if (pattern == null || pattern.isBlank() || pattern.equals("*") || pattern.equals(".*")) return true;
        try {
            if (tableName.matches(pattern)) return true;
        } catch (Exception ignored) {}
        String regex = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".");
        return tableName.matches(regex);
    }

    @Override
    public void close() {
        running.set(false);
        if (bootstrapExecutor != null) {
            bootstrapExecutor.shutdownNow();
        }
    }
}
