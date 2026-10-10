package org.apache.hadoop.explorer.replicator.hms.service;

import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.generated.GetMetadataSnapshotResponse;
import org.apache.hadoop.explorer.replicator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.hms.filter.TableSupportFilter;
import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.hms.rewriter.HmsPathRewriter;
import org.apache.hadoop.explorer.replicator.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Исполнитель задач репликации Hive Metastore на стороне Source Agent.
 * <p>
 * Выполняет масштабируемый многопоточный Bootstrap с чанкингом партиций (1000 шт),
 * двусторонний Diff и Reconciliation лишних таблиц/партиций, регистрацию HDFS саб-джоб
 * в Оркестраторе и отправку метаданных через прямой gRPC конвейер на Target Agent.
 */
public class HmsTaskExecutor implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(HmsTaskExecutor.class);

    private final String agentId;
    private final String clusterId;
    private final HmsClient hmsClient;
    private final OrchestratorClient orchestratorClient;
    private final TableSupportFilter tableFilter;
    private final HmsPathRewriter pathRewriter;
    private final ExecutorService bootstrapExecutor;
    private final boolean grpcTlsEnabled;
    private final boolean insecureSkipVerify;
    private final int partitionBatchSize;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public HmsTaskExecutor(
            String agentId,
            String clusterId,
            HmsClient hmsClient,
            OrchestratorClient orchestratorClient,
            boolean grpcTlsEnabled,
            boolean insecureSkipVerify,
            int concurrency,
            int partitionBatchSize
    ) {
        this.agentId = agentId;
        this.clusterId = clusterId;
        this.hmsClient = hmsClient;
        this.orchestratorClient = orchestratorClient;
        this.tableFilter = new TableSupportFilter();
        this.pathRewriter = new HmsPathRewriter();
        this.grpcTlsEnabled = grpcTlsEnabled;
        this.insecureSkipVerify = insecureSkipVerify;
        this.partitionBatchSize = partitionBatchSize > 0 ? partitionBatchSize : 1000;

        int poolSize = concurrency > 0 ? concurrency : 8;
        this.bootstrapExecutor = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "hms-agent-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Выполнение полного начального Bootstrap для задачи.
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

        try (HmsPipelineSender sender = new HmsPipelineSender(job.targetAgentGrpcAddress(), grpcTlsEnabled, insecureSkipVerify)) {
            long highWaterMark = hmsClient.getCurrentNotificationEventId();

            // 1. Создание БД на целевой стороне
            sender.applyDatabase(job.id(), job.targetDbName(), null);

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
                        sender.reconcileExtraneousTables(job.id(), job.targetDbName(), dropTables);

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

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // Итоговый отчет о завершении Bootstrap
            HmsProgressReportRequest finalReport = new HmsProgressReportRequest(
                    "ACTIVE",
                    tables.size(),
                    replicatedTablesCount.get(),
                    totalPartitionsCount.get(),
                    replicatedPartitionsCount.get(),
                    highWaterMark,
                    highWaterMark,
                    0L,
                    String.format("Первичный Bootstrap успешно завершен (таблиц: %d/%d, партиций: %d). Режим CDC активен.",
                            replicatedTablesCount.get(), tables.size(), replicatedPartitionsCount.get()),
                    new ArrayList<>(eventLog),
                    agentId
            );
            orchestratorClient.reportHmsProgress(job.id(), finalReport);

            log.info("[HmsTaskExecutor] Bootstrap для задачи {} успешно завершен: таблиц={}/{}, партиций={}/{}",
                    job.id(), replicatedTablesCount.get(), tables.size(), replicatedPartitionsCount.get(), totalPartitionsCount.get());
            return true;
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

        // 5. Создание/обновление таблицы на целевой стороне через gRPC
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
                table.serdeLib()
        );
        sender.applyTable(job.id(), targetTable);

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
                        sender.reconcileExtraneousPartitions(job.id(), job.targetDbName(), tblName, dropPartitionVals);
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
                            part.parameters()
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
                    sender.applyPartitionBatch(job.id(), job.targetDbName(), tblName, targetChunk);
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

        try (HmsPipelineSender sender = new HmsPipelineSender(job.targetAgentGrpcAddress(), grpcTlsEnabled, insecureSkipVerify)) {
            for (HmsNotificationEventDto event : events) {
                String tbl = event.tableName();
                String eventType = event.eventType();

                if ("ADD_PARTITION".equalsIgnoreCase(eventType) || "ALTER_PARTITION".equalsIgnoreCase(eventType)) {
                    Optional<HmsTableDto> optTable = hmsClient.getTable(job.sourceDbName(), tbl);
                    if (optTable.isPresent()) {
                        List<HmsPartitionDto> parts = hmsClient.getPartitions(job.sourceDbName(), tbl);
                        for (HmsPartitionDto part : parts) {
                            var partRewrite = pathRewriter.rewrite(part.location(), job.sourceClusterId(), job.targetClusterId());
                            String subjobId = orchestratorClient.createHdfsSubjob(job.id(), partRewrite.sourceUri(),
                                    partRewrite.targetUri(), partRewrite.sourceClusterId(), partRewrite.targetClusterId(), job.executionPrincipal());

                            sender.applyPartitionBatch(job.id(), job.targetDbName(), tbl, List.of(
                                    new HmsPartitionDto("hive", job.targetDbName(), tbl, part.values(), partRewrite.targetUri(), part.parameters())
                            ));

                            String partName = part.getPartitionName(optTable.get().partitionKeys());
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, partName,
                                    partRewrite.sourceUri(), partRewrite.targetUri(), subjobId, "APPLIED", null));
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
                                    table.inputFormat(), table.outputFormat(), table.serdeLib()
                            );
                            sender.applyTable(job.id(), targetTable);
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null,
                                    rewrite.sourceUri(), rewrite.targetUri(), subjobId, "APPLIED", null));
                        } else {
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null,
                                    table.sdLocation(), null, null, "SKIPPED_ACID", check.reason()));
                        }
                    }
                } else if ("DROP_TABLE".equalsIgnoreCase(eventType)) {
                    sender.reconcileExtraneousTables(job.id(), job.targetDbName(), List.of(tbl));
                    eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null, null, null, null, "APPLIED", "deleteData=false"));
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
                        sender.reconcileExtraneousPartitions(job.id(), job.targetDbName(), tbl, List.of(partVals));
                        eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, String.join(",", partVals), null, null, null, "APPLIED", "deleteData=false"));
                    }
                }

                lastEventId = event.eventId();
                processed++;
            }

            long currentMax = hmsClient.getCurrentNotificationEventId();
            long lag = Math.max(0, currentMax - lastEventId);

            orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                    "ACTIVE", 0, 0, 0, 0, lastEventId, null, lag,
                    String.format("Синхронизировано %d событий CDC. Текущее отставание: %d событий.", processed, lag),
                    eventLog, agentId
            ));
        } catch (Exception e) {
            log.error("[HmsTaskExecutor] Ошибка при передаче CDC событий: {}", e.getMessage(), e);
        }

        return processed;
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
        if (bootstrapExecutor != null) {
            bootstrapExecutor.shutdown();
        }
    }
}
