package org.apache.hadoop.explorer.replicator.hms.service;

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
import org.apache.hadoop.explorer.replicator.model.HmsSchemaDriftReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Исполнитель задач репликации схемы и метаданных Hive Metastore на стороне Replicator Agent.
 * Выполняет первичный Bootstrap и потоковую CDC-синхронизацию из NOTIFICATION_LOG.
 */
public class HmsTaskExecutor implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(HmsTaskExecutor.class);

    /** Поле журнала задачи ограничено 2048 символами — длинный список расхождений обрезается. */
    private static final int MAX_DRIFT_MESSAGE_LENGTH = 2000;

    private final String agentId;
    private final String clusterId;
    private final HmsClient hmsClient;
    private final OrchestratorGrpcClient orchestratorClient;
    private final TableSupportFilter tableFilter;
    private final HmsSchemaDriftDetector driftDetector = new HmsSchemaDriftDetector();
    private final HmsPathRewriter pathRewriter;
    private final ExecutorService bootstrapExecutor;
    private final boolean grpcTlsEnabled;
    private final boolean insecureSkipVerify;
    private final int partitionBatchSize;
    private final String agentSecret;
    private final ConcurrentMap<String, AtomicInteger> syncedCdcEventsPerJob;
    /** Каналы к агентам приемников, переживающие отдельный опрос CDC. */
    private final ConcurrentMap<String, HmsPipelineSender> cdcSenders;
    private final ConcurrentMap<String, AtomicInteger> totalPartitionsPerJob;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // Расширения надежности и консистентности:
    /** Неудачные попытки события по ключу «задача:событие»: журнал метастора общий для задач. */
    private final ConcurrentMap<String, AtomicInteger> eventFailureCounts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicBoolean> jobLagSynchronized = new ConcurrentHashMap<>();
    private final int maxEventRetries;
    private final boolean dataBarrierEnabled;
    private final long dataBarrierTimeoutSeconds;
    private final ConcurrentMap<String, AtomicBoolean> jobFencedFlags = new ConcurrentHashMap<>();

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
        this(agentId, clusterId, hmsClient, orchestratorClient, pathRewriter, grpcTlsEnabled, insecureSkipVerify,
                bootstrapConcurrency, partitionBatchSize, agentSecret, true, 60L, 3);
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
            String agentSecret,
            boolean dataBarrierEnabled,
            long dataBarrierTimeoutSeconds,
            int maxEventRetries
    ) {
        this.agentId = agentId;
        this.clusterId = clusterId;
        this.hmsClient = hmsClient;
        this.orchestratorClient = orchestratorClient;
        if (orchestratorClient != null) {
            orchestratorClient.setAgentId(agentId);
        }
        this.pathRewriter = pathRewriter != null ? pathRewriter : new HmsPathRewriter();
        this.tableFilter = new TableSupportFilter();
        this.grpcTlsEnabled = grpcTlsEnabled;
        this.insecureSkipVerify = insecureSkipVerify;
        this.partitionBatchSize = partitionBatchSize > 0 ? partitionBatchSize : 1000;
        this.agentSecret = agentSecret;
        this.dataBarrierEnabled = dataBarrierEnabled;
        this.dataBarrierTimeoutSeconds = dataBarrierTimeoutSeconds > 0 ? dataBarrierTimeoutSeconds : 60L;
        this.maxEventRetries = maxEventRetries > 0 ? maxEventRetries : 3;
        this.syncedCdcEventsPerJob = new ConcurrentHashMap<>();
        this.cdcSenders = new ConcurrentHashMap<>();
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
    /** Типы событий CDC, которые перенос схем выполняет. */
    private static final Set<String> HANDLED_CDC_EVENTS = Set.of(
            "ADD_PARTITION", "ALTER_PARTITION", "CREATE_TABLE", "ALTER_TABLE", "DROP_TABLE", "DROP_PARTITION", "INSERT");

    private static boolean handledEventType(String eventType) {
        return eventType != null && HANDLED_CDC_EVENTS.contains(eventType.toUpperCase());
    }

    /**
     * Сравнение схемы таблицы с фактической схемой на приемнике до применения DDL.
     *
     * <p>Сравнивать можно только здесь: агент источника единственный, кто видит и свой метастор,
     * и метастор приемника (через gRPC). После применения таблицы совпадают по построению, так
     * что интересен именно момент «до»: ручная правка схемы на приемнике или отставание от
     * источника. Расхождение не останавливает перенос — оно записывается в журнал задачи, откуда
     * оркестратор собирает отчет о дрейфе.
     */
    private void recordSchemaDriftIfAny(HmsPendingJobDto job, HmsPipelineSender sender, String tblName,
                                        HmsTableDto expectedTable, long highWaterMark,
                                        List<HmsEventReportDto> eventLog) {
        try {
            GetMetadataSnapshotResponse snapshot = sender.getTableSchemaSnapshot(
                    job.id(), job.targetDbName(), tblName, job.executionPrincipal());

            Optional<HmsTableDto> actualTable = snapshot.getExistingTableSchemasList().stream()
                    .map(HmsConversionHelper::toDto)
                    .filter(t -> tblName.equalsIgnoreCase(t.tableName()))
                    .findFirst();
            if (actualTable.isEmpty()) {
                // Таблицы на приемнике еще нет — расхождение не с чем фиксировать
                return;
            }

            var report = driftDetector.detectDrift(expectedTable, actualTable.get());
            if (!report.hasDrift()) {
                return;
            }

            String discrepancies = HmsSchemaDriftReport.joinDiscrepancies(report.discrepancies());
            if (discrepancies.length() > MAX_DRIFT_MESSAGE_LENGTH) {
                discrepancies = discrepancies.substring(0, MAX_DRIFT_MESSAGE_LENGTH - 3) + "...";
            }
            eventLog.add(new HmsEventReportDto(highWaterMark, "SCHEMA_DRIFT", tblName, null,
                    expectedTable.sdLocation(), actualTable.get().sdLocation(), null,
                    HmsSchemaDriftReport.EVENT_STATUS, discrepancies));
        } catch (Exception e) {
            // Обнаружение дрейфа — наблюдение, а не часть переноса: его сбой не должен ронять задачу
            log.warn("[HmsTaskExecutor] Не удалось сравнить схему таблицы {}.{} с приемником: {}",
                    job.targetDbName(), tblName, e.getMessage());
        }
    }

    /**
     * Перезапись локации объекта HMS с отказом на пути мимо заданных сопоставлений.
     *
     * <p>Признак {@code unmapped} означает, что перезапись вернула исходный URI. Раньше его
     * никто не читал: объект создавался в целевом метасторе с локацией кластера-источника, а
     * HDFS-подзадача копировала путь сам в себя. Если оператор задал сопоставления федерации
     * или целевую ФС по умолчанию, путь мимо них — ошибка конфигурации, и переносить такой
     * объект нельзя. Без заданных правил сквозной перенос ожидаем (одинаковое пространство
     * имен на обеих сторонах) и отмечается предупреждением.
     */
    private HmsPathRewriter.PathRewriteResult rewriteLocation(String sourceLocation, HmsPendingJobDto job, String what) {
        var rewrite = pathRewriter.rewrite(sourceLocation, job.sourceClusterId(), job.targetClusterId());
        if (!rewrite.unmapped()) {
            return rewrite;
        }
        if (pathRewriter.hasExplicitMappings()) {
            throw new IllegalStateException(String.format(
                    "Локация %s '%s' не покрыта правилами перезаписи: перенос создал бы объект в целевом "
                            + "метасторе с путем кластера-источника. Добавьте сопоставление федерации "
                            + "(REPLICATOR_FEDERATION_MAPPINGS) или задайте целевую ФС по умолчанию.",
                    what, sourceLocation));
        }
        log.warn("[HmsTaskExecutor] Локация {} '{}' переносится без перезаписи: правила перезаписи не заданы, "
                        + "предполагается одинаковое пространство имен на источнике и приемнике",
                what, sourceLocation);
        return rewrite;
    }

    public boolean runBootstrap(HmsPendingJobDto job) {
        if (job.targetAgentGrpcAddress() == null || job.targetAgentGrpcAddress().isBlank()) {
            log.error("[HmsTaskExecutor] Задача {}: не указан gRPC адрес целевого агента", job.id());
            return false;
        }

        // Запуск пришел из опроса, то есть аренда сейчас у этого агента: отказ, полученный
        // прошлым прогоном, к нему уже не относится. Флаг создается заранее: обработчики таблиц
        // берут его один раз в начале и должны видеть отказ, пришедший посреди переноса
        jobFencedFlags.put(job.id(), new AtomicBoolean(false));

        log.info("[HmsTaskExecutor] Запуск Bootstrap для задачи {} ({}.{} -> {}.{}) через целевой агент {}",
                job.id(), job.sourceClusterId(), job.sourceDbName(), job.targetClusterId(), job.targetDbName(),
                job.targetAgentGrpcAddress());

        orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                "BOOTSTRAPPING", 0, 0, 0, 0, null, null, null,
                "Агент " + agentId + " запустил начальный перенос схемы (Bootstrap)", Collections.emptyList(), agentId
        ));

        try (HmsPipelineSender sender = newPipelineSender(job.targetAgentGrpcAddress())) {
            long highWaterMark = hmsClient.getCurrentNotificationEventId();

            // 1. Создание БД на целевой стороне и валидация ответа
            var dbResp = sender.applyDatabase(job.id(), job.targetDbName(), null, job.executionPrincipal());
            if (!dbResp.getSuccess()) {
                throw new RuntimeException("Целевой агент вернул ошибку при создании БД '" + job.targetDbName() + "': " + dbResp.getMessage());
            }

            // 1.1. Автоматический запуск связанного фонового HDFS Inotify-стримера для директории базы данных warehouse
            if (job.enableHdfsStreaming()) {
                try {
                    Optional<String> dbLocOpt = hmsClient.getDatabaseLocation(job.sourceDbName());
                    if (dbLocOpt.isPresent() && !dbLocOpt.get().isBlank()) {
                        var dbRewrite = rewriteLocation(dbLocOpt.get(), job, "базы данных");
                        String companionId = orchestratorClient.createHdfsSubjob(
                                job.id(),
                                dbRewrite.sourceUri(),
                                dbRewrite.targetUri(),
                                job.sourceClusterId(),
                                job.targetClusterId(),
                                job.executionPrincipal(),
                                "STREAMING_INOTIFY",
                                "HMS_COMPANION_STREAMING"
                        );
                        if (companionId != null) {
                            log.info("[HmsTaskExecutor] Задача {}: успешно зарегистрирован связанный HDFS Inotify стример '{}' для warehouse '{}' -> '{}'",
                                    job.id(), companionId, dbRewrite.sourceUri(), dbRewrite.targetUri());
                        }
                    }
                } catch (Exception e) {
                    log.warn("[HmsTaskExecutor] Задача {}: не удалось автоматически запустить Companion HDFS стример: {}",
                            job.id(), e.getMessage());
                }
            }

            List<String> allSourceTables = hmsClient.getAllTables(job.sourceDbName());
            List<String> tables = allSourceTables.stream()
                    .filter(t -> matchesPattern(t, job.tableIncludePattern()))
                    .toList();

            List<HmsEventReportDto> reconciliationEvents = new ArrayList<>();

            // 2. Двусторонний Diff и Reconciliation лишних таблиц
            if (job.dropExtraneousTables()) {
                try {
                    GetMetadataSnapshotResponse snapshot = sender.getMetadataSnapshot(job.id(), job.targetDbName(), "", job.executionPrincipal());
                    List<String> targetExistingTables = snapshot.getExistingTablesList();
                    Set<String> sourceTableSet = new HashSet<>(tables);

                    List<String> dropTables = targetExistingTables.stream()
                            .filter(t -> matchesPattern(t, job.tableIncludePattern()) && !sourceTableSet.contains(t))
                            .toList();

                    if (!dropTables.isEmpty()) {
                        log.warn("[HmsTaskExecutor] Reconciliation: обнаружены лишние таблицы на приемнике: {}", dropTables);
                        var dropResp = sender.reconcileExtraneousTables(job.id(), job.targetDbName(), dropTables, job.executionPrincipal());
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
            BootstrapEventLog eventLog = new BootstrapEventLog(BootstrapEventLog.DEFAULT_FLUSH_THRESHOLD,
                    batch -> {
                        boolean accepted = orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                                "BOOTSTRAPPING",
                                tables.size(),
                                replicatedTablesCount.get(),
                                totalPartitionsCount.get(),
                                replicatedPartitionsCount.get(),
                                null,
                                highWaterMark,
                                null,
                                null,
                                batch,
                                agentId
                        ));
                        if (!accepted) {
                            log.warn("[HmsTaskExecutor] Fencing: оркестратор отклонил журнал bootstrap задачи {}, bootstrap прерывается", job.id());
                            jobFencedFlags.computeIfAbsent(job.id(), k -> new AtomicBoolean(false)).set(true);
                        }
                    });
            reconciliationEvents.forEach(eventLog::add);

            // Фоновый heartbeat-таймер для непрерывного продления аренды (lease renewal) во время тяжелого Bootstrap
            ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "HmsHeartbeat-" + job.id());
                t.setDaemon(true);
                return t;
            });
            heartbeatScheduler.scheduleAtFixedRate(() -> {
                try {
                    boolean accepted = orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
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
                    if (!accepted) {
                        // Задачу отобрали (failover, остановка, rebootstrap): дальше работает
                        // другой агент, и продолжение здесь означало бы двойную запись в приемник
                        log.warn("[HmsTaskExecutor] Fencing: оркестратор отклонил heartbeat задачи {}, bootstrap прерывается", job.id());
                        jobFencedFlags.computeIfAbsent(job.id(), k -> new AtomicBoolean(false)).set(true);
                    }
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
                    } catch (OrchestratorGrpcClient.SubjobRejectedException e) {
                        // Задачу удалили или отобрали: остальные таблицы этого bootstrap не нужны
                        log.warn("[HmsTaskExecutor] Fencing: {}. Bootstrap задачи {} прерывается", e.getMessage(), job.id());
                        jobFencedFlags.computeIfAbsent(job.id(), k -> new AtomicBoolean(false)).set(true);
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

            AtomicBoolean fenced = jobFencedFlags.get(job.id());
            if (fenced != null && fenced.get()) {
                // Задачу отобрали посреди bootstrap: итоговый ACTIVE от этого агента был бы ложью
                // о частично перенесенной схеме, ее доведет до конца новый владелец
                log.warn("[HmsTaskExecutor] Bootstrap задачи {} прерван fencing'ом, итоговый отчет не отправляется", job.id());
                return false;
            }

            totalPartitionsPerJob.put(job.id(), totalPartitionsCount);

            boolean hasFailures = eventLog.hasFailures();
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
                    eventLog.drain(),
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

    /** Остановка обхода партиций таблицы по stop/fencing изнутри обработчика порции. */
    private static final class TableInterruptedException extends RuntimeException {
        TableInterruptedException() {
            super(null, null, false, false);
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
    ) throws Exception {
        AtomicBoolean isFenced = jobFencedFlags.get(job.id());
        if (!running.get() || (isFenced != null && isFenced.get())) {
            log.warn("[HmsTaskExecutor] Пропуск обработки таблицы {}.{} задачи {} по сигналу Fencing/Stop",
                    job.sourceDbName(), tblName, job.id());
            return;
        }

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
        var rewrite = rewriteLocation(table.sdLocation(), job, "таблицы " + tblName);

        // 3. Создание 1 корневой саб-джобы HDFS передачи файлов через Оркестратор!
        String tableSubjobId = orchestratorClient.createHdfsSubjob(job.id(), rewrite.sourceUri(), rewrite.targetUri(),
                rewrite.sourceClusterId(), rewrite.targetClusterId(), job.executionPrincipal());

        // Data-First Consistency Barrier: ожидание физической готовности данных в HDFS перед DDL в целевой HMS
        if (dataBarrierEnabled && tableSubjobId == null) {
            // Без подзадачи проверять нечего: DDL без данных — ровно то, от чего барьер защищает
            throw new RuntimeException(String.format(
                    "Data-First Barrier: оркестратор не создал подзадачу копирования данных таблицы '%s'. DDL отменен.",
                    tblName));
        }
        if (dataBarrierEnabled) {
            boolean dataReady = orchestratorClient.awaitSubjobCompletion(tableSubjobId, dataBarrierTimeoutSeconds);
            if (!dataReady) {
                throw new RuntimeException(String.format(
                        "Data-First Barrier: HDFS подзадача копирования данных таблицы '%s' (%s) не завершена вовремя. DDL отменен.",
                        tblName, tableSubjobId));
            }
        }

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
        // 5.1 Фиксация дрейфа схемы: что на приемнике было до применения DDL
        recordSchemaDriftIfAny(job, sender, tblName, targetTable, highWaterMark, eventLog);

        var tableResp = sender.applyTable(job.id(), targetTable, job.executionPrincipal());
        if (!tableResp.getSuccess()) {
            throw new RuntimeException("Целевой агент вернул ошибку при создании/обновлении таблицы " + tblName + ": " + tableResp.getMessage());
        }

        // 6. Обработка партиций
        if (table.isPartitioned()) {
            String tableBaseLocation = table.sdLocation();
            // Значения партиций источника нужны только для согласования лишних партиций приемника:
            // список самих партиций в памяти не держится, обход идет порциями
            Set<List<String>> srcPartitionValues = job.dropExtraneousPartitions() ? new HashSet<>() : null;
            AtomicInteger offset = new AtomicInteger(0);
            try {
                hmsClient.forEachPartitionBatch(job.sourceDbName(), tblName, partitionBatchSize, (chunk, total) -> {
                    if (!running.get() || (isFenced != null && isFenced.get())) {
                        throw new TableInterruptedException();
                    }
                    int from = offset.getAndAdd(chunk.size());
                    if (from == 0) {
                        totalPartitions.addAndGet(total);
                    }
                    if (srcPartitionValues != null) {
                        chunk.forEach(p -> srcPartitionValues.add(p.values()));
                    }
                    List<HmsPartitionDto> targetChunk = new ArrayList<>(chunk.size());
                    // Подзадачи данных порции заводятся разом и ждутся вместе: ожидание каждой
                    // по очереди превратило бы перенос 200k партиций в 200k последовательных копий
                    List<String> chunkSubjobIds = new ArrayList<>();

                    for (HmsPartitionDto part : chunk) {
                        var partRewrite = rewriteLocation(part.location(), job, "партиции таблицы " + tblName);
                        String partSubjobId = tableSubjobId;

                        if (!isLocationWithinTable(part.location(), tableBaseLocation)) {
                            partSubjobId = orchestratorClient.createHdfsSubjob(job.id(), partRewrite.sourceUri(),
                                    partRewrite.targetUri(), partRewrite.sourceClusterId(), partRewrite.targetClusterId(), job.executionPrincipal());
                            if (dataBarrierEnabled && partSubjobId == null) {
                                throw new RuntimeException(String.format(
                                        "Data-First Barrier: оркестратор не создал подзадачу копирования данных партиции '%s'. DDL отменен.",
                                        part.location()));
                            }
                            if (partSubjobId != null) {
                                chunkSubjobIds.add(partSubjobId);
                            }
                        }

                        targetChunk.add(part.relocatedTo(job.targetDbName(), tblName, partRewrite.targetUri()));

                        if (total <= 100 || !isLocationWithinTable(part.location(), tableBaseLocation)) {
                            String partName = part.getPartitionName(table.partitionKeys());
                            eventLog.add(new HmsEventReportDto(
                                    highWaterMark, "BOOTSTRAP_PARTITION", tblName, partName,
                                    partRewrite.sourceUri(), partRewrite.targetUri(), partSubjobId, "APPLIED", null
                            ));
                        }
                    }

                    if (dataBarrierEnabled && !chunkSubjobIds.isEmpty()
                            && !orchestratorClient.awaitSubjobsCompletion(chunkSubjobIds, dataBarrierTimeoutSeconds)) {
                        throw new RuntimeException(String.format(
                                "Data-First Barrier: копирование данных %d партиций таблицы %s не завершено успешно. DDL отменен.",
                                chunkSubjobIds.size(), tblName));
                    }

                    if (!targetChunk.isEmpty()) {
                        var chunkResp = sender.applyPartitionBatch(job.id(), job.targetDbName(), tblName, targetChunk, job.executionPrincipal());
                        if (!chunkResp.getSuccess()) {
                            throw new RuntimeException("Целевой агент вернул ошибку при пакетной вставке партиций: " + chunkResp.getMessage());
                        }
                    }

                    if (total > 100) {
                        eventLog.add(new HmsEventReportDto(
                                highWaterMark, "BOOTSTRAP_PARTITION_CHUNK", tblName,
                                String.format("Батч партиций [%d..%d] из %d", from + 1, from + chunk.size(), total),
                                null, null, tableSubjobId, "APPLIED",
                                String.format("Пакетно реплицировано %d партиций", targetChunk.size())
                        ));
                    }

                    repPartitions.addAndGet(chunk.size());
                });
            } catch (TableInterruptedException e) {
                log.warn("[HmsTaskExecutor] Прерывание чанков партиций таблицы {} задачи {} по сигналу Fencing/Stop", tblName, job.id());
                return;
            }

            // Согласование после переноса: множество партиций источника собрано при обходе
            if (job.dropExtraneousPartitions()) {
                try {
                    List<List<String>> dropPartitionVals = new ArrayList<>();
                    for (String targetName : sender.listTargetPartitionNames(job.id(), job.targetDbName(), tblName, job.executionPrincipal())) {
                        List<String> vals = partitionValuesFromName(targetName);
                        if (!srcPartitionValues.contains(vals)) {
                            dropPartitionVals.add(vals);
                        }
                    }

                    if (!dropPartitionVals.isEmpty()) {
                        var partDropResp = sender.reconcileExtraneousPartitions(job.id(), job.targetDbName(), tblName, dropPartitionVals, job.executionPrincipal());
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

        // Задачу вернул опрос — аренда у этого агента, прошлый отказ сбрасывается
        AtomicBoolean isFenced = new AtomicBoolean(false);
        jobFencedFlags.put(job.id(), isFenced);
        if (!running.get()) {
            log.warn("[HmsTaskExecutor] Пропуск CDC опроса для задачи {}: агент останавливается", job.id());
            return 0;
        }

        long lastEventId = job.lastProcessedEventId() != null ? job.lastProcessedEventId() : 0L;
        List<HmsNotificationEventDto> events;
        try {
            events = hmsClient.getNextNotifications(lastEventId, CDC_BATCH_SIZE);
        } catch (Exception e) {
            if (isEventGap(e)) {
                reportEventGap(job, lastEventId, e.getMessage());
            } else {
                log.warn("[HmsTaskExecutor] Задача {}: не удалось прочитать журнал событий HMS, повтор на следующем опросе: {}",
                        job.id(), e.getMessage());
            }
            return 0;
        }
        if (!events.isEmpty() && lastEventId > 0 && events.get(0).eventId() > lastEventId + 1) {
            // Журнал читается без фильтра, поэтому события идут подряд: дыра означает, что
            // метастор уже удалил их по TTL (hive.metastore.event.db.listener.timetolive)
            reportEventGap(job, lastEventId, "первое доступное событие " + events.get(0).eventId());
            return 0;
        }
        if (events.isEmpty()) {
            AtomicBoolean synced = jobLagSynchronized.computeIfAbsent(job.id(), k -> new AtomicBoolean(false));
            if (!synced.get()) {
                long currentMax = hmsClient.getCurrentNotificationEventId();
                long lag = Math.max(0, currentMax - lastEventId);
                if (lag == 0) {
                    synced.set(true);
                    int curTables = 0;
                    try {
                        curTables = hmsClient.getAllTables(job.sourceDbName()).size();
                    } catch (Exception ignored) {}
                    int curParts = totalPartitionsPerJob.computeIfAbsent(job.id(), k -> new AtomicInteger(0)).get();
                    int cumulativeEvents = syncedCdcEventsPerJob.computeIfAbsent(job.id(), k -> new AtomicInteger(0)).get();
                    orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                            "ACTIVE", curTables, curTables, curParts, curParts, lastEventId, null, 0L,
                            String.format("Синхронизировано %d событий CDC. Текущее отставание: 0 событий.", cumulativeEvents),
                            Collections.emptyList(), agentId
                    ));
                }
            }
            return 0;
        }
        jobLagSynchronized.computeIfAbsent(job.id(), k -> new AtomicBoolean(false)).set(false);

        List<HmsEventReportDto> eventLog = new ArrayList<>();
        int processed = 0;

        long lastCommittedEventId = lastEventId;
        boolean batchHasFailures = false;
        long failedEventId = -1;
        String batchFailureReason = null;

        HmsPipelineSender sender = cdcSender(job.targetAgentGrpcAddress());
        try {
            for (HmsNotificationEventDto event : events) {
                if (!running.get() || (isFenced != null && isFenced.get())) break;
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
                            // Без значений партиций событие не применить: прежний запасной путь
                            // читал все партиции таблицы (200k на каждое событие) и брал «последнюю
                            // по имени» — то есть, как правило, не ту. Событие уходит в повтор и DLQ.
                            eventSuccess = false;
                            failureReason = "Не удалось разобрать значения партиций в сообщении события (формат: "
                                    + event.messageFormat() + ")";
                        }

                        for (HmsPartitionDto part : partsToSync) {
                            var partRewrite = rewriteLocation(part.location(), job, "партиции таблицы " + tbl);
                            String subjobId = orchestratorClient.createHdfsSubjob(job.id(), partRewrite.sourceUri(),
                                    partRewrite.targetUri(), partRewrite.sourceClusterId(), partRewrite.targetClusterId(), job.executionPrincipal());

                            if (dataBarrierEnabled && subjobId != null) {
                                boolean dataOk = orchestratorClient.awaitSubjobCompletion(subjobId, dataBarrierTimeoutSeconds);
                                if (!dataOk) {
                                    eventSuccess = false;
                                    failureReason = "Data-First Barrier: HDFS подзадача копирования партиции " + subjobId + " не завершена вовремя";
                                    break;
                                }
                            }

                            var partResp = sender.applyPartitionBatch(job.id(), job.targetDbName(), tbl, List.of(
                                    part.relocatedTo(job.targetDbName(), tbl, partRewrite.targetUri())
                            ), job.executionPrincipal());

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
                            var rewrite = rewriteLocation(table.sdLocation(), job, "таблицы " + tbl);
                            String subjobId = orchestratorClient.createHdfsSubjob(job.id(), rewrite.sourceUri(),
                                    rewrite.targetUri(), job.sourceClusterId(), job.targetClusterId(), job.executionPrincipal());

                            if (dataBarrierEnabled && subjobId != null) {
                                boolean dataOk = orchestratorClient.awaitSubjobCompletion(subjobId, dataBarrierTimeoutSeconds);
                                if (!dataOk) {
                                    eventSuccess = false;
                                    failureReason = "Data-First Barrier: HDFS подзадача данных таблицы " + subjobId + " не завершена вовремя";
                                }
                            }

                            if (eventSuccess) {
                                HmsTableDto targetTable = new HmsTableDto(
                                        "hive", job.targetDbName(), tbl, table.tableType(), rewrite.targetUri(),
                                        tableFilter.sanitizeParameters(table.parameters()), table.partitionKeys(),
                                        table.inputFormat(), table.outputFormat(), table.serdeLib(),
                                        table.columns(), table.partitionKeyFields(), table.serdeParameters()
                                );
                                var tblResp = sender.applyTable(job.id(), targetTable, job.executionPrincipal());
                                boolean ok = tblResp.getSuccess();
                                eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null,
                                        rewrite.sourceUri(), rewrite.targetUri(), subjobId,
                                        ok ? "APPLIED" : "FAILED", tblResp.getMessage()));

                                if (!ok) {
                                    eventSuccess = false;
                                    failureReason = tblResp.getMessage();
                                }
                            }
                        } else {
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null,
                                    table.sdLocation(), null, null, "SKIPPED_ACID", check.reason()));
                        }
                    }
                } else if ("ALTER_TABLE".equalsIgnoreCase(eventType)) {
                    // Изменение схемы (ADD COLUMNS, RENAME, смена локации) раньше не доезжало до
                    // приемника вовсе: событие проваливалось мимо всех веток и отмечалось
                    // обработанным. Схема расходилась навсегда, без следа в журнале задачи.
                    // RENAME: событие может нести старое имя, а в источнике таблица уже под новым
                    var rename = HmsEventMessageDecoder.tableRename(event.message(), event.messageFormat())
                            .filter(HmsEventMessageDecoder.TableRename::isRename);
                    String renameFrom = rename.map(HmsEventMessageDecoder.TableRename::before).orElse(null);
                    if (rename.isPresent()) {
                        tbl = rename.get().after();
                    }
                    Optional<HmsTableDto> optAltered = hmsClient.getTable(job.sourceDbName(), tbl);
                    if (optAltered.isPresent()) {
                        HmsTableDto table = optAltered.get();
                        var check = tableFilter.evaluate(table);
                        if (check.supported()) {
                            var rewrite = rewriteLocation(table.sdLocation(), job, "таблицы " + tbl);
                            HmsTableDto targetTable = new HmsTableDto(
                                    "hive", job.targetDbName(), tbl, table.tableType(), rewrite.targetUri(),
                                    tableFilter.sanitizeParameters(table.parameters()), table.partitionKeys(),
                                    table.inputFormat(), table.outputFormat(), table.serdeLib(),
                                    table.columns(), table.partitionKeyFields(), table.serdeParameters()
                            );
                            var alterResp = sender.applyTable(job.id(), targetTable, job.executionPrincipal(), renameFrom);
                            boolean ok = alterResp.getSuccess();
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null,
                                    rewrite.sourceUri(), rewrite.targetUri(), null,
                                    ok ? "APPLIED" : "FAILED", alterResp.getMessage()));
                            if (!ok) {
                                eventSuccess = false;
                                failureReason = alterResp.getMessage();
                            }
                        } else {
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null,
                                    table.sdLocation(), null, null, "SKIPPED_ACID", check.reason()));
                        }
                    } else {
                        // Таблицу переименовали или удалили сразу после изменения
                        eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null, null, null, null,
                                "SKIPPED_UNSUPPORTED", "Таблица не найдена в источнике на момент обработки события"));
                    }
                } else if ("DROP_TABLE".equalsIgnoreCase(eventType)) {
                    var dropTblResp = sender.reconcileExtraneousTables(job.id(), job.targetDbName(), List.of(tbl), job.executionPrincipal());
                    boolean ok = dropTblResp.getSuccess();
                    eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null, null, null, null,
                            ok ? "APPLIED" : "FAILED", "deleteData=false"));

                    if (!ok) {
                        eventSuccess = false;
                        failureReason = dropTblResp.getMessage();
                    }
                } else if ("DROP_PARTITION".equalsIgnoreCase(eventType)) {
                    // Событие DROP_PARTITION несет все удаленные партиции разом; прежний разбор
                    // искал ключ values, которого в сообщениях Hive нет, и молча помечал событие
                    // успешным, оставляя партиции на приемнике
                    List<List<String>> dropVals = extractPartitionValuesFromEvent(event);
                    if (dropVals.isEmpty()) {
                        eventSuccess = false;
                        failureReason = "Не удалось разобрать значения удаленных партиций (формат: " + event.messageFormat() + ")";
                    } else {
                        var dropPartResp = sender.reconcileExtraneousPartitions(job.id(), job.targetDbName(), tbl, dropVals, job.executionPrincipal());
                        boolean ok = dropPartResp.getSuccess();
                        for (List<String> partVals : dropVals) {
                            eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, String.join(",", partVals), null, null, null,
                                    ok ? "APPLIED" : "FAILED", "deleteData=false"));
                        }
                        if (!ok) {
                            eventSuccess = false;
                            failureReason = dropPartResp.getMessage();
                        }
                    }
                } else if ("INSERT".equalsIgnoreCase(eventType)) {
                    failureReason = applyInsertEvent(job, sender, event, tbl, eventLog);
                    eventSuccess = failureReason == null;
                }

                if (!handledEventType(eventType)) {
                    // Тип события перенос не выполняет (INSERT, операции с базами данных и
                    // прочее). Раньше такое событие просто отмечалось обработанным, и понять,
                    // что именно пропущено, было нельзя.
                    eventLog.add(new HmsEventReportDto(event.eventId(), eventType, tbl, null, null, null, null,
                            "SKIPPED_UNSUPPORTED", "Тип события не переносится репликатором схем: " + eventType));
                }

                if (eventSuccess) {
                    lastCommittedEventId = event.eventId();
                    eventFailureCounts.remove(job.id() + ":" + event.eventId());
                    processed++;
                } else {
                    AtomicInteger failCounter = eventFailureCounts.computeIfAbsent(job.id() + ":" + event.eventId(), k -> new AtomicInteger(0));
                    int attempts = failCounter.incrementAndGet();

                    if (attempts >= maxEventRetries) {
                        log.warn("[HmsTaskExecutor] Событие CDC id={}, тип={}, таблица={} превысило лимит попыток ({}/{}). Изоляция в Dead-Letter Queue (DLQ).",
                                event.eventId(), eventType, tbl, attempts, maxEventRetries);
                        eventLog.add(new HmsEventReportDto(
                                event.eventId(), eventType, tbl, null,
                                null, null, null,
                                "DLQ_ISOLATED",
                                String.format("Ядовитое событие (attempts=%d/%d): %s", attempts, maxEventRetries, failureReason)
                        ));
                        lastCommittedEventId = event.eventId();
                        eventFailureCounts.remove(job.id() + ":" + event.eventId());
                        processed++;
                    } else {
                        log.error("[HmsTaskExecutor] Сбой применения события CDC id={}, тип={}, таблица={} (попытка {}/{}): {}. Остановка пачки для повтора.",
                                event.eventId(), eventType, tbl, attempts, maxEventRetries, failureReason);
                        batchHasFailures = true;
                        failedEventId = event.eventId();
                        batchFailureReason = failureReason;
                        break;
                    }
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
                    ? String.format("Сбой на событии %d: %s. Повтор пачки CDC (отставание: %d событий).",
                            failedEventId > 0 ? failedEventId : lastCommittedEventId + 1,
                            batchFailureReason != null ? batchFailureReason : "ошибка применения",
                            lag)
                    : String.format("Синхронизировано %d событий CDC. Текущее отставание: %d событий.",
                            cumulativeEvents > 0 ? cumulativeEvents : processed, lag);

            boolean ok = orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                    "ACTIVE", currentTables.size(), currentTables.size(), curPartCount, curPartCount, lastCommittedEventId, null, lag,
                    statusMessage,
                    eventLog, agentId
            ));
            if (!ok) {
                log.warn("[HmsTaskExecutor] Preemptive Fencing: Оркестратор отклонил отчет прогресса CDC задачи {}. Выставлен флаг прерывания.", job.id());
                jobFencedFlags.computeIfAbsent(job.id(), k -> new AtomicBoolean(false)).set(true);
            }
        } catch (OrchestratorGrpcClient.SubjobRejectedException e) {
            // Задачу удалили или отобрали посреди пачки: остаток пачки не применяется
            log.warn("[HmsTaskExecutor] Fencing: {}. Пачка CDC задачи {} прервана", e.getMessage(), job.id());
            isFenced.set(true);
        } catch (Exception e) {
            log.error("[HmsTaskExecutor] Ошибка при передаче CDC событий: {}", e.getMessage(), e);
        }

        return processed;
    }

    /**
     * Событие INSERT: в существующую таблицу или партицию дописаны данные (или перезаписаны).
     *
     * <p>Без inotify это единственный сигнал о новых файлах внутри уже перенесенной партиции:
     * раньше событие пропускалось, и данные на приемник не доезжали. Порядок:
     * <ol>
     *   <li>свежая подзадача копирования каталога — без переиспользования прошедшей копии,
     *       которая новых файлов не видела; при включенном барьере — ожидание ее завершения;</li>
     *   <li>для INSERT OVERWRITE — удаление на приемнике файлов, которых в каталоге больше нет,
     *       иначе запросы видели бы старые строки рядом с новыми;</li>
     *   <li>обновление метаданных партиции или таблицы: изменилась статистика.</li>
     * </ol>
     *
     * @return причина неудачи или {@code null}, если событие применено
     */
    private String applyInsertEvent(HmsPendingJobDto job, HmsPipelineSender sender,
                                    HmsNotificationEventDto event, String tbl, List<HmsEventReportDto> eventLog) {
        var insert = HmsEventMessageDecoder.insert(event.message(), event.messageFormat());
        if (insert.isEmpty()) {
            return "Не удалось разобрать сообщение INSERT (формат: " + event.messageFormat() + ")";
        }
        Optional<HmsTableDto> optTable = hmsClient.getTable(job.sourceDbName(), tbl);
        if (optTable.isEmpty()) {
            eventLog.add(new HmsEventReportDto(event.eventId(), "INSERT", tbl, null, null, null, null,
                    "SKIPPED_UNSUPPORTED", "Таблица не найдена в источнике на момент обработки события"));
            return null;
        }
        HmsTableDto table = optTable.get();
        var check = tableFilter.evaluate(table);
        if (!check.supported()) {
            eventLog.add(new HmsEventReportDto(event.eventId(), "INSERT", tbl, null,
                    table.sdLocation(), null, null, "SKIPPED_ACID", check.reason()));
            return null;
        }

        List<String> partVals = insert.get().partitionValues();
        Optional<HmsPartitionDto> partition = Optional.empty();
        String location = table.sdLocation();
        if (table.isPartitioned()) {
            if (partVals.isEmpty()) {
                return "Событие INSERT в партиционированную таблицу без значений партиции";
            }
            partition = hmsClient.getPartition(job.sourceDbName(), tbl, partVals);
            if (partition.isEmpty()) {
                eventLog.add(new HmsEventReportDto(event.eventId(), "INSERT", tbl, String.join(",", partVals),
                        null, null, null, "SKIPPED_UNSUPPORTED", "Партиция удалена до обработки события"));
                return null;
            }
            location = partition.get().location();
        }
        var rewrite = rewriteLocation(location, job, "каталога INSERT таблицы " + tbl);
        String partName = partition.map(p -> p.getPartitionName(table.partitionKeys())).orElse(null);

        String subjobId = orchestratorClient.createFreshHdfsSubjob(job.id(), rewrite.sourceUri(), rewrite.targetUri(),
                rewrite.sourceClusterId(), rewrite.targetClusterId(), job.executionPrincipal());
        if (dataBarrierEnabled) {
            if (subjobId == null) {
                return "Data-First Barrier: оркестратор не создал подзадачу копирования для INSERT в " + rewrite.sourceUri();
            }
            if (!orchestratorClient.awaitSubjobsCompletion(List.of(subjobId), dataBarrierTimeoutSeconds)) {
                return "Data-First Barrier: копирование данных INSERT (" + subjobId + ") не завершено успешно";
            }
        }

        int pruned = 0;
        if (insert.get().replace()) {
            Set<String> keep = insert.get().fileNames();
            Optional<List<String>> targetFiles = sender.listTargetFiles(rewrite.targetUri(), job.executionPrincipal());
            for (String relative : targetFiles.orElse(List.of())) {
                // Только файлы верхнего уровня каталога: подкаталоги (например, staging Hive)
                // событие не описывает, трогать их без сведений нельзя
                if (!relative.contains("/") && !keep.contains(relative)) {
                    String path = rewrite.targetUri().replaceAll("/+$", "") + "/" + relative;
                    if (!sender.deleteTargetPath(job.id(), path, job.executionPrincipal())) {
                        return "Не удалось удалить на приемнике устаревший файл после INSERT OVERWRITE: " + path;
                    }
                    pruned++;
                }
            }
        }

        var resp = partition.isPresent()
                ? sender.applyPartitionBatch(job.id(), job.targetDbName(), tbl,
                        List.of(partition.get().relocatedTo(job.targetDbName(), tbl, rewrite.targetUri())), job.executionPrincipal())
                : sender.applyTable(job.id(), new HmsTableDto(
                        "hive", job.targetDbName(), tbl, table.tableType(), rewrite.targetUri(),
                        tableFilter.sanitizeParameters(table.parameters()), table.partitionKeys(),
                        table.inputFormat(), table.outputFormat(), table.serdeLib(),
                        table.columns(), table.partitionKeyFields(), table.serdeParameters()), job.executionPrincipal());
        eventLog.add(new HmsEventReportDto(event.eventId(), "INSERT", tbl, partName,
                rewrite.sourceUri(), rewrite.targetUri(), subjobId, resp.getSuccess() ? "APPLIED" : "FAILED",
                insert.get().replace()
                        ? "INSERT OVERWRITE: удалено устаревших файлов на приемнике: " + pruned
                        : resp.getMessage()));
        return resp.getSuccess() ? null : resp.getMessage();
    }

    /**
     * Значения партиции из ее имени в формате метастора: {@code dt=2026-10-01/region=eu%2Fwest}.
     * Метастор экранирует спецсимволы в значениях как {@code %XX} (FileUtils.escapePathName).
     */
    static List<String> partitionValuesFromName(String name) {
        List<String> values = new ArrayList<>();
        for (String part : name.split("/")) {
            int eq = part.indexOf('=');
            values.add(unescapePathName(eq >= 0 ? part.substring(eq + 1) : part));
        }
        return values;
    }

    private static String unescapePathName(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length()) {
                try {
                    sb.append((char) Integer.parseInt(s.substring(i + 1, i + 3), 16));
                    i += 2;
                    continue;
                } catch (NumberFormatException ignored) {
                    // не экранированный символ — оставляем как есть
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /** Сколько событий журнала читается за один опрос. */
    private static final int CDC_BATCH_SIZE = 1000;

    private static boolean isEventGap(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("Notification events are missing")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Разрыв журнала событий: часть DDL источника потеряна безвозвратно.
     *
     * <p>Продолжать CDC с первого доступного события значило бы молча разойтись с источником,
     * а прежний код вовсе возвращал пустой список и навсегда останавливал CDC без ошибки.
     * Задача переводится в ERROR — нужен повторный bootstrap.
     */
    private void reportEventGap(HmsPendingJobDto job, long lastEventId, String detail) {
        log.error("[HmsTaskExecutor] Задача {}: разрыв журнала событий HMS после события {} ({})", job.id(), lastEventId, detail);
        orchestratorClient.reportHmsProgress(job.id(), new HmsProgressReportRequest(
                "ERROR", 0, 0, 0, 0, null, null, null,
                String.format("Разрыв журнала событий HMS после события %d (%s): события удалены метастором по TTL. "
                        + "Требуется повторный bootstrap.", lastEventId, detail),
                Collections.emptyList(), agentId));
    }

    private List<List<String>> extractPartitionValuesFromEvent(HmsNotificationEventDto event) {
        if (event == null) {
            return Collections.emptyList();
        }
        return HmsEventMessageDecoder.partitionValues(event.message(), event.messageFormat());
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

    /**
     * Канал к агенту приемника для опроса CDC.
     *
     * <p>Опрос идет по расписанию для каждой задачи, поэтому канал живет между вызовами: иначе на
     * каждый опрос приходится разрешение имени, TCP-соединение и TLS-рукопожатие — ради нескольких
     * событий, а при пустом журнале и вовсе впустую. Переподключение внутри живого канала gRPC
     * берет на себя сам. Бутстрап — разовая операция, и там канал по-прежнему свой.
     */
    private HmsPipelineSender cdcSender(String targetAddress) {
        return cdcSenders.computeIfAbsent(targetAddress, this::newPipelineSender);
    }

    /**
     * Создание канала к агенту приемника. Точка подмены в тестах.
     */
    protected HmsPipelineSender newPipelineSender(String targetAddress) {
        return new HmsPipelineSender(targetAddress, grpcTlsEnabled, insecureSkipVerify, agentSecret);
    }

    @Override
    public void close() {
        running.set(false);
        if (bootstrapExecutor != null) {
            bootstrapExecutor.shutdownNow();
        }
        for (HmsPipelineSender sender : cdcSenders.values()) {
            try {
                sender.close();
            } catch (Exception e) {
                log.warn("[HmsTaskExecutor] Не удалось закрыть канал к приемнику: {}", e.getMessage());
            }
        }
        cdcSenders.clear();
    }
}
