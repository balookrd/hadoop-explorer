package org.apache.hadoop.explorer.replicator.orchestrator.hms.service;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClientPool;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.filter.TableSupportFilter;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.rewriter.HmsPathRewriter;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class HmsCoordinatorService {

    private static final Logger log = LoggerFactory.getLogger(HmsCoordinatorService.class);

    private final HmsReplicationJobRepository hmsJobRepository;
    private final HmsEventLogRepository hmsEventLogRepository;
    private final HmsClientPool hmsClientPool;
    private final TableSupportFilter tableFilter;
    private final HmsPathRewriter pathRewriter;
    private final JobService jobService;
    private final ReplicatorProperties properties;
    private final ExecutorService tableBootstrapExecutor;

    public HmsCoordinatorService(
            HmsReplicationJobRepository hmsJobRepository,
            HmsEventLogRepository hmsEventLogRepository,
            HmsClientPool hmsClientPool,
            TableSupportFilter tableFilter,
            HmsPathRewriter pathRewriter,
            JobService jobService,
            ReplicatorProperties properties
    ) {
        this.hmsJobRepository = hmsJobRepository;
        this.hmsEventLogRepository = hmsEventLogRepository;
        this.hmsClientPool = hmsClientPool;
        this.tableFilter = tableFilter;
        this.pathRewriter = pathRewriter;
        this.jobService = jobService;
        this.properties = properties;

        int concurrency = (properties != null && properties.getHmsBootstrapConcurrency() > 0)
                ? properties.getHmsBootstrapConcurrency() : 8;
        this.tableBootstrapExecutor = Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r, "hms-bootstrap-worker");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    public void shutdown() {
        if (tableBootstrapExecutor != null) {
            tableBootstrapExecutor.shutdown();
        }
    }

    /**
     * Создание новой задачи репликации базы данных и запуск первичного Bootstrap.
     */
    @Transactional
    public HmsReplicationJobEntity createAndStartReplication(
            String sourceClusterId,
            String targetClusterId,
            String sourceDb,
            String targetDb,
            String tablePattern,
            String author
    ) {
        return createAndStartReplication(sourceClusterId, targetClusterId, sourceDb, targetDb, tablePattern, author, false, false);
    }

    /**
     * Создание новой задачи репликации базы данных с явным указанием флагов сверки/удаления
     * лишних объектов и запуск первичного Bootstrap.
     */
    @Transactional
    public HmsReplicationJobEntity createAndStartReplication(
            String sourceClusterId,
            String targetClusterId,
            String sourceDb,
            String targetDb,
            String tablePattern,
            String author,
            boolean dropExtraneousTables,
            boolean dropExtraneousPartitions
    ) {
        String id = "hms-job-" + sourceDb + "-" + UUID.randomUUID().toString().substring(0, 8);

        HmsReplicationJobEntity entity = new HmsReplicationJobEntity();
        entity.setId(id);
        entity.setSourceClusterId(sourceClusterId != null ? sourceClusterId : "dc1");
        entity.setTargetClusterId(targetClusterId != null ? targetClusterId : "dc2");
        entity.setSourceDbName(sourceDb);
        entity.setTargetDbName(targetDb != null ? targetDb : sourceDb);
        entity.setTableIncludePattern(tablePattern != null ? tablePattern : "*");
        entity.setCreatedBy(author != null ? author : "system_operator");
        entity.setDropExtraneousTables(dropExtraneousTables);
        entity.setDropExtraneousPartitions(dropExtraneousPartitions);
        entity.setStatus("BOOTSTRAPPING");

        hmsJobRepository.saveAndFlush(entity);

        // Запуск первичного полного Bootstrap
        executeBootstrap(entity);

        return entity;
    }

    /**
     * Выполнение полного начального Bootstrap (Initial Full Sync).
     * Многопоточная обработка таблиц и чанкинг партиций с умной агрегацией саб-джоб HDFS.
     */
    public void executeBootstrap(HmsReplicationJobEntity job) {
        log.info("Запуск масштабируемого первичного Bootstrap для задачи {} ({}.{}) с параллелизмом {}",
                job.getId(), job.getSourceClusterId(), job.getSourceDbName(),
                (properties != null ? properties.getHmsBootstrapConcurrency() : 8));

        HmsClient srcClient = hmsClientPool.getClient(job.getSourceClusterId());
        HmsClient dstClient = hmsClientPool.getClient(job.getTargetClusterId());

        long highWaterMark = srcClient.getCurrentNotificationEventId();
        job.setBootstrapEventId(highWaterMark);
        job.setLastProcessedEventId(highWaterMark);

        // Создаем базу на приемнике, если отсутствует
        dstClient.createDatabase(job.getTargetDbName(), null);

        List<String> allSourceTables = srcClient.getAllTables(job.getSourceDbName());
        List<String> tables = allSourceTables.stream()
                .filter(t -> matchesPattern(t, job.getTableIncludePattern()))
                .toList();

        // Табличный Diff & Reconciliation:
        // Если активирован флаг dropExtraneousTables, удаляем таблицы на приемнике,
        // которых нет в источнике (и которые подпадают под паттерн репликации).
        // ВАЖНО: deleteData ВСЕГДА false (данные на HDFS остаются нетронутыми).
        if (job.isDropExtraneousTables()) {
            List<String> dstExistingTables = dstClient.getAllTables(job.getTargetDbName());
            Set<String> sourceTableSet = new HashSet<>(tables);
            for (String dstTable : dstExistingTables) {
                if (matchesPattern(dstTable, job.getTableIncludePattern()) && !sourceTableSet.contains(dstTable)) {
                    log.warn("Reconciliation: обнаружена лишняя таблица на приемнике {}.{} (задача {}) — удаление из HMS",
                            job.getTargetDbName(), dstTable, job.getId());
                    dstClient.dropTable(job.getTargetDbName(), dstTable, false);
                    recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_DROP_EXTRA_TABLE", dstTable, null,
                            null, null, null, "DROPPED",
                            "Лишняя таблица удалена при начальном согласовании (Reconciliation). Данные на HDFS сохранены.");
                }
            }
        }

        job.setTotalTables(tables.size());
        hmsJobRepository.saveAndFlush(job);

        AtomicInteger replicatedTablesCount = new AtomicInteger(0);
        AtomicInteger replicatedPartitionsCount = new AtomicInteger(0);
        AtomicInteger totalPartitionsCount = new AtomicInteger(0);

        List<CompletableFuture<Void>> tableFutures = new ArrayList<>();
        for (String tblName : tables) {
            tableFutures.add(CompletableFuture.runAsync(() -> {
                try {
                    bootstrapSingleTable(job, srcClient, dstClient, highWaterMark, tblName,
                            replicatedTablesCount, replicatedPartitionsCount, totalPartitionsCount);
                } catch (Exception e) {
                    log.error("Сбой репликации таблицы {}.{} в задаче {}: {}",
                            job.getSourceDbName(), tblName, job.getId(), e.getMessage(), e);
                    recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_TABLE", tblName, null,
                            null, null, null, "FAILED", e.getMessage());
                }
            }, tableBootstrapExecutor));
        }

        // Ожидаем завершения параллельного переноса всех таблиц
        CompletableFuture.allOf(tableFutures.toArray(new CompletableFuture[0])).join();

        job.setReplicatedTables(replicatedTablesCount.get());
        job.setTotalPartitions(totalPartitionsCount.get());
        job.setReplicatedPartitions(replicatedPartitionsCount.get());
        job.setStatus("ACTIVE");
        job.setLastSyncAt(Instant.now());
        job.setMessage(String.format("Первичный Bootstrap успешно завершен (таблиц: %d/%d, партиций: %d). Репликация переведена в потоковый режим CDC.",
                replicatedTablesCount.get(), tables.size(), replicatedPartitionsCount.get()));
        hmsJobRepository.save(job);

        log.info("Масштабируемый Bootstrap завершен для {}: таблиц={}/{}, партиций={}/{}",
                job.getId(), replicatedTablesCount.get(), tables.size(),
                replicatedPartitionsCount.get(), totalPartitionsCount.get());
    }

    /**
     * Изолированная обработка отдельной таблицы в пуле потоков воркеров.
     */
    private void bootstrapSingleTable(
            HmsReplicationJobEntity job,
            HmsClient srcClient,
            HmsClient dstClient,
            long highWaterMark,
            String tblName,
            AtomicInteger replicatedTablesCount,
            AtomicInteger replicatedPartitionsCount,
            AtomicInteger totalPartitionsCount
    ) {
        Optional<HmsTableDto> optTable = srcClient.getTable(job.getSourceDbName(), tblName);
        if (optTable.isEmpty()) return;
        HmsTableDto table = optTable.get();

        // 1. Проверка поддержки типа таблицы (Non-ACID Gate)
        var filterResult = tableFilter.evaluate(table);
        if (!filterResult.supported()) {
            log.info("Пропущена таблица {}.{}: {}", job.getSourceDbName(), tblName, filterResult.reason());
            recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_TABLE", tblName, null,
                    table.sdLocation(), null, null, "SKIPPED_ACID", filterResult.reason());
            return;
        }

        // 2. Трансляция пути корня HDFS таблицы с учетом федерации
        var rewriteResult = pathRewriter.rewrite(
                table.sdLocation(), job.getSourceClusterId(), job.getTargetClusterId()
        );

        // 3. Создание 1 корневой саб-джобы HDFS передачи данных таблицы!
        JobResponse tableSubjob = createHdfsSubjob(job.getId(), rewriteResult.sourceUri(), rewriteResult.targetUri(),
                rewriteResult.sourceClusterId(), rewriteResult.targetClusterId());

        // 4. Очистка вендорных параметров HDP 3.1
        Map<String, String> cleanParams = tableFilter.sanitizeParameters(table.parameters());

        // 5. Создание или обновление таблицы на приемнике (Идемпотентный DDL: Create or Alter)
        HmsTableDto targetTable = new HmsTableDto(
                "hive",
                job.getTargetDbName(),
                tblName,
                table.tableType(),
                rewriteResult.targetUri(),
                cleanParams,
                table.partitionKeys(),
                table.inputFormat(),
                table.outputFormat(),
                table.serdeLib()
        );
        Optional<HmsTableDto> existingTargetTable = dstClient.getTable(job.getTargetDbName(), tblName);
        if (existingTargetTable.isPresent()) {
            log.info("Таблица {}.{} уже существует на приемнике — сверка схемы через alterTable",
                    job.getTargetDbName(), tblName);
            dstClient.alterTable(targetTable);
        } else {
            dstClient.createTable(targetTable);
        }

        // 6. Обработка партиций с батчингом (Chunking) и умной агрегацией HDFS
        if (table.isPartitioned()) {
            List<HmsPartitionDto> allPartitions = srcClient.getPartitions(job.getSourceDbName(), tblName);
            totalPartitionsCount.addAndGet(allPartitions.size());

            // Reconciliation партиций:
            // Если включен флаг dropExtraneousPartitions, сверяем партиции на приемнике
            // и удаляем те, которых больше нет в источнике.
            // ВАЖНО: deleteData ВСЕГДА false (файлы на HDFS не удаляются).
            if (job.isDropExtraneousPartitions()) {
                List<HmsPartitionDto> dstPartitions = dstClient.getPartitions(job.getTargetDbName(), tblName);
                Set<List<String>> srcPartitionValues = allPartitions.stream()
                        .map(HmsPartitionDto::values)
                        .collect(Collectors.toSet());

                for (HmsPartitionDto dstPart : dstPartitions) {
                    if (!srcPartitionValues.contains(dstPart.values())) {
                        String partName = dstPart.getPartitionName(table.partitionKeys());
                        log.warn("Reconciliation: удаление лишней партиции на приемнике {}.{}/{} (задача {})",
                                job.getTargetDbName(), tblName, partName, job.getId());
                        dstClient.dropPartition(job.getTargetDbName(), tblName, dstPart.values(), false);
                        recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_DROP_EXTRA_PARTITION", tblName, partName,
                                null, null, null, "DROPPED",
                                "Лишняя партиция удалена при начальном согласовании (Reconciliation). Данные на HDFS сохранены.");
                    }
                }
            }

            int batchSize = (properties != null && properties.getHmsPartitionBatchSize() > 0)
                    ? properties.getHmsPartitionBatchSize() : 1000;
            String tableBaseLocation = table.sdLocation();

            // Разбиваем партиции на порции для безопасной передачи по Thrift RPC
            for (int i = 0; i < allPartitions.size(); i += batchSize) {
                int toIndex = Math.min(i + batchSize, allPartitions.size());
                List<HmsPartitionDto> batch = allPartitions.subList(i, toIndex);

                List<HmsPartitionDto> targetPartitionsBatch = new ArrayList<>(batch.size());

                for (HmsPartitionDto part : batch) {
                    var partRewrite = pathRewriter.rewrite(
                            part.location(), job.getSourceClusterId(), job.getTargetClusterId()
                    );

                    // УМНАЯ АГРЕГАЦИЯ HDFS:
                    // Если партиция вынесена за пределы корневого каталога таблицы (cold tier / другой NameService / кастомный путь),
                    // создаем индивидуальную саб-джобу.
                    // Если партиция в стандартном дереве таблицы, данные скопируются корневой саб-джобой таблицы!
                    String partSubjobId = tableSubjob.id();
                    if (!isLocationWithinTable(part.location(), tableBaseLocation)) {
                        JobResponse partSubjob = createHdfsSubjob(job.getId(), partRewrite.sourceUri(),
                                partRewrite.targetUri(), partRewrite.sourceClusterId(), partRewrite.targetClusterId());
                        partSubjobId = partSubjob.id();
                    }

                    HmsPartitionDto targetPart = new HmsPartitionDto(
                            "hive",
                            job.getTargetDbName(),
                            tblName,
                            part.values(),
                            partRewrite.targetUri(),
                            part.parameters()
                    );
                    targetPartitionsBatch.add(targetPart);

                    // Если партиций немного (<= 100) или это вынесенная партиция — пишем детальное событие
                    if (allPartitions.size() <= 100 || !isLocationWithinTable(part.location(), tableBaseLocation)) {
                        String partName = part.getPartitionName(table.partitionKeys());
                        recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_PARTITION", tblName, partName,
                                partRewrite.sourceUri(), partRewrite.targetUri(), partSubjobId, "APPLIED", null);
                    }
                }

                // Пакетная вставка в целевой HMS (Thrift RPC safe chunk)
                if (!targetPartitionsBatch.isEmpty()) {
                    dstClient.addPartitions(job.getTargetDbName(), tblName, targetPartitionsBatch);
                }

                // Для больших таблиц (> 100 партиций) пишем агрегированную запись на чанк, сохраняя производительность СУБД
                if (allPartitions.size() > 100) {
                    recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_PARTITION_CHUNK", tblName,
                            String.format("Батч партиций [%d..%d] из %d", i + 1, toIndex, allPartitions.size()),
                            null, null, tableSubjob.id(), "APPLIED",
                            String.format("Пакетно реплицировано %d партиций", targetPartitionsBatch.size()));
                }

                replicatedPartitionsCount.addAndGet(batch.size());
            }
        }

        recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_TABLE", tblName, null,
                rewriteResult.sourceUri(), rewriteResult.targetUri(), tableSubjob.id(), "APPLIED", null);
        replicatedTablesCount.incrementAndGet();

        log.info("Таблица {}.{} успешно реплицирована в Bootstrap", job.getTargetDbName(), tblName);
    }

    /**
     * Проверка, находится ли директория партиции внутри стандартного корня таблицы.
     */
    private boolean isLocationWithinTable(String partLocation, String tableBaseLocation) {
        if (partLocation == null || tableBaseLocation == null) {
            return false;
        }
        String cleanTable = tableBaseLocation.replaceAll("/+$", "");
        String cleanPart = partLocation.replaceAll("/+$", "");
        return cleanPart.startsWith(cleanTable + "/");
    }

    /**
     * Потоковая синхронизация порции событий из NOTIFICATION_LOG.
     */
    @Transactional
    public int pollCdcEvents(String hmsJobId) {
        Optional<HmsReplicationJobEntity> optJob = hmsJobRepository.findById(hmsJobId);
        if (optJob.isEmpty()) return 0;
        HmsReplicationJobEntity job = optJob.get();

        if (!"ACTIVE".equalsIgnoreCase(job.getStatus())) {
            return 0;
        }

        HmsClient srcClient = hmsClientPool.getClient(job.getSourceClusterId());
        HmsClient dstClient = hmsClientPool.getClient(job.getTargetClusterId());

        long lastEventId = job.getLastProcessedEventId() != null ? job.getLastProcessedEventId() : 0L;
        List<HmsNotificationEventDto> events = srcClient.getNextNotifications(lastEventId, 50);

        if (events.isEmpty()) {
            job.setEventLag(0L);
            hmsJobRepository.save(job);
            return 0;
        }

        int processed = 0;
        for (HmsNotificationEventDto event : events) {
            String tbl = event.tableName();
            String eventType = event.eventType();

            log.info("Обработка CDC события #{} [{}]: {}.{}", event.eventId(), eventType, event.dbName(), tbl);

            if ("ADD_PARTITION".equalsIgnoreCase(eventType) || "ALTER_PARTITION".equalsIgnoreCase(eventType)) {
                Optional<HmsTableDto> optTable = srcClient.getTable(job.getSourceDbName(), tbl);
                if (optTable.isPresent()) {
                    List<HmsPartitionDto> parts = srcClient.getPartitions(job.getSourceDbName(), tbl);
                    for (HmsPartitionDto part : parts) {
                        var partRewrite = pathRewriter.rewrite(
                                part.location(), job.getSourceClusterId(), job.getTargetClusterId()
                        );
                        JobResponse subjob = createHdfsSubjob(job.getId(), partRewrite.sourceUri(),
                                partRewrite.targetUri(), partRewrite.sourceClusterId(), partRewrite.targetClusterId());

                        dstClient.addPartitions(job.getTargetDbName(), tbl, List.of(
                                new HmsPartitionDto("hive", job.getTargetDbName(), tbl, part.values(), partRewrite.targetUri(), part.parameters())
                        ));

                        String partName = part.getPartitionName(optTable.get().partitionKeys());
                        recordEvent(job.getId(), event.eventId(), eventType, tbl, partName,
                                partRewrite.sourceUri(), partRewrite.targetUri(), subjob.id(), "APPLIED", null);
                    }
                }
            } else if ("CREATE_TABLE".equalsIgnoreCase(eventType)) {
                Optional<HmsTableDto> optTable = srcClient.getTable(job.getSourceDbName(), tbl);
                if (optTable.isPresent()) {
                    HmsTableDto table = optTable.get();
                    var check = tableFilter.evaluate(table);
                    if (check.supported()) {
                        var rewrite = pathRewriter.rewrite(table.sdLocation(), job.getSourceClusterId(), job.getTargetClusterId());
                        JobResponse subjob = createHdfsSubjob(job.getId(), rewrite.sourceUri(), rewrite.targetUri(), rewrite.sourceClusterId(), rewrite.targetClusterId());
                        HmsTableDto targetTable = new HmsTableDto(
                                "hive", job.getTargetDbName(), tbl, table.tableType(), rewrite.targetUri(),
                                tableFilter.sanitizeParameters(table.parameters()), table.partitionKeys(),
                                table.inputFormat(), table.outputFormat(), table.serdeLib()
                        );
                        dstClient.createTable(targetTable);
                        recordEvent(job.getId(), event.eventId(), eventType, tbl, null,
                                rewrite.sourceUri(), rewrite.targetUri(), subjob.id(), "APPLIED", null);
                    } else {
                        recordEvent(job.getId(), event.eventId(), eventType, tbl, null,
                                table.sdLocation(), null, null, "SKIPPED_ACID", check.reason());
                    }
                }
            } else if ("DROP_TABLE".equalsIgnoreCase(eventType)) {
                // Строгая политика: deleteData = false (сохранение файлов HDFS)
                dstClient.dropTable(job.getTargetDbName(), tbl, false);
                recordEvent(job.getId(), event.eventId(), eventType, tbl, null, null, null, null, "APPLIED", "deleteData=false");
            } else if ("DROP_PARTITION".equalsIgnoreCase(eventType)) {
                // Строгая политика: deleteData = false
                dstClient.dropPartition(job.getTargetDbName(), tbl, Collections.emptyList(), false);
                recordEvent(job.getId(), event.eventId(), eventType, tbl, null, null, null, null, "APPLIED", "deleteData=false");
            }

            job.setLastProcessedEventId(event.eventId());
            processed++;
        }

        long currentMax = srcClient.getCurrentNotificationEventId();
        job.setEventLag(Math.max(0, currentMax - job.getLastProcessedEventId()));
        job.setLastSyncAt(Instant.now());
        hmsJobRepository.save(job);

        return processed;
    }

    private JobResponse createHdfsSubjob(String hmsJobId, String srcPath, String dstPath, String srcCl, String dstCl) {
        CreateJobRequest subjobReq = new CreateJobRequest(
                srcPath,
                dstPath,
                srcCl,
                dstCl,
                0L,
                "hdfs@EXAMPLE.COM",
                true,
                false,
                null,
                20,
                "HMS_SUBJOB",
                hmsJobId
        );
        return jobService.createJob(subjobReq, "hms-coordinator");
    }

    private void recordEvent(
            String hmsJobId,
            long eventId,
            String eventType,
            String tableName,
            String partitionName,
            String srcUri,
            String dstUri,
            String subjobId,
            String status,
            String errorMsg
    ) {
        HmsEventLogEntity event = new HmsEventLogEntity();
        event.setId(UUID.randomUUID().toString());
        event.setHmsJobId(hmsJobId);
        event.setEventId(eventId);
        event.setEventType(eventType);
        event.setTableName(tableName);
        event.setPartitionName(partitionName);
        event.setSourceUri(srcUri);
        event.setTargetUri(dstUri);
        event.setSubjobId(subjobId);
        event.setStatus(status);
        event.setErrorMessage(errorMsg);
        hmsEventLogRepository.save(event);
    }

    @Transactional
    public boolean deleteReplicationJob(String id) {
        if (!hmsJobRepository.existsById(id)) {
            return false;
        }
        hmsEventLogRepository.deleteByHmsJobId(id);
        jobService.deleteSubjobsByParentId(id);
        hmsJobRepository.deleteById(id);
        log.info("Задача репликации HMS {} и все связанные ресурсы (события, саб-джобы) успешно удалены", id);
        return true;
    }

    @Transactional
    public void rebootstrap(String id) {
        HmsReplicationJobEntity job = hmsJobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Задача репликации HMS не найдена: " + id));
        job.setStatus("BOOTSTRAPPING");
        hmsJobRepository.saveAndFlush(job);
        executeBootstrap(job);
    }

    /**
     * Автоматическое восстановление задач, прерванных рестартом или сбоем оркестратора (Failover Crash Recovery).
     * Обнаруживает задачи в статусе BOOTSTRAPPING и возобновляет их перенос.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void recoverInterruptedJobsOnStartup() {
        List<HmsReplicationJobEntity> interrupted = hmsJobRepository.findByStatus("BOOTSTRAPPING");
        if (interrupted.isEmpty()) {
            return;
        }
        log.warn("Обнаружено {} прерванных задач(и) HMS репликации в статусе BOOTSTRAPPING после старта оркестратора. Запуск авто-восстановления...",
                interrupted.size());
        for (HmsReplicationJobEntity job : interrupted) {
            log.info("Восстановление прерванной задачи Bootstrap: {} ({}.{})", job.getId(), job.getSourceClusterId(), job.getSourceDbName());
            try {
                executeBootstrap(job);
            } catch (Exception e) {
                log.error("Сбой авто-восстановления Bootstrap для {}: {}", job.getId(), e.getMessage(), e);
                job.setStatus("FAILED");
                job.setMessage("Сбой авто-восстановления после перезапуска оркестратора: " + e.getMessage());
                hmsJobRepository.save(job);
            }
        }
    }

    /**
     * Автоматический фоновый опрос CDC событий из NOTIFICATION_LOG для всех активных задач репликации.
     */
    @Scheduled(fixedDelayString = "${hadoop.replicator.hms-poll-interval-ms:5000}")
    public void scheduledCdcPoll() {
        List<HmsReplicationJobEntity> activeJobs = hmsJobRepository.findByStatus("ACTIVE");
        for (HmsReplicationJobEntity job : activeJobs) {
            try {
                pollCdcEvents(job.getId());
            } catch (Exception e) {
                log.error("Ошибка фонового опроса CDC для HMS задачи {}: {}", job.getId(), e.getMessage());
            }
        }
    }

    /**
     * Проверка соответствия имени таблицы шаблону (glob-маска со звёздочкой).
     */
    private boolean matchesPattern(String tableName, String pattern) {
        if (pattern == null || pattern.isBlank() || pattern.equals("*")) {
            return true;
        }
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return tableName.matches(regex);
    }
}
