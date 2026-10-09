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
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Service
public class HmsCoordinatorService {

    private static final Logger log = LoggerFactory.getLogger(HmsCoordinatorService.class);

    private final HmsReplicationJobRepository hmsJobRepository;
    private final HmsEventLogRepository hmsEventLogRepository;
    private final HmsClientPool hmsClientPool;
    private final TableSupportFilter tableFilter;
    private final HmsPathRewriter pathRewriter;
    private final JobService jobService;

    public HmsCoordinatorService(
            HmsReplicationJobRepository hmsJobRepository,
            HmsEventLogRepository hmsEventLogRepository,
            HmsClientPool hmsClientPool,
            TableSupportFilter tableFilter,
            HmsPathRewriter pathRewriter,
            JobService jobService
    ) {
        this.hmsJobRepository = hmsJobRepository;
        this.hmsEventLogRepository = hmsEventLogRepository;
        this.hmsClientPool = hmsClientPool;
        this.tableFilter = tableFilter;
        this.pathRewriter = pathRewriter;
        this.jobService = jobService;
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
        String id = "hms-job-" + sourceDb + "-" + UUID.randomUUID().toString().substring(0, 8);

        HmsReplicationJobEntity entity = new HmsReplicationJobEntity();
        entity.setId(id);
        entity.setSourceClusterId(sourceClusterId != null ? sourceClusterId : "dc1");
        entity.setTargetClusterId(targetClusterId != null ? targetClusterId : "dc2");
        entity.setSourceDbName(sourceDb);
        entity.setTargetDbName(targetDb != null ? targetDb : sourceDb);
        entity.setTableIncludePattern(tablePattern != null ? tablePattern : "*");
        entity.setCreatedBy(author != null ? author : "system_operator");
        entity.setStatus("BOOTSTRAPPING");

        hmsJobRepository.saveAndFlush(entity);

        // Запуск первичного полного Bootstrap
        executeBootstrap(entity);

        return entity;
    }

    /**
     * Выполнение полного начального Bootstrap (Initial Full Sync).
     */
    public void executeBootstrap(HmsReplicationJobEntity job) {
        log.info("Запуск первичного Bootstrap для задачи {} ({}.{})",
                job.getId(), job.getSourceClusterId(), job.getSourceDbName());

        HmsClient srcClient = hmsClientPool.getClient(job.getSourceClusterId());
        HmsClient dstClient = hmsClientPool.getClient(job.getTargetClusterId());

        long highWaterMark = srcClient.getCurrentNotificationEventId();
        job.setBootstrapEventId(highWaterMark);
        job.setLastProcessedEventId(highWaterMark);

        // Создаем базу на приемнике, если отсутствует
        dstClient.createDatabase(job.getTargetDbName(), null);

        List<String> tables = srcClient.getAllTables(job.getSourceDbName());
        job.setTotalTables(tables.size());
        int replicatedTablesCount = 0;
        int replicatedPartitionsCount = 0;

        for (String tblName : tables) {
            Optional<HmsTableDto> optTable = srcClient.getTable(job.getSourceDbName(), tblName);
            if (optTable.isEmpty()) continue;
            HmsTableDto table = optTable.get();

            // 1. Проверка поддержки типа таблицы (Non-ACID Gate)
            var filterResult = tableFilter.evaluate(table);
            if (!filterResult.supported()) {
                log.info("Пропущена таблица {}.{}: {}", job.getSourceDbName(), tblName, filterResult.reason());
                recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_TABLE", tblName, null,
                        table.sdLocation(), null, null, "SKIPPED_ACID", filterResult.reason());
                continue;
            }

            // 2. Трансляция пути HDFS таблицы с учетом федерации
            var rewriteResult = pathRewriter.rewrite(
                    table.sdLocation(), job.getSourceClusterId(), job.getTargetClusterId()
            );

            // 3. Создание скрытой саб-джобы HDFS передачи
            JobResponse subjob = createHdfsSubjob(job.getId(), rewriteResult.sourceUri(), rewriteResult.targetUri(),
                    rewriteResult.sourceClusterId(), rewriteResult.targetClusterId());

            // 4. Очистка вендорных параметров HDP 3.1
            Map<String, String> cleanParams = tableFilter.sanitizeParameters(table.parameters());

            // 5. Создание таблицы на приемнике (Apache Hive 3.1.3)
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
            dstClient.createTable(targetTable);

            // 6. Если есть партиции - переносим каждую партицию с учетом её NameService
            if (table.isPartitioned()) {
                List<HmsPartitionDto> partitions = srcClient.getPartitions(job.getSourceDbName(), tblName);
                List<HmsPartitionDto> targetPartitions = new ArrayList<>();

                for (HmsPartitionDto part : partitions) {
                    var partRewrite = pathRewriter.rewrite(
                            part.location(), job.getSourceClusterId(), job.getTargetClusterId()
                    );

                    JobResponse partSubjob = createHdfsSubjob(job.getId(), partRewrite.sourceUri(),
                            partRewrite.targetUri(), partRewrite.sourceClusterId(), partRewrite.targetClusterId());

                    HmsPartitionDto targetPart = new HmsPartitionDto(
                            "hive",
                            job.getTargetDbName(),
                            tblName,
                            part.values(),
                            partRewrite.targetUri(),
                            part.parameters()
                    );
                    targetPartitions.add(targetPart);

                    String partName = part.getPartitionName(table.partitionKeys());
                    recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_PARTITION", tblName, partName,
                            partRewrite.sourceUri(), partRewrite.targetUri(), partSubjob.id(), "APPLIED", null);
                    replicatedPartitionsCount++;
                }

                if (!targetPartitions.isEmpty()) {
                    dstClient.addPartitions(job.getTargetDbName(), tblName, targetPartitions);
                }
            }

            recordEvent(job.getId(), highWaterMark, "BOOTSTRAP_TABLE", tblName, null,
                    rewriteResult.sourceUri(), rewriteResult.targetUri(), subjob.id(), "APPLIED", null);
            replicatedTablesCount++;
        }

        job.setReplicatedTables(replicatedTablesCount);
        job.setTotalPartitions(replicatedPartitionsCount);
        job.setReplicatedPartitions(replicatedPartitionsCount);
        job.setStatus("ACTIVE");
        job.setLastSyncAt(Instant.now());
        job.setMessage("Первичный Bootstrap успешно завершен. Репликация переведена в потоковый режим CDC.");
        hmsJobRepository.save(job);

        log.info("Bootstrap завершен для {}: таблиц={}/{}, партиций={}",
                job.getId(), replicatedTablesCount, tables.size(), replicatedPartitionsCount);
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
}
