package org.apache.hadoop.explorer.replicator.hms.service;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * gRPC серверный обработчик прямого конвейера метаданных Hive Metastore на стороне целевого агента (Target Agent).
 * Локально взаимодействует с Thrift Hive Metastore целевого дата-центра.
 */
public class HmsTransferServiceImpl extends HmsTransferServiceGrpc.HmsTransferServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(HmsTransferServiceImpl.class);

    private final HmsClient hmsClient;

    public HmsTransferServiceImpl(HmsClient hmsClient) {
        this.hmsClient = hmsClient;
    }

    @Override
    public void applyDatabase(ApplyDatabaseRequest request, StreamObserver<HmsOperationResponse> responseObserver) {
        try {
            String dbName = request.getDbName();
            String loc = request.getLocationUri().isBlank() ? null : request.getLocationUri();
            hmsClient.executeAs(request.getExecutionPrincipal(), () -> {
                hmsClient.createDatabase(dbName, loc);
                return null;
            });

            log.info("[HmsTransferService] База данных '{}' успешно создана/проверена на целевом кластере (doAs={})",
                    dbName, request.getExecutionPrincipal());
            responseObserver.onNext(HmsOperationResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(true)
                    .setMessage("Database ready: " + dbName)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[HmsTransferService] Ошибка создания базы {}: {}", request.getDbName(), e.getMessage(), e);
            responseObserver.onNext(HmsOperationResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(false)
                    .setMessage("Error applying database: " + e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void applyTable(ApplyTableRequest request, StreamObserver<HmsOperationResponse> responseObserver) {
        try {
            HmsTableDto tableDto = HmsConversionHelper.toDto(request.getTable());
            hmsClient.executeAs(request.getExecutionPrincipal(), () -> {
                Optional<HmsTableDto> existing = hmsClient.getTable(tableDto.dbName(), tableDto.tableName());
                String renameFrom = request.getRenameFrom();

                if (existing.isEmpty() && !renameFrom.isBlank()
                        && hmsClient.getTable(tableDto.dbName(), renameFrom).isPresent()) {
                    log.info("[HmsTransferService] Переименование таблицы {}.{} -> {} на приемнике (doAs={})",
                            tableDto.dbName(), renameFrom, tableDto.tableName(), request.getExecutionPrincipal());
                    hmsClient.renameTable(tableDto.dbName(), renameFrom, tableDto);
                } else if (existing.isPresent()) {
                    log.info("[HmsTransferService] Таблица {}.{} уже существует на приемнике — выполнение alterTable (doAs={})",
                            tableDto.dbName(), tableDto.tableName(), request.getExecutionPrincipal());
                    hmsClient.alterTable(tableDto);
                } else {
                    log.info("[HmsTransferService] Создание таблицы {}.{} на приемнике (doAs={})",
                            tableDto.dbName(), tableDto.tableName(), request.getExecutionPrincipal());
                    hmsClient.createTable(tableDto);
                }
                return null;
            });

            responseObserver.onNext(HmsOperationResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(true)
                    .setMessage("Table applied successfully: " + tableDto.tableName())
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[HmsTransferService] Ошибка применения таблицы {}: {}",
                    request.getTable().getTableName(), e.getMessage(), e);
            responseObserver.onNext(HmsOperationResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(false)
                    .setMessage("Error applying table: " + e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void applyPartitionBatch(ApplyPartitionBatchRequest request, StreamObserver<HmsOperationResponse> responseObserver) {
        try {
            List<HmsPartitionDto> batch = request.getPartitionsList().stream()
                    .map(HmsConversionHelper::toDto)
                    .toList();

            if (!batch.isEmpty()) {
                hmsClient.executeAs(request.getExecutionPrincipal(), () -> {
                    hmsClient.addPartitions(request.getDbName(), request.getTableName(), batch);
                    return null;
                });
            }

            log.info("[HmsTransferService] Успешно применено {} партиций для {}.{} (doAs={})",
                    batch.size(), request.getDbName(), request.getTableName(), request.getExecutionPrincipal());

            responseObserver.onNext(HmsOperationResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(true)
                    .setAffectedCount(batch.size())
                    .setMessage(String.format("Added/Updated %d partitions", batch.size()))
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[HmsTransferService] Ошибка пакетной вставки партиций для {}.{}: {}",
                    request.getDbName(), request.getTableName(), e.getMessage(), e);
            responseObserver.onNext(HmsOperationResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(false)
                    .setMessage("Error applying partitions: " + e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void reconcileExtraneous(ReconcileExtraneousRequest request, StreamObserver<HmsOperationResponse> responseObserver) {
        try {
            int droppedCount = hmsClient.executeAs(request.getExecutionPrincipal(), () -> {
                int count = 0;
                // Удаление лишних таблиц (deleteData = false)
                for (String extraTbl : request.getDropTablesList()) {
                    log.warn("[HmsTransferService] Reconciliation: удаление лишней таблицы на приемнике: {}.{} (doAs={})",
                            request.getDbName(), extraTbl, request.getExecutionPrincipal());
                    hmsClient.dropTable(request.getDbName(), extraTbl, false);
                    count++;
                }

                // Удаление лишних партиций (deleteData = false)
                if (!request.getTableName().isBlank() && !request.getDropPartitionsList().isEmpty()) {
                    for (HmsDroppedPartitionProto p : request.getDropPartitionsList()) {
                        List<String> vals = p.getValuesList();
                        log.warn("[HmsTransferService] Reconciliation: удаление лишней партиции на приемнике: {}.{} [{}] (doAs={})",
                                request.getDbName(), request.getTableName(), vals, request.getExecutionPrincipal());
                        hmsClient.dropPartition(request.getDbName(), request.getTableName(), vals, false);
                        count++;
                    }
                }
                return count;
            });

            responseObserver.onNext(HmsOperationResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(true)
                    .setAffectedCount(droppedCount)
                    .setMessage("Reconciled successfully, dropped objects: " + droppedCount)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[HmsTransferService] Ошибка согласования лишних объектов: {}", e.getMessage(), e);
            responseObserver.onNext(HmsOperationResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(false)
                    .setMessage("Error reconciling extraneous objects: " + e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getMetadataSnapshot(GetMetadataSnapshotRequest request, StreamObserver<GetMetadataSnapshotResponse> responseObserver) {
        try {
            hmsClient.executeAs(request.getExecutionPrincipal(), () -> {
                boolean dbExists = hmsClient.getAllDatabases().stream()
                        .anyMatch(d -> d.equalsIgnoreCase(request.getDbName()));

                List<String> tables = dbExists ? hmsClient.getAllTables(request.getDbName()) : List.of();
                List<HmsPartitionProto> partitionProtos = new ArrayList<>();
                GetMetadataSnapshotResponse.Builder response = GetMetadataSnapshotResponse.newBuilder()
                        .setDbExists(dbExists)
                        .addAllExistingTables(tables);

                if (request.getPartitionNamesOnly() && !request.getTableName().isBlank()
                        && tables.contains(request.getTableName())) {
                    List<String> names = hmsClient.getPartitionNames(request.getDbName(), request.getTableName());
                    int size = request.getPartitionPageSize() > 0 ? Math.min(request.getPartitionPageSize(), 50_000) : 10_000;
                    int from = Math.min(names.size(), Math.max(0, request.getPartitionPage()) * size);
                    response.addAllExistingPartitionNames(names.subList(from, Math.min(names.size(), from + size)))
                            .setTotalPartitionNames(names.size());
                } else if (!request.getTableName().isBlank() && tables.contains(request.getTableName())) {
                    List<HmsPartitionDto> partitions = request.getSkipPartitions()
                            ? List.of()
                            : hmsClient.getPartitions(request.getDbName(), request.getTableName());
                    for (HmsPartitionDto p : partitions) {
                        partitionProtos.add(HmsConversionHelper.toProto(p));
                    }
                    // Схема нужна источнику для сравнения с его собственной (Schema Drift):
                    // отдаем ее только для адресного запроса, чтобы не обходить всю базу.
                    hmsClient.getTable(request.getDbName(), request.getTableName())
                            .ifPresent(t -> response.addExistingTableSchemas(HmsConversionHelper.toProto(t)));
                }

                responseObserver.onNext(response
                        .addAllExistingPartitions(partitionProtos)
                        .build());
                responseObserver.onCompleted();
                return null;
            });
        } catch (Exception e) {
            log.error("[HmsTransferService] Ошибка получения снимка метаданных: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}
