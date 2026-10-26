package org.apache.hadoop.explorer.replicator.hms.service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Клиент прямого агентного конвейера метаданных Hive Metastore (Source Agent -> Target Agent).
 * Управляет защищенным gRPC TLS каналом передачи DDL-команд.
 */
public class HmsPipelineSender implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(HmsPipelineSender.class);

    private final String targetAddress;
    private final ManagedChannel channel;
    private final HmsTransferServiceGrpc.HmsTransferServiceBlockingStub baseStub;
    private final long defaultTimeoutSeconds;

    /** Предел ответа приемника; по умолчанию gRPC принимает лишь 4 МБ. */
    private static final int MAX_INBOUND_MESSAGE_BYTES = 64 * 1024 * 1024;

    public HmsPipelineSender(String targetAddress, boolean useTls, boolean insecureSkipVerify) {
        this(targetAddress, useTls, insecureSkipVerify, null);
    }

    public HmsPipelineSender(String targetAddress, boolean useTls, boolean insecureSkipVerify, String agentSecret) {
        // Пачка в 1000 партиций с alter на существующие на большом метасторе идет дольше минуты
        this(targetAddress, useTls, insecureSkipVerify, agentSecret, 300L);
    }

    public HmsPipelineSender(String targetAddress, boolean useTls, boolean insecureSkipVerify, String agentSecret, long defaultTimeoutSeconds) {
        this.targetAddress = targetAddress;
        this.defaultTimeoutSeconds = defaultTimeoutSeconds > 0 ? defaultTimeoutSeconds : 60L;

        if (useTls) {
            try {
                var sslContextBuilder = GrpcSslContexts.forClient();
                if (insecureSkipVerify) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                }
                var builder = NettyChannelBuilder.forTarget(targetAddress)
                        .sslContext(sslContextBuilder.build());
                if (agentSecret != null && !agentSecret.isBlank()) {
                    builder.intercept(org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor.createClientInterceptor(agentSecret));
                }
                builder.intercept(org.apache.hadoop.explorer.replicator.telemetry.GrpcTracing.clientInterceptor());
                builder.maxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES);
                this.channel = builder.build();
            } catch (SSLException e) {
                throw new RuntimeException("Ошибка инициализации TLS контекста для HMS конвейера к " + targetAddress, e);
            }
        } else {
            var builder = NettyChannelBuilder.forTarget(targetAddress)
                    .usePlaintext()
                    .maxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES);
            if (agentSecret != null && !agentSecret.isBlank()) {
                builder.intercept(org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor.createClientInterceptor(agentSecret));
            }
            this.channel = builder.build();
        }

        this.baseStub = HmsTransferServiceGrpc.newBlockingStub(this.channel);
        log.info("[HmsPipelineSender] Инициализирован gRPC канал к целевому агенту '{}' (TLS={}, timeout={}s)",
                targetAddress, useTls, this.defaultTimeoutSeconds);
    }

    private HmsTransferServiceGrpc.HmsTransferServiceBlockingStub stub() {
        return baseStub.withDeadlineAfter(defaultTimeoutSeconds, TimeUnit.SECONDS);
    }

    public HmsTransferServiceGrpc.HmsTransferServiceBlockingStub stub(long timeoutSeconds) {
        return baseStub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
    }

    public HmsOperationResponse applyDatabase(String jobId, String dbName, String locationUri) {
        return applyDatabase(jobId, dbName, locationUri, null);
    }

    public HmsOperationResponse applyDatabase(String jobId, String dbName, String locationUri, String executionPrincipal) {
        ApplyDatabaseRequest.Builder req = ApplyDatabaseRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .setLocationUri(locationUri != null ? locationUri : "");
        if (executionPrincipal != null && !executionPrincipal.isBlank()) {
            req.setExecutionPrincipal(executionPrincipal);
        }
        return stub().applyDatabase(req.build());
    }

    public HmsOperationResponse applyTable(String jobId, HmsTableDto tableDto) {
        return applyTable(jobId, tableDto, null);
    }

    public HmsOperationResponse applyTable(String jobId, HmsTableDto tableDto, String executionPrincipal) {
        return applyTable(jobId, tableDto, executionPrincipal, null);
    }

    /** Применение таблицы; {@code renameFrom} — прежнее имя, если таблицу переименовали. */
    public HmsOperationResponse applyTable(String jobId, HmsTableDto tableDto, String executionPrincipal, String renameFrom) {
        ApplyTableRequest.Builder req = ApplyTableRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setTable(HmsConversionHelper.toProto(tableDto));
        if (renameFrom != null && !renameFrom.isBlank()) {
            req.setRenameFrom(renameFrom);
        }
        if (executionPrincipal != null && !executionPrincipal.isBlank()) {
            req.setExecutionPrincipal(executionPrincipal);
        }
        return stub().applyTable(req.build());
    }

    public HmsOperationResponse applyPartitionBatch(String jobId, String dbName, String tableName, List<HmsPartitionDto> partitions) {
        return applyPartitionBatch(jobId, dbName, tableName, partitions, null);
    }

    public HmsOperationResponse applyPartitionBatch(String jobId, String dbName, String tableName, List<HmsPartitionDto> partitions, String executionPrincipal) {
        ApplyPartitionBatchRequest.Builder b = ApplyPartitionBatchRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .setTableName(tableName);
        if (executionPrincipal != null && !executionPrincipal.isBlank()) {
            b.setExecutionPrincipal(executionPrincipal);
        }

        for (HmsPartitionDto p : partitions) {
            b.addPartitions(HmsConversionHelper.toProto(p));
        }

        return stub().applyPartitionBatch(b.build());
    }

    public HmsOperationResponse reconcileExtraneousTables(String jobId, String dbName, List<String> dropTables) {
        return reconcileExtraneousTables(jobId, dbName, dropTables, null);
    }

    public HmsOperationResponse reconcileExtraneousTables(String jobId, String dbName, List<String> dropTables, String executionPrincipal) {
        ReconcileExtraneousRequest.Builder req = ReconcileExtraneousRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .addAllDropTables(dropTables);
        if (executionPrincipal != null && !executionPrincipal.isBlank()) {
            req.setExecutionPrincipal(executionPrincipal);
        }
        return stub().reconcileExtraneous(req.build());
    }

    public HmsOperationResponse reconcileExtraneousPartitions(String jobId, String dbName, String tableName, List<List<String>> dropPartVals) {
        return reconcileExtraneousPartitions(jobId, dbName, tableName, dropPartVals, null);
    }

    public HmsOperationResponse reconcileExtraneousPartitions(String jobId, String dbName, String tableName, List<List<String>> dropPartVals, String executionPrincipal) {
        ReconcileExtraneousRequest.Builder b = ReconcileExtraneousRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .setTableName(tableName);
        if (executionPrincipal != null && !executionPrincipal.isBlank()) {
            b.setExecutionPrincipal(executionPrincipal);
        }

        for (List<String> vals : dropPartVals) {
            b.addDropPartitions(HmsDroppedPartitionProto.newBuilder().addAllValues(vals).build());
        }

        return stub().reconcileExtraneous(b.build());
    }

    /** Схема таблицы приемника без ее партиций (проверка дрейфа). */
    public GetMetadataSnapshotResponse getTableSchemaSnapshot(String jobId, String dbName, String tableName,
                                                              String executionPrincipal) {
        GetMetadataSnapshotRequest.Builder req = GetMetadataSnapshotRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .setTableName(tableName != null ? tableName : "")
                .setSkipPartitions(true);
        if (executionPrincipal != null && !executionPrincipal.isBlank()) {
            req.setExecutionPrincipal(executionPrincipal);
        }
        return stub().getMetadataSnapshot(req.build());
    }

    /** Размер страницы имен партиций: 10k имен — сотни килобайт, далеко от лимита сообщения. */
    private static final int PARTITION_NAMES_PAGE = 10_000;

    /**
     * Все имена партиций таблицы приемника, постранично.
     *
     * <p>Полный список партиций одним ответом на таблице в 200k партиций — десятки мегабайт,
     * он не проходил лимит gRPC, а ошибку проглатывали: лишние партиции молча не удалялись.
     */
    public List<String> listTargetPartitionNames(String jobId, String dbName, String tableName, String executionPrincipal) {
        List<String> names = new java.util.ArrayList<>();
        for (int page = 0; ; page++) {
            GetMetadataSnapshotRequest.Builder req = GetMetadataSnapshotRequest.newBuilder()
                    .setJobId(jobId != null ? jobId : "")
                    .setDbName(dbName)
                    .setTableName(tableName)
                    .setPartitionNamesOnly(true)
                    .setPartitionPage(page)
                    .setPartitionPageSize(PARTITION_NAMES_PAGE);
            if (executionPrincipal != null && !executionPrincipal.isBlank()) {
                req.setExecutionPrincipal(executionPrincipal);
            }
            GetMetadataSnapshotResponse resp = stub().getMetadataSnapshot(req.build());
            names.addAll(resp.getExistingPartitionNamesList());
            if (resp.getExistingPartitionNamesCount() == 0 || names.size() >= resp.getTotalPartitionNames()) {
                return names;
            }
        }
    }

    /** Файлы каталога приемника относительно его корня (для зачистки после INSERT OVERWRITE). */
    public java.util.Optional<List<String>> listTargetFiles(String path, String executionPrincipal) {
        var req = org.apache.hadoop.explorer.replicator.generated.DirectoryManifestRequest.newBuilder()
                .setPath(path)
                .setExecutionPrincipal(executionPrincipal != null ? executionPrincipal : "")
                .setRunAsServiceAccount(executionPrincipal == null || executionPrincipal.isBlank())
                .build();
        var resp = dataStub().getDirectoryManifest(req);
        if (!resp.getExists()) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(resp.getFilesList().stream()
                .map(org.apache.hadoop.explorer.replicator.generated.FileManifestEntry::getRelativePath)
                .toList());
    }

    /** Удаление файла на приемнике. */
    public boolean deleteTargetPath(String jobId, String path, String executionPrincipal) {
        var req = org.apache.hadoop.explorer.replicator.generated.DeletePathRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setPath(path)
                .setRecursive(false)
                .setExecutionPrincipal(executionPrincipal != null ? executionPrincipal : "")
                .setRunAsServiceAccount(executionPrincipal == null || executionPrincipal.isBlank())
                .build();
        return dataStub().deletePath(req).getSuccess();
    }

    private org.apache.hadoop.explorer.replicator.generated.DataTransferServiceGrpc.DataTransferServiceBlockingStub dataStub() {
        return org.apache.hadoop.explorer.replicator.generated.DataTransferServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(defaultTimeoutSeconds, TimeUnit.SECONDS);
    }

    public GetMetadataSnapshotResponse getMetadataSnapshot(String jobId, String dbName, String tableName) {
        return getMetadataSnapshot(jobId, dbName, tableName, null);
    }

    public GetMetadataSnapshotResponse getMetadataSnapshot(String jobId, String dbName, String tableName, String executionPrincipal) {
        GetMetadataSnapshotRequest.Builder req = GetMetadataSnapshotRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .setTableName(tableName != null ? tableName : "");
        if (executionPrincipal != null && !executionPrincipal.isBlank()) {
            req.setExecutionPrincipal(executionPrincipal);
        }
        return stub().getMetadataSnapshot(req.build());
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
