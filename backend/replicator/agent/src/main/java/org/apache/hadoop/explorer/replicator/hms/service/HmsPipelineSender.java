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

    public HmsPipelineSender(String targetAddress, boolean useTls, boolean insecureSkipVerify) {
        this(targetAddress, useTls, insecureSkipVerify, null);
    }

    public HmsPipelineSender(String targetAddress, boolean useTls, boolean insecureSkipVerify, String agentSecret) {
        this(targetAddress, useTls, insecureSkipVerify, agentSecret, 60L);
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
                this.channel = builder.build();
            } catch (SSLException e) {
                throw new RuntimeException("Ошибка инициализации TLS контекста для HMS конвейера к " + targetAddress, e);
            }
        } else {
            var builder = NettyChannelBuilder.forTarget(targetAddress)
                    .usePlaintext();
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
        ApplyTableRequest.Builder req = ApplyTableRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setTable(HmsConversionHelper.toProto(tableDto));
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
