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
    private final HmsTransferServiceGrpc.HmsTransferServiceBlockingStub blockingStub;

    public HmsPipelineSender(String targetAddress, boolean useTls, boolean insecureSkipVerify) {
        this.targetAddress = targetAddress;

        if (useTls) {
            try {
                var sslContextBuilder = GrpcSslContexts.forClient();
                if (insecureSkipVerify) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                }
                this.channel = NettyChannelBuilder.forTarget(targetAddress)
                        .sslContext(sslContextBuilder.build())
                        .build();
            } catch (SSLException e) {
                throw new RuntimeException("Ошибка инициализации TLS контекста для HMS конвейера к " + targetAddress, e);
            }
        } else {
            this.channel = ManagedChannelBuilder.forTarget(targetAddress)
                    .usePlaintext()
                    .build();
        }

        this.blockingStub = HmsTransferServiceGrpc.newBlockingStub(this.channel);
        log.info("[HmsPipelineSender] Инициализирован gRPC канал к целевому агенту '{}' (TLS={})", targetAddress, useTls);
    }

    public HmsOperationResponse applyDatabase(String jobId, String dbName, String locationUri) {
        ApplyDatabaseRequest req = ApplyDatabaseRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .setLocationUri(locationUri != null ? locationUri : "")
                .build();
        return blockingStub.applyDatabase(req);
    }

    public HmsOperationResponse applyTable(String jobId, HmsTableDto tableDto) {
        ApplyTableRequest req = ApplyTableRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setTable(HmsConversionHelper.toProto(tableDto))
                .build();
        return blockingStub.applyTable(req);
    }

    public HmsOperationResponse applyPartitionBatch(String jobId, String dbName, String tableName, List<HmsPartitionDto> partitions) {
        ApplyPartitionBatchRequest.Builder b = ApplyPartitionBatchRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .setTableName(tableName);

        for (HmsPartitionDto p : partitions) {
            b.addPartitions(HmsConversionHelper.toProto(p));
        }

        return blockingStub.applyPartitionBatch(b.build());
    }

    public HmsOperationResponse reconcileExtraneousTables(String jobId, String dbName, List<String> dropTables) {
        ReconcileExtraneousRequest req = ReconcileExtraneousRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .addAllDropTables(dropTables)
                .build();
        return blockingStub.reconcileExtraneous(req);
    }

    public HmsOperationResponse reconcileExtraneousPartitions(String jobId, String dbName, String tableName, List<List<String>> dropPartVals) {
        ReconcileExtraneousRequest.Builder b = ReconcileExtraneousRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .setTableName(tableName);

        for (List<String> vals : dropPartVals) {
            b.addDropPartitions(HmsDroppedPartitionProto.newBuilder().addAllValues(vals).build());
        }

        return blockingStub.reconcileExtraneous(b.build());
    }

    public GetMetadataSnapshotResponse getMetadataSnapshot(String jobId, String dbName, String tableName) {
        GetMetadataSnapshotRequest req = GetMetadataSnapshotRequest.newBuilder()
                .setJobId(jobId != null ? jobId : "")
                .setDbName(dbName)
                .setTableName(tableName != null ? tableName : "")
                .build();
        return blockingStub.getMetadataSnapshot(req);
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
