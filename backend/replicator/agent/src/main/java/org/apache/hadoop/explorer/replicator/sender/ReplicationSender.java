package org.apache.hadoop.explorer.replicator.sender;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.model.TokenRequest;
import org.apache.hadoop.explorer.replicator.model.UpdateJobRequest;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Клиент передачи файлов чанками с многоуровневым контролем полосы (Sender).
 */
public class ReplicationSender {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationSender.class);

    private final String workerId;
    private final OrchestratorClient orchestratorClient;
    private final HadoopFsManager fsManager;
    private final LocalBandwidthLimiter bandwidthLimiter;
    private final int chunkSize;
    private final boolean tlsEnabled;
    private final String trustCertCollectionPath;
    private final String clientCertChainPath;
    private final String clientPrivateKeyPath;
    private final boolean insecureSkipVerify;

    public ReplicationSender(String workerId, OrchestratorClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize) {
        this(workerId, orchestratorClient, fsManager, bandwidthLimiter, chunkSize, false, null, null, null, false);
    }

    public ReplicationSender(String workerId, OrchestratorClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize,
                             boolean tlsEnabled, String trustCertCollectionPath,
                             String clientCertChainPath, String clientPrivateKeyPath,
                             boolean insecureSkipVerify) {
        this.workerId = workerId;
        this.orchestratorClient = orchestratorClient;
        this.fsManager = fsManager;
        this.bandwidthLimiter = bandwidthLimiter;
        this.chunkSize = chunkSize > 0 ? chunkSize : 64 * 1024;
        this.tlsEnabled = tlsEnabled;
        this.trustCertCollectionPath = trustCertCollectionPath;
        this.clientCertChainPath = clientCertChainPath;
        this.clientPrivateKeyPath = clientPrivateKeyPath;
        this.insecureSkipVerify = insecureSkipVerify;
    }

    /**
     * Потоковая передача файла на целевой узел gRPC с контролем полосы и обновлением прогресса.
     */
    public boolean transferFile(JobDto job, String targetAddress) {
        String jobId = job.getId();
        String sourcePath = job.getSourcePath();
        String targetPath = job.getTargetPath();

        logger.info("Агент '{}' начинает передачу задачи {}: '{}' -> '{}' (целевой узел: {})",
                workerId, jobId, sourcePath, targetPath, targetAddress);

        if (!fsManager.exists(sourcePath)) {
            boolean autoCreate = Boolean.parseBoolean(System.getenv().getOrDefault("REPLICATOR_AUTO_CREATE_TEST_DATA", "true"));
            if (autoCreate) {
                try {
                    File file = new File(sourcePath);
                    if (file.getParentFile() != null && !file.getParentFile().exists()) {
                        file.getParentFile().mkdirs();
                    }
                    long sizeToGenerate = (job.getTotalBytes() != null && job.getTotalBytes() > 0) ? job.getTotalBytes() : 2 * 1024 * 1024;
                    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(file)) {
                        byte[] dummy = new byte[64 * 1024];
                        java.util.Arrays.fill(dummy, (byte) 'A');
                        long written = 0;
                        while (written < sizeToGenerate) {
                            int toWrite = (int) Math.min(dummy.length, sizeToGenerate - written);
                            fos.write(dummy, 0, toWrite);
                            written += toWrite;
                        }
                    }
                    logger.info("Демо-режим: сгенерирован тестовый файл для репликации: {} ({} байт)", sourcePath, sizeToGenerate);
                } catch (Exception ex) {
                    logger.warn("Не удалось сгенерировать демо-файл {}: {}", sourcePath, ex.getMessage());
                }
            }
        }

        if (!fsManager.exists(sourcePath)) {
            String err = "Исходный файл не найден: " + sourcePath;
            logger.error(err);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, 0L, err));
            return false;
        }

        long totalBytes = 0;
        try {
            totalBytes = fsManager.getFileSize(sourcePath, job.getExecutionPrincipal(), job.getRunAsServiceAccount());
        } catch (Exception e) {
            logger.warn("Не удалось определить размер файла '{}': {}", sourcePath, e.getMessage());
        }

        // Обновляем статус RUNNING
        orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("RUNNING", 0L, totalBytes, "Начало передачи данных (Java Agent)"));

        ManagedChannel channel;
        try {
            channel = createManagedChannel(targetAddress);
        } catch (Exception e) {
            String err = "Не удалось создать gRPC соединение к " + targetAddress + ": " + e.getMessage();
            logger.error(err, e);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, totalBytes, err));
            return false;
        }

        CompletableFuture<TransferFileResponse> responseFuture = new CompletableFuture<>();

        try {
            DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);

            StreamObserver<TransferFileRequest> requestObserver = stub.transferFile(new StreamObserver<>() {
                @Override
                public void onNext(TransferFileResponse value) {
                    responseFuture.complete(value);
                }

                @Override
                public void onError(Throwable t) {
                    responseFuture.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                    // responseFuture уже завершен в onNext
                }
            });

            // 1. Отправка метаданных файла
            FileMetadata metadata = FileMetadata.newBuilder()
                    .setJobId(jobId)
                    .setSourcePath(sourcePath)
                    .setTargetPath(targetPath)
                    .setTotalBytes(totalBytes)
                    .setRunAsServiceAccount(job.getRunAsServiceAccount())
                    .setExecutionPrincipal(job.getExecutionPrincipal() != null ? job.getExecutionPrincipal() : "hdfs-replicator@REALM.LOCAL")
                    .build();

            requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

            // 2. Чтение и потоковая отправка файла чанками с doAs имперсонацией
            try (InputStream in = fsManager.openInputStream(sourcePath, job.getExecutionPrincipal(), job.getRunAsServiceAccount())) {
                byte[] buffer = new byte[chunkSize];
                long offset = 0;
                long lastProgressUpdate = 0;
                int bytesRead;

                while ((bytesRead = in.read(buffer)) != -1) {
                    boolean isLast = (offset + bytesRead) >= totalBytes;

                    // Шейпинг полосы (локальный Token Bucket)
                    if (bandwidthLimiter != null) {
                        bandwidthLimiter.throttle(bytesRead);
                    }

                    // Шейпинг полосы (глобальный Orchestrator Token Bucket)
                    double orchestratorWait = orchestratorClient.requestNetworkTokens(
                            new TokenRequest(workerId, bytesRead, job.getSourceClusterId(), job.getTargetClusterId())
                    );
                    if (orchestratorWait > 0) {
                        try {
                            Thread.sleep((long) (orchestratorWait * 1000));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    FileChunk chunk = FileChunk.newBuilder()
                            .setJobId(jobId)
                            .setOffset(offset)
                            .setData(ByteString.copyFrom(buffer, 0, bytesRead))
                            .setIsLastChunk(isLast)
                            .build();

                    requestObserver.onNext(TransferFileRequest.newBuilder().setChunk(chunk).build());

                    offset += bytesRead;

                    // Периодически обновляем прогресс в Оркестраторе (каждые 1 МБ или в конце)
                    if (offset - lastProgressUpdate >= 1024 * 1024 || isLast) {
                        orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest(null, offset, totalBytes,
                                "Передано " + offset + " из " + totalBytes + " байт"));
                        lastProgressUpdate = offset;
                    }
                }

                requestObserver.onCompleted();
            }

            // Ожидаем ответ приемника
            TransferFileResponse response = responseFuture.get(60, TimeUnit.MINUTES);
            if (response.getSuccess()) {
                logger.info("Передача задачи {} успешно завершена! Записано {} байт, sha256={}",
                        jobId, response.getBytesWritten(), response.getChecksum());
                orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("COMPLETED", response.getBytesWritten(), totalBytes,
                        "Репликация успешно завершена (Java Agent)"));
                return true;
            } else {
                logger.error("Ошибка на стороне приемника для задачи {}: {}", jobId, response.getMessage());
                orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", response.getBytesWritten(), totalBytes,
                        "Ошибка приемника: " + response.getMessage()));
                return false;
            }

        } catch (Exception e) {
            logger.error("Исключение при передаче файла задачи {}: {}", jobId, e.getMessage(), e);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", null, totalBytes,
                    "Ошибка gRPC передачи: " + e.getMessage()));
            return false;
        } finally {
            if (channel != null) {
                channel.shutdown();
                try {
                    channel.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    /**
     * Создает gRPC ManagedChannel с учетом параметров TLS / plaintext.
     */
    private ManagedChannel createManagedChannel(String targetAddress) throws javax.net.ssl.SSLException {
        String cleanAddress = targetAddress;
        boolean useTls = this.tlsEnabled;

        if (cleanAddress.startsWith("grpcs://")) {
            cleanAddress = cleanAddress.substring("grpcs://".length());
            useTls = true;
        } else if (cleanAddress.startsWith("grpc://")) {
            cleanAddress = cleanAddress.substring("grpc://".length());
        }

        if (useTls) {
            SslContextBuilder sslBuilder = GrpcSslContexts.forClient();
            if (insecureSkipVerify) {
                sslBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else if (trustCertCollectionPath != null && !trustCertCollectionPath.isBlank()) {
                sslBuilder.trustManager(new File(trustCertCollectionPath));
            }

            if (clientCertChainPath != null && !clientCertChainPath.isBlank()
                    && clientPrivateKeyPath != null && !clientPrivateKeyPath.isBlank()) {
                sslBuilder.keyManager(new File(clientCertChainPath), new File(clientPrivateKeyPath));
            }

            logger.info("Создание защищенного gRPC TLS канала к '{}' (insecureSkipVerify={})", cleanAddress, insecureSkipVerify);
            return NettyChannelBuilder.forTarget(cleanAddress)
                    .sslContext(sslBuilder.build())
                    .maxInboundMessageSize(64 * 1024 * 1024)
                    .build();
        } else {
            return NettyChannelBuilder.forTarget(cleanAddress)
                    .usePlaintext()
                    .maxInboundMessageSize(64 * 1024 * 1024)
                    .build();
        }
    }
}
