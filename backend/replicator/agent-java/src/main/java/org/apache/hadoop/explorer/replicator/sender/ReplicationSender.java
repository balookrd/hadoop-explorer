package org.apache.hadoop.explorer.replicator.sender;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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

    public ReplicationSender(String workerId, OrchestratorClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize) {
        this.workerId = workerId;
        this.orchestratorClient = orchestratorClient;
        this.fsManager = fsManager;
        this.bandwidthLimiter = bandwidthLimiter;
        this.chunkSize = chunkSize > 0 ? chunkSize : 64 * 1024;
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

        ManagedChannel channel = ManagedChannelBuilder.forTarget(targetAddress)
                .usePlaintext()
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build();

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
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
        }
    }
}
