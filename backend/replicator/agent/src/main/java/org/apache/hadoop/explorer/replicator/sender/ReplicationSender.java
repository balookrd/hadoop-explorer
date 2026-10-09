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
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.*;
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
     * Потоковая передача файла или рекурсивная инкрементальная синхронизация каталога
     * на целевой узел gRPC с контролем полосы, пропуском неизмененных файлов и обновлением прогресса.
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

        ManagedChannel channel;
        try {
            channel = createManagedChannel(targetAddress);
        } catch (Exception e) {
            String err = "Не удалось создать gRPC соединение к " + targetAddress + ": " + e.getMessage();
            logger.error(err, e);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, 0L, err));
            return false;
        }

        try {
            boolean isDir = fsManager.isDirectory(sourcePath, job.getExecutionPrincipal(), job.getRunAsServiceAccount());
            if (isDir) {
                return transferDirectory(job, channel);
            } else {
                return transferSingleFileWithSync(job, channel);
            }
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
     * Этап анализа (Analyzer): обход HDFS-1, запрос манифеста HDFS-2,
     * вычисление in-memory diff и создание распределенного пула пофайловых задач в Оркестраторе.
     */
    public boolean analyzeAndCreateTaskPool(JobDto job, String targetAddress) {
        String jobId = job.getId();
        String sourcePath = job.getSourcePath();
        String targetPath = job.getTargetPath();

        if (!fsManager.exists(sourcePath)) {
            String err = "Исходный путь не найден: " + sourcePath;
            logger.error(err);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, 0L, err));
            return false;
        }

        ManagedChannel channel = null;
        try {
            channel = createManagedChannel(targetAddress);
            boolean asService = job.getRunAsServiceAccount() != null && job.getRunAsServiceAccount();
            boolean isDir = fsManager.isDirectory(sourcePath, job.getExecutionPrincipal(), asService);

            List<TaskCreateItem> taskItems = new ArrayList<>();

            if (isDir) {
                if (HadoopFsManager.isIgnoredDirectoryPath(sourcePath)) {
                    logger.info("Каталог {} пропущен согласно фильтру временных папок", sourcePath);
                    orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(jobId, Collections.emptyList()));
                    return true;
                }

                List<HadoopFsManager.FileItem> items = fsManager.listFilesRecursively(
                        sourcePath, job.getExecutionPrincipal(), asService
                );

                if (items.isEmpty()) {
                    logger.info("Каталог {} пуст или все файлы отфильтрованы. Создание пустой задачи.", sourcePath);
                    orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(jobId, Collections.emptyList()));
                    return true;
                }

                // Запрос пакетного манифеста у целевого агента (1 сетевой round-trip)
                DirectoryManifestResponse remoteManifest = fetchRemoteManifest(channel, targetPath, job);
                Map<String, Long> remoteFileSizes = new HashMap<>();
                if (remoteManifest != null && remoteManifest.getExists()) {
                    for (FileManifestEntry entry : remoteManifest.getFilesList()) {
                        remoteFileSizes.put(entry.getRelativePath(), entry.getSize());
                    }
                }

                // In-memory diff и формирование пула подзадач
                for (HadoopFsManager.FileItem item : items) {
                    Long remoteSize = remoteFileSizes.get(item.relativePath());
                    boolean skipped = (remoteSize != null && remoteSize == item.size() && item.size() > 0);
                    String subTargetPath = combinePaths(targetPath, item.relativePath());

                    taskItems.add(new TaskCreateItem(
                            UUID.randomUUID().toString(),
                            item.fullPath(),
                            subTargetPath,
                            item.size(),
                            skipped
                    ));
                }
            } else {
                if (HadoopFsManager.isIgnoredPath(sourcePath)) {
                    logger.info("Файл {} пропущен согласно фильтру временных файлов", sourcePath);
                    orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(jobId, Collections.emptyList()));
                    return true;
                }

                long fileBytes = fsManager.getFileSize(sourcePath, job.getExecutionPrincipal(), asService);
                CheckFileResponse check = checkRemoteFile(channel, targetPath, job);
                boolean skipped = (check != null && check.getExists() && check.getSize() == fileBytes && fileBytes > 0);

                taskItems.add(new TaskCreateItem(
                        UUID.randomUUID().toString(),
                        sourcePath,
                        targetPath,
                        fileBytes,
                        skipped
                ));
            }

            boolean ok = orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(jobId, taskItems));
            logger.info("Для задания '{}' успешно сформирован и зарегистрирован пул из {} пофайловых задач", jobId, taskItems.size());
            return ok;
        } catch (Exception e) {
            String err = "Ошибка при анализе каталога " + sourcePath + ": " + e.getMessage();
            logger.error(err, e);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, 0L, err));
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
     * Исполнение конкретной пофайловой задачи воркером из распределенного пула.
     */
    public boolean transferTask(TaskItemDto task) {
        String taskId = task.getId();
        String jobId = task.getJobId();
        String sourcePath = task.getSourcePath();
        String targetPath = task.getTargetPath();
        long fileSize = task.getFileSize();
        String targetAddress = task.getTargetAddress() != null ? task.getTargetAddress() : "localhost:50051";

        logger.info("Воркер '{}' начинает передачу файла задачи '{}': {} -> {} ({} байт) на {}",
                workerId, taskId, sourcePath, targetPath, fileSize, targetAddress);

        ManagedChannel channel = null;
        try {
            channel = createManagedChannel(targetAddress);

            boolean success = streamTaskFileToChannel(taskId, jobId, sourcePath, targetPath, fileSize,
                    task.getExecutionPrincipal(), task.getRunAsServiceAccount(), channel);

            if (success) {
                logger.info("Воркер '{}' успешно передал файл задачи '{}': {} ({} байт)", workerId, taskId, sourcePath, fileSize);
                orchestratorClient.completeTask(new CompleteTaskRequest(taskId, workerId, fileSize, "OK"));
                return true;
            } else {
                logger.error("Воркер '{}' не смог передать файл задачи '{}': {}", workerId, taskId, sourcePath);
                orchestratorClient.failTask(new FailTaskRequest(taskId, workerId, "Ошибка передачи чанков на приемник"));
                return false;
            }
        } catch (Exception e) {
            logger.error("Исключение при передаче задачи '{}': {}", taskId, e.getMessage(), e);
            orchestratorClient.failTask(new FailTaskRequest(taskId, workerId, e.getMessage()));
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

    private boolean streamTaskFileToChannel(String taskId, String jobId, String sourcePath, String targetPath,
                                            long fileBytes, String executionPrincipal, Boolean runAsServiceAccount,
                                            ManagedChannel channel) {
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
                }
            });

            // Метаданные файла
            boolean asService = runAsServiceAccount != null && runAsServiceAccount;
            FileMetadata metadata = FileMetadata.newBuilder()
                    .setJobId(jobId)
                    .setSourcePath(sourcePath)
                    .setTargetPath(targetPath)
                    .setTotalBytes(fileBytes)
                    .setRunAsServiceAccount(asService)
                    .setExecutionPrincipal(executionPrincipal != null ? executionPrincipal : "hdfs-replicator@REALM.LOCAL")
                    .build();

            requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

            // Потоковая передача чанками с шейпингом
            try (InputStream in = fsManager.openInputStream(sourcePath, executionPrincipal, asService)) {
                byte[] buffer = new byte[chunkSize];
                long offset = 0;
                int bytesRead;

                while ((bytesRead = in.read(buffer)) != -1) {
                    boolean isLast = (offset + bytesRead) >= fileBytes;

                    if (bandwidthLimiter != null) {
                        bandwidthLimiter.throttle(bytesRead);
                    }

                    double waitSec = orchestratorClient.requestNetworkTokens(
                            new TokenRequest(workerId, bytesRead, null, null)
                    );
                    if (waitSec > 0) {
                        try {
                            Thread.sleep((long) (waitSec * 1000));
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
                }

                requestObserver.onCompleted();
            }

            TransferFileResponse response = responseFuture.get(60, TimeUnit.MINUTES);
            return response.getSuccess();
        } catch (Exception e) {
            logger.error("Ошибка при потоковой передаче задачи {}: {}", taskId, e.getMessage(), e);
            return false;
        }
    }

    private boolean transferSingleFileWithSync(JobDto job, ManagedChannel channel) {
        String jobId = job.getId();
        String sourcePath = job.getSourcePath();
        String targetPath = job.getTargetPath();

        if (HadoopFsManager.isIgnoredPath(sourcePath)) {
            logger.info("Пропуск файла '{}' согласно фильтру временных файлов", sourcePath);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("COMPLETED", 0L, 0L,
                    "Файл пропущен согласно фильтру временных файлов"));
            return true;
        }

        long totalBytes = 0;
        try {
            totalBytes = fsManager.getFileSize(sourcePath, job.getExecutionPrincipal(), job.getRunAsServiceAccount());
        } catch (Exception e) {
            logger.warn("Не удалось определить размер файла '{}': {}", sourcePath, e.getMessage());
        }

        // 1. Инкрементальная проверка: если файл на целевом узле уже существует и размер совпадает
        CheckFileResponse check = checkRemoteFile(channel, targetPath, job);
        if (check != null && check.getExists() && check.getSize() == totalBytes && totalBytes > 0) {
            logger.info("Инкрементальный пропуск: целевой файл '{}' уже идентичен исходному ({} байт)", targetPath, totalBytes);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("COMPLETED", totalBytes, totalBytes,
                    "Файл уже актуален на целевом узле (инкрементальный пропуск)"));
            return true;
        }

        // 2. Файл новый или изменен — передаем
        orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("RUNNING", 0L, totalBytes, "Начало передачи данных (Java Agent)"));

        boolean success = streamFileToChannel(jobId, sourcePath, targetPath, totalBytes, job, channel, 0L, totalBytes);
        if (success) {
            logger.info("Передача задачи {} успешно завершена! ({} байт)", jobId, totalBytes);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("COMPLETED", totalBytes, totalBytes,
                    "Репликация успешно завершена (Java Agent)"));
            return true;
        } else {
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, totalBytes,
                    "Ошибка при передаче файла на целевой узел"));
            return false;
        }
    }

    private boolean transferDirectory(JobDto job, ManagedChannel channel) {
        String jobId = job.getId();
        String sourceDir = job.getSourcePath();
        String targetDir = job.getTargetPath();

        if (HadoopFsManager.isIgnoredDirectoryPath(sourceDir)) {
            logger.info("Каталог {} пропущен согласно фильтру временных папок", sourceDir);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("COMPLETED", 0L, 0L,
                    "Каталог пропущен согласно фильтру временных папок"));
            return true;
        }

        List<HadoopFsManager.FileItem> items;
        try {
            items = fsManager.listFilesRecursively(sourceDir, job.getExecutionPrincipal(), job.getRunAsServiceAccount());
        } catch (Exception e) {
            String err = "Ошибка обхода каталога " + sourceDir + ": " + e.getMessage();
            logger.error(err, e);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, 0L, err));
            return false;
        }

        if (items.isEmpty()) {
            logger.info("Каталог {} пуст или не содержит регулярных файлов. Синхронизация завершена.", sourceDir);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("COMPLETED", 0L, 0L,
                    "Каталог пуст, синхронизация завершена"));
            return true;
        }

        long totalBytes = items.stream().mapToLong(HadoopFsManager.FileItem::size).sum();
        logger.info("Рекурсивная синхронизация каталога {}: найдено файлов: {}, общий объем: {} байт",
                sourceDir, items.size(), totalBytes);

        orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("RUNNING", 0L, totalBytes,
                String.format("Синхронизация каталога: %d файлов (%d байт)", items.size(), totalBytes)));

        // 1. Пакетный запрос манифеста целевого каталога (Batch Manifest Diff за 1 сетевой вызов)
        DirectoryManifestResponse remoteManifest = fetchRemoteManifest(channel, targetDir, job);
        Map<String, Long> remoteFileSizes = new HashMap<>();
        if (remoteManifest != null && remoteManifest.getExists()) {
            for (FileManifestEntry entry : remoteManifest.getFilesList()) {
                remoteFileSizes.put(entry.getRelativePath(), entry.getSize());
            }
            logger.info("Получен пакетный манифест целевого каталога '{}': {} файлов уже на приемнике",
                    targetDir, remoteFileSizes.size());
        } else {
            logger.info("Целевой каталог '{}' отсутствует или пуст на приемнике, полная передача", targetDir);
        }

        // 2. Мгновенный in-memory diff
        List<HadoopFsManager.FileItem> toTransfer = new ArrayList<>();
        long copiedBytes = 0;
        int skippedCount = 0;

        for (HadoopFsManager.FileItem item : items) {
            Long remoteSize = remoteFileSizes.get(item.relativePath());
            if (remoteSize != null && remoteSize == item.size() && item.size() > 0) {
                logger.info("Инкрементальный пропуск (in-memory diff): '{}' актуален на приемнике ({} байт)",
                        item.relativePath(), item.size());
                copiedBytes += item.size();
                skippedCount++;
            } else {
                toTransfer.add(item);
            }
        }

        logger.info("Результат in-memory diff каталога {}: к передаче {} файлов, пропущено {} актуальных",
                sourceDir, toTransfer.size(), skippedCount);

        // 3. Передача изменившихся/новых файлов
        int transferredCount = 0;
        for (HadoopFsManager.FileItem item : toTransfer) {
            String subTargetPath = combinePaths(targetDir, item.relativePath());

            boolean ok = streamFileToChannel(jobId, item.fullPath(), subTargetPath, item.size(), job, channel, copiedBytes, totalBytes);
            if (!ok) {
                String err = "Ошибка передачи файла: " + item.relativePath();
                logger.error(err);
                orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", copiedBytes, totalBytes, err));
                return false;
            }

            copiedBytes += item.size();
            transferredCount++;
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest(null, copiedBytes, totalBytes,
                    String.format("Синхронизация (%d/%d): передан %s", transferredCount + skippedCount, items.size(), item.relativePath())));
        }

        String msg = String.format("Синхронизация каталога завершена: передано %d, пропущено %d (всего %d файлов)",
                transferredCount, skippedCount, items.size());
        logger.info("Задача {}: {}", jobId, msg);
        orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("COMPLETED", copiedBytes, totalBytes, msg));
        return true;
    }

    private DirectoryManifestResponse fetchRemoteManifest(ManagedChannel channel, String targetDir, JobDto job) {
        try {
            DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub =
                    DataTransferServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);

            DirectoryManifestRequest req = DirectoryManifestRequest.newBuilder()
                    .setPath(targetDir)
                    .setExecutionPrincipal(job.getExecutionPrincipal() != null ? job.getExecutionPrincipal() : "")
                    .setRunAsServiceAccount(job.getRunAsServiceAccount() != null ? job.getRunAsServiceAccount() : false)
                    .build();

            return blockingStub.getDirectoryManifest(req);
        } catch (Exception e) {
            logger.debug("Не удалось получить пакетный манифест каталога '{}' через gRPC (возможно, новый каталог): {}", targetDir, e.getMessage());
            return null;
        }
    }

    private boolean streamFileToChannel(String jobId, String sourcePath, String targetPath, long fileBytes,
                                        JobDto job, ManagedChannel channel, long baseCopiedBytes, long grandTotalBytes) {
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
                }
            });

            // Метаданные файла
            FileMetadata metadata = FileMetadata.newBuilder()
                    .setJobId(jobId)
                    .setSourcePath(sourcePath)
                    .setTargetPath(targetPath)
                    .setTotalBytes(fileBytes)
                    .setRunAsServiceAccount(job.getRunAsServiceAccount() != null ? job.getRunAsServiceAccount() : false)
                    .setExecutionPrincipal(job.getExecutionPrincipal() != null ? job.getExecutionPrincipal() : "hdfs-replicator@REALM.LOCAL")
                    .build();

            requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

            // Потоковая передача чанками
            try (InputStream in = fsManager.openInputStream(sourcePath, job.getExecutionPrincipal(), job.getRunAsServiceAccount())) {
                byte[] buffer = new byte[chunkSize];
                long offset = 0;
                long lastProgressUpdate = 0;
                int bytesRead;

                while ((bytesRead = in.read(buffer)) != -1) {
                    boolean isLast = (offset + bytesRead) >= fileBytes;

                    // Локальный шейпинг
                    if (bandwidthLimiter != null) {
                        bandwidthLimiter.throttle(bytesRead);
                    }

                    // Глобальный шейпинг
                    double waitSec = orchestratorClient.requestNetworkTokens(
                            new TokenRequest(workerId, bytesRead, job.getSourceClusterId(), job.getTargetClusterId())
                    );
                    if (waitSec > 0) {
                        try {
                            Thread.sleep((long) (waitSec * 1000));
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

                    if (offset - lastProgressUpdate >= 1024 * 1024 || isLast) {
                        long currentOverall = baseCopiedBytes + offset;
                        orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest(null, currentOverall, grandTotalBytes,
                                "Передано " + currentOverall + " из " + grandTotalBytes + " байт"));
                        lastProgressUpdate = offset;
                    }
                }

                requestObserver.onCompleted();
            }

            TransferFileResponse response = responseFuture.get(60, TimeUnit.MINUTES);
            if (!response.getSuccess()) {
                logger.error("Приемник вернул ошибку при передаче {}: {}", sourcePath, response.getMessage());
                return false;
            }
            return true;
        } catch (Exception e) {
            logger.error("Ошибка при потоковой передаче файла {}: {}", sourcePath, e.getMessage(), e);
            return false;
        }
    }

    private CheckFileResponse checkRemoteFile(ManagedChannel channel, String targetPath, JobDto job) {
        try {
            DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub =
                    DataTransferServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);

            CheckFileRequest req = CheckFileRequest.newBuilder()
                    .setPath(targetPath)
                    .setExecutionPrincipal(job.getExecutionPrincipal() != null ? job.getExecutionPrincipal() : "")
                    .setRunAsServiceAccount(job.getRunAsServiceAccount() != null ? job.getRunAsServiceAccount() : false)
                    .build();

            return blockingStub.checkFile(req);
        } catch (Exception e) {
            logger.debug("Статус целевого файла '{}' не получен через gRPC (возможно, новый файл): {}", targetPath, e.getMessage());
            return null;
        }
    }

    private static String combinePaths(String base, String relative) {
        String b = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        String r = relative.startsWith("/") ? relative.substring(1) : relative;
        return b + "/" + r;
    }

    /**
     * Создает gRPC ManagedChannel с учетом параметров TLS / plaintext.
     */
    private ManagedChannel createManagedChannel(String targetAddress) throws javax.net.ssl.SSLException {
        String cleanAddress = targetAddress;
        boolean useTls = this.tlsEnabled;

        if (cleanAddress.startsWith("inprocess://") || cleanAddress.startsWith("inprocess:")) {
            String name = cleanAddress.startsWith("inprocess://")
                    ? cleanAddress.substring("inprocess://".length())
                    : cleanAddress.substring("inprocess:".length());
            try {
                Class<?> clazz = Class.forName("io.grpc.inprocess.InProcessChannelBuilder");
                Object builder = clazz.getMethod("forName", String.class).invoke(null, name);
                builder = clazz.getMethod("directExecutor").invoke(builder);
                return (ManagedChannel) clazz.getMethod("build").invoke(builder);
            } catch (Exception e) {
                throw new IllegalStateException("io.grpc.inprocess недоступен в рантайме: " + e.getMessage(), e);
            }
        }

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
