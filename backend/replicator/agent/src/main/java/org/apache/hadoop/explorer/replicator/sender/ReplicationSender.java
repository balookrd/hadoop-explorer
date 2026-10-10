package org.apache.hadoop.explorer.replicator.sender;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.StreamObserver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.compression.WireCompressor;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Клиент передачи файлов чанками с многоуровневым контролем полосы (Sender).
 */
public class ReplicationSender {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationSender.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

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
    private final long smallFileThresholdBytes;
    private final long bundleTargetSizeBytes;
    private final int maxBundleFiles;
    private final CompressionCodec defaultCompressionCodec;
    private final int compressionLevel;

    public ReplicationSender(String workerId, OrchestratorClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize) {
        this(workerId, orchestratorClient, fsManager, bandwidthLimiter, chunkSize, false, null, null, null, false);
    }

    public ReplicationSender(String workerId, OrchestratorClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize,
                             boolean tlsEnabled, String trustCertCollectionPath,
                             String clientCertChainPath, String clientPrivateKeyPath,
                             boolean insecureSkipVerify) {
        this(workerId, orchestratorClient, fsManager, bandwidthLimiter, chunkSize, tlsEnabled,
                trustCertCollectionPath, clientCertChainPath, clientPrivateKeyPath, insecureSkipVerify,
                0L, 16 * 1024 * 1024L, 500);
    }

    public ReplicationSender(String workerId, OrchestratorClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize,
                             boolean tlsEnabled, String trustCertCollectionPath,
                             String clientCertChainPath, String clientPrivateKeyPath,
                             boolean insecureSkipVerify,
                             long smallFileThresholdBytes, long bundleTargetSizeBytes, int maxBundleFiles) {
        this(workerId, orchestratorClient, fsManager, bandwidthLimiter, chunkSize, tlsEnabled,
                trustCertCollectionPath, clientCertChainPath, clientPrivateKeyPath, insecureSkipVerify,
                smallFileThresholdBytes, bundleTargetSizeBytes, maxBundleFiles,
                CompressionCodec.COMPRESSION_ZSTD, WireCompressor.DEFAULT_ZSTD_LEVEL);
    }

    public ReplicationSender(String workerId, OrchestratorClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize,
                             boolean tlsEnabled, String trustCertCollectionPath,
                             String clientCertChainPath, String clientPrivateKeyPath,
                             boolean insecureSkipVerify,
                             long smallFileThresholdBytes, long bundleTargetSizeBytes, int maxBundleFiles,
                             CompressionCodec defaultCompressionCodec, int compressionLevel) {
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
        this.smallFileThresholdBytes = smallFileThresholdBytes;
        this.bundleTargetSizeBytes = bundleTargetSizeBytes > 0 ? bundleTargetSizeBytes : 16 * 1024 * 1024L;
        this.maxBundleFiles = maxBundleFiles > 0 ? maxBundleFiles : 500;
        this.defaultCompressionCodec = defaultCompressionCodec != null ? defaultCompressionCodec : CompressionCodec.COMPRESSION_ZSTD;
        this.compressionLevel = compressionLevel > 0 ? compressionLevel : WireCompressor.DEFAULT_ZSTD_LEVEL;
    }

    public CompressionCodec getDefaultCompressionCodec() {
        return defaultCompressionCodec;
    }

    public int getCompressionLevel() {
        return compressionLevel;
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

        if (!fsManager.exists(sourcePath, job.getExecutionPrincipal(), Boolean.TRUE.equals(job.getRunAsServiceAccount()))) {
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

        if (!fsManager.exists(sourcePath, job.getExecutionPrincipal(), Boolean.TRUE.equals(job.getRunAsServiceAccount()))) {
            String err = "Исходный путь не найден: " + sourcePath;
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

        boolean asService = job.getRunAsServiceAccount() != null && job.getRunAsServiceAccount();
        if (!fsManager.exists(sourcePath, job.getExecutionPrincipal(), asService)) {
            String err = "Исходный путь не найден: " + sourcePath;
            logger.error(err);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, 0L, err));
            return false;
        }

        ManagedChannel channel = null;
        try {
            channel = createManagedChannel(targetAddress);
            // Pre-flight очистка старых staging-файлов предыдущей попытки этого задания
            try {
                fsManager.cleanStagingFiles(targetPath, jobId, 0L, job.getExecutionPrincipal(), asService);
            } catch (Exception e) {
                logger.debug("Предстартовая очистка staging файлов для job_id={}: {}", jobId, e.getMessage());
            }

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

                // In-memory diff и формирование пула подзадач с бандлингом мелких файлов
                List<HadoopFsManager.FileItem> currentBundle = new ArrayList<>();
                long currentBundleBytes = 0;

                for (HadoopFsManager.FileItem item : items) {
                    Long remoteSize = remoteFileSizes.get(item.relativePath());
                    boolean skipped = (remoteSize != null && remoteSize == item.size() && item.size() > 0);
                    String subTargetPath = combinePaths(targetPath, item.relativePath());

                    if (skipped) {
                        taskItems.add(new TaskCreateItem(
                                UUID.randomUUID().toString(),
                                item.fullPath(),
                                subTargetPath,
                                item.size(),
                                true,
                                null,
                                "FILE",
                                1,
                                null
                        ));
                        continue;
                    }

                    // Если файл >= порога мелких файлов или бандлинг отключен (порог <= 0)
                    if (smallFileThresholdBytes <= 0 || item.size() >= smallFileThresholdBytes) {
                        taskItems.add(new TaskCreateItem(
                                UUID.randomUUID().toString(),
                                item.fullPath(),
                                subTargetPath,
                                item.size(),
                                false,
                                null,
                                "FILE",
                                1,
                                null
                        ));
                    } else {
                        // Мелкий файл: добавляем в накапливаемый бандл
                        currentBundle.add(item);
                        currentBundleBytes += item.size();

                        if (currentBundle.size() >= maxBundleFiles || currentBundleBytes >= bundleTargetSizeBytes) {
                            taskItems.add(createBundleTaskItem(sourcePath, targetPath, currentBundle, currentBundleBytes));
                            currentBundle = new ArrayList<>();
                            currentBundleBytes = 0;
                        }
                    }
                }

                // Завершающий бандл (если остались файлы)
                if (!currentBundle.isEmpty()) {
                    taskItems.add(createBundleTaskItem(sourcePath, targetPath, currentBundle, currentBundleBytes));
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
        if ("BUNDLE_TAR".equalsIgnoreCase(task.getTaskType())) {
            return transferBundleTarTask(task);
        }

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
        StreamObserver<TransferFileRequest> requestObserver = null;
        try {
            DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);

            requestObserver = stub.transferFile(new StreamObserver<>() {
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
            CompressionCodec effectiveCodec = WireCompressor.resolveCodecForPath(sourcePath, defaultCompressionCodec);
            FileMetadata metadata = FileMetadata.newBuilder()
                    .setJobId(jobId)
                    .setSourcePath(sourcePath)
                    .setTargetPath(targetPath)
                    .setTotalBytes(fileBytes)
                    .setRunAsServiceAccount(asService)
                    .setExecutionPrincipal(executionPrincipal != null ? executionPrincipal : "hdfs-replicator@REALM.LOCAL")
                    .setCompressionCodec(effectiveCodec)
                    .build();

            requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

            // Потоковая передача чанками со сжатием на лету и шейпингом
            try (InputStream in = fsManager.openInputStream(sourcePath, executionPrincipal, asService)) {
                byte[] buffer = new byte[chunkSize];
                long offset = 0;
                int bytesRead;

                while ((bytesRead = in.read(buffer)) != -1) {
                    boolean isLast = (offset + bytesRead) >= fileBytes;

                    // Сжатие чанка на лету
                    WireCompressor.CompressedPayload payload = WireCompressor.compress(
                            buffer, 0, bytesRead, effectiveCodec, compressionLevel
                    );
                    int wireBytes = payload.bytes().length;

                    // Локальный шейпинг полосы WAN по фактическому объему сжатых данных
                    if (bandwidthLimiter != null) {
                        bandwidthLimiter.throttle(wireBytes);
                    }

                    // Глобальный шейпинг токенов оркестратора
                    double waitSec = orchestratorClient.requestNetworkTokens(
                            new TokenRequest(workerId, wireBytes, null, null)
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
                            .setData(ByteString.copyFrom(payload.bytes()))
                            .setIsLastChunk(isLast)
                            .setCompressionCodec(payload.appliedCodec())
                            .setUncompressedSize(bytesRead)
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
            if (requestObserver != null) {
                try {
                    requestObserver.onError(e);
                } catch (Exception ignored) {}
            }
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

        // Pre-flight очистка старых staging-файлов предыдущей попытки этого задания
        try {
            fsManager.cleanStagingFiles(targetDir, jobId, 0L, job.getExecutionPrincipal(), job.getRunAsServiceAccount() != null && job.getRunAsServiceAccount());
        } catch (Exception e) {
            logger.debug("Предстартовая очистка staging файлов для job_id={}: {}", jobId, e.getMessage());
        }

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
            CompressionCodec effectiveCodec = WireCompressor.resolveCodecForPath(sourcePath, defaultCompressionCodec);
            FileMetadata metadata = FileMetadata.newBuilder()
                    .setJobId(jobId)
                    .setSourcePath(sourcePath)
                    .setTargetPath(targetPath)
                    .setTotalBytes(fileBytes)
                    .setRunAsServiceAccount(job.getRunAsServiceAccount() != null ? job.getRunAsServiceAccount() : false)
                    .setExecutionPrincipal(job.getExecutionPrincipal() != null ? job.getExecutionPrincipal() : "hdfs-replicator@REALM.LOCAL")
                    .setCompressionCodec(effectiveCodec)
                    .build();

            requestObserver.onNext(TransferFileRequest.newBuilder().setMetadata(metadata).build());

            // Потоковая передача чанками со сжатием на лету
            try (InputStream in = fsManager.openInputStream(sourcePath, job.getExecutionPrincipal(), job.getRunAsServiceAccount())) {
                byte[] buffer = new byte[chunkSize];
                long offset = 0;
                long lastProgressUpdate = 0;
                int bytesRead;

                while ((bytesRead = in.read(buffer)) != -1) {
                    boolean isLast = (offset + bytesRead) >= fileBytes;

                    // Сжатие чанка на лету
                    WireCompressor.CompressedPayload payload = WireCompressor.compress(
                            buffer, 0, bytesRead, effectiveCodec, compressionLevel
                    );
                    int wireBytes = payload.bytes().length;

                    // Локальный шейпинг полосы WAN по фактическому объему сжатых данных
                    if (bandwidthLimiter != null) {
                        bandwidthLimiter.throttle(wireBytes);
                    }

                    // Глобальный шейпинг
                    double waitSec = orchestratorClient.requestNetworkTokens(
                            new TokenRequest(workerId, wireBytes, job.getSourceClusterId(), job.getTargetClusterId())
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
                            .setData(ByteString.copyFrom(payload.bytes()))
                            .setIsLastChunk(isLast)
                            .setCompressionCodec(payload.appliedCodec())
                            .setUncompressedSize(bytesRead)
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

    private TaskCreateItem createBundleTaskItem(String baseSourcePath, String baseTargetPath,
                                                List<HadoopFsManager.FileItem> bundleFiles, long totalBytes) {
        List<BundleFileEntry> entries = new ArrayList<>(bundleFiles.size());
        for (HadoopFsManager.FileItem item : bundleFiles) {
            entries.add(new BundleFileEntry(item.fullPath(), item.relativePath(), item.size()));
        }
        String manifestJson = "";
        try {
            manifestJson = objectMapper.writeValueAsString(entries);
        } catch (Exception e) {
            logger.error("Ошибка сериализации bundleManifest: {}", e.getMessage());
        }
        return new TaskCreateItem(
                UUID.randomUUID().toString(),
                baseSourcePath,
                baseTargetPath,
                totalBytes,
                false,
                null,
                "BUNDLE_TAR",
                bundleFiles.size(),
                manifestJson
        );
    }

    /**
     * Потоковая передача пакета мелких файлов в виде единого непрерывного TAR-архива на лету.
     */
    public boolean transferBundleTarTask(TaskItemDto task) {
        String taskId = task.getId();
        String jobId = task.getJobId();
        String targetPath = task.getTargetPath();
        long totalBytes = task.getFileSize();
        int fileCount = task.getFileCount();
        String targetAddress = task.getTargetAddress() != null ? task.getTargetAddress() : "localhost:50051";

        logger.info("Воркер '{}' начинает потоковую передачу бандла TAR '{}': {} файлов ({} байт) в '{}' на {}",
                workerId, taskId, fileCount, totalBytes, targetPath, targetAddress);

        List<BundleFileEntry> entries;
        try {
            entries = objectMapper.readValue(task.getBundleManifest(), new TypeReference<List<BundleFileEntry>>() {});
        } catch (Exception e) {
            String err = "Ошибка десериализации bundleManifest для задачи " + taskId + ": " + e.getMessage();
            logger.error(err);
            orchestratorClient.failTask(new FailTaskRequest(taskId, workerId, err));
            return false;
        }

        ManagedChannel channel = null;
        StreamObserver<TarStreamRequest> requestObserver = null;
        try {
            channel = createManagedChannel(targetAddress);
            DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);

            CompletableFuture<TarStreamResponse> responseFuture = new CompletableFuture<>();

            requestObserver = stub.transferTarStream(new StreamObserver<>() {
                @Override
                public void onNext(TarStreamResponse value) {
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

            // 1. Метаданные бандла
            boolean asService = task.getRunAsServiceAccount() != null && task.getRunAsServiceAccount();
            String principal = task.getExecutionPrincipal() != null ? task.getExecutionPrincipal() : "hdfs-replicator@REALM.LOCAL";
            boolean allPrecompressed = entries.stream().allMatch(e ->
                    WireCompressor.isPrecompressedPath(e.getSourcePath()) || WireCompressor.isPrecompressedPath(e.getRelativePath()));
            CompressionCodec tarCodec = allPrecompressed ? CompressionCodec.COMPRESSION_NONE : defaultCompressionCodec;

            TarStreamMetadata metadata = TarStreamMetadata.newBuilder()
                    .setJobId(jobId)
                    .setTaskId(taskId)
                    .setBaseTargetPath(targetPath)
                    .setTotalFiles(entries.size())
                    .setTotalBytes(totalBytes)
                    .setRunAsServiceAccount(asService)
                    .setExecutionPrincipal(principal)
                    .setCompressionCodec(tarCodec)
                    .build();

            requestObserver.onNext(TarStreamRequest.newBuilder().setMetadata(metadata).build());

            // 2. Потоковая упаковка TAR на лету в памяти с чанкованием, сжатием, шейпингом и запросом токенов
            TarChunkingOutputStream chunkingOut = new TarChunkingOutputStream(
                    taskId,
                    requestObserver,
                    chunkSize,
                    bandwidthLimiter,
                    orchestratorClient,
                    workerId,
                    tarCodec,
                    compressionLevel
            );

            try (TarArchiveOutputStream tarOut = new TarArchiveOutputStream(new BufferedOutputStream(chunkingOut, 32 * 1024))) {
                tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                tarOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

                for (BundleFileEntry entry : entries) {
                    TarArchiveEntry tarEntry = new TarArchiveEntry(entry.getRelativePath());
                    tarEntry.setSize(entry.getSize());
                    tarOut.putArchiveEntry(tarEntry);

                    if (!fsManager.exists(entry.getSourcePath(), principal, asService)) {
                        logger.warn("Файл бандла '{}' не найден в HDFS, пропуск", entry.getSourcePath());
                        continue;
                    }

                    try (InputStream in = fsManager.openInputStream(entry.getSourcePath(), principal, asService)) {
                        IOUtils.copyBytes(in, tarOut, 32 * 1024, false);
                    }
                    tarOut.closeArchiveEntry();
                }

                tarOut.finish();
                tarOut.flush();
            }

            chunkingOut.flush();
            requestObserver.onCompleted();

            TarStreamResponse resp = responseFuture.get(120, TimeUnit.SECONDS);
            if (resp.getSuccess()) {
                logger.info("Воркер '{}' успешно передал виртуальный TAR-стрим '{}': {} файлов ({} байт)",
                        workerId, taskId, resp.getFilesCommitted(), resp.getBytesWritten());
                orchestratorClient.completeTask(new CompleteTaskRequest(taskId, workerId, resp.getBytesWritten(), "OK", resp.getFilesCommitted()));
                return true;
            } else {
                logger.error("Воркер '{}' получил ошибку передачи бандла '{}': {}", workerId, taskId, resp.getMessage());
                orchestratorClient.failTask(new FailTaskRequest(taskId, workerId, resp.getMessage()));
                return false;
            }
        } catch (Exception e) {
            logger.error("Исключение при передаче бандла TAR '{}': {}", taskId, e.getMessage(), e);
            if (requestObserver != null) {
                try {
                    requestObserver.onError(e);
                } catch (Exception ignored) {}
            }
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

    /**
     * Потоковый адаптер OutputStream, упаковывающий байты архива в gRPC чанки по 64 КБ с шейпингом.
     */
    public static class TarChunkingOutputStream extends OutputStream {
        private final String taskId;
        private final StreamObserver<TarStreamRequest> requestObserver;
        private final int chunkSize;
        private final LocalBandwidthLimiter bandwidthLimiter;
        private final OrchestratorClient orchestratorClient;
        private final String workerId;
        private final CompressionCodec codec;
        private final int compressionLevel;
        private final byte[] buffer;
        private int count = 0;
        private long offset = 0;

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorClient orchestratorClient, String workerId) {
            this(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId,
                    CompressionCodec.COMPRESSION_ZSTD, WireCompressor.DEFAULT_ZSTD_LEVEL);
        }

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorClient orchestratorClient, String workerId,
                                       CompressionCodec codec, int compressionLevel) {
            this.taskId = taskId;
            this.requestObserver = requestObserver;
            this.chunkSize = chunkSize > 0 ? chunkSize : 64 * 1024;
            this.bandwidthLimiter = bandwidthLimiter;
            this.orchestratorClient = orchestratorClient;
            this.workerId = workerId;
            this.codec = codec != null ? codec : CompressionCodec.COMPRESSION_NONE;
            this.compressionLevel = compressionLevel > 0 ? compressionLevel : WireCompressor.DEFAULT_ZSTD_LEVEL;
            this.buffer = new byte[this.chunkSize];
        }

        @Override
        public void write(int b) throws IOException {
            buffer[count++] = (byte) b;
            if (count >= chunkSize) {
                flushBuffer();
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            int remaining = len;
            int currentOff = off;
            while (remaining > 0) {
                int space = chunkSize - count;
                int toCopy = Math.min(space, remaining);
                System.arraycopy(b, currentOff, buffer, count, toCopy);
                count += toCopy;
                currentOff += toCopy;
                remaining -= toCopy;
                if (count >= chunkSize) {
                    flushBuffer();
                }
            }
        }

        private void flushBuffer() throws IOException {
            if (count == 0) return;

            WireCompressor.CompressedPayload payload = WireCompressor.compress(
                    buffer, 0, count, codec, compressionLevel
            );
            int wireBytes = payload.bytes().length;

            if (bandwidthLimiter != null) {
                bandwidthLimiter.throttle(wireBytes);
            }
            if (orchestratorClient != null && workerId != null) {
                double waitSec = orchestratorClient.requestNetworkTokens(
                        new TokenRequest(workerId, wireBytes, null, null)
                );
                if (waitSec > 0) {
                    try {
                        Thread.sleep((long) (waitSec * 1000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException("Передача прервана во время ожидания токенов");
                    }
                }
            }

            TarStreamChunk chunk = TarStreamChunk.newBuilder()
                    .setTaskId(taskId)
                    .setOffset(offset)
                    .setData(ByteString.copyFrom(payload.bytes()))
                    .setCompressionCodec(payload.appliedCodec())
                    .setUncompressedSize(count)
                    .build();

            requestObserver.onNext(TarStreamRequest.newBuilder().setChunk(chunk).build());
            offset += count;
            count = 0;
        }

        @Override
        public void flush() throws IOException {
            flushBuffer();
        }
    }

    /**
     * DTO описания файла внутри бандла.
     */
    public static class BundleFileEntry {
        @JsonProperty("source_path")
        private String sourcePath;
        @JsonProperty("relative_path")
        private String relativePath;
        @JsonProperty("size")
        private long size;

        public BundleFileEntry() {}

        public BundleFileEntry(String sourcePath, String relativePath, long size) {
            this.sourcePath = sourcePath;
            this.relativePath = relativePath;
            this.size = size;
        }

        public String getSourcePath() { return sourcePath; }
        public void setSourcePath(String sourcePath) { this.sourcePath = sourcePath; }
        public String getRelativePath() { return relativePath; }
        public void setRelativePath(String relativePath) { this.relativePath = relativePath; }
        public long getSize() { return size; }
        public void setSize(long size) { this.size = size; }
    }
}
