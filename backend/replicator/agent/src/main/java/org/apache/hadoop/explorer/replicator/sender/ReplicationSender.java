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
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.compression.WireCompressor;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.metrics.AgentMetrics;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.receiver.DataTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * gRPC Клиент передачи файлов чанками с многоуровневым контролем полосы (Sender).
 * Поддерживает пул постоянных gRPC каналов (ManagedChannel pool) и пакетирование сетевых токенов.
 */
public class ReplicationSender implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationSender.class);

    /** Предел ожидания готовности исходящего сокета перед отправкой чанка. */
    private static final long CHUNK_READY_TIMEOUT_MS = 120_000L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String workerId;
    private final OrchestratorGrpcClient orchestratorClient;
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
    private final String agentSecret;
    private final DirectoryDiffEngine diffEngine;
    private final TarStreamPipeline tarPipeline;
    private final AtomicLong totalBytesTransferred = new AtomicLong(0);
    private final AtomicLong totalTransferErrors = new AtomicLong(0);

    // Пул постоянных gRPC каналов с поддержкой HTTP/2 keepalive
    private final ConcurrentHashMap<String, ManagedChannel> channelPool = new ConcurrentHashMap<>();

    // Локальный буфер сетевых токенов (Hierarchical Token Bucket batching)
    private final ConcurrentHashMap<String, AtomicLong> tokenBatchLease = new ConcurrentHashMap<>();
    public static final long DEFAULT_MIN_TOKEN_BATCH_BYTES = 2 * 1024 * 1024L; // 2 MB на батч
    public static final long DEFAULT_MAX_TOKEN_BATCH_BYTES = 16 * 1024 * 1024L; // 16 MB на батч
    public static final long DEFAULT_TOKEN_BATCH_BYTES = DEFAULT_MIN_TOKEN_BATCH_BYTES; // Обратная совместимость

    private static final ConcurrentHashMap<String, DynamicRouteInfo> SHARED_ROUTE_INFOS = new ConcurrentHashMap<>();

    private final long minTokenBatchBytes;
    private final long maxTokenBatchBytes;
    private final ConcurrentHashMap<String, DynamicRouteInfo> dynamicRouteInfos = new ConcurrentHashMap<>();

    /**
     * Состояние динамического адаптивного лизинга токенов для конкретного маршрута (source -> target).
     * Реализует масштабирование размера батча (AIMD / Dynamic Step Escalation) в зависимости от задержек сети.
     */
    public static class DynamicRouteInfo {
        private final AtomicLong currentBatchSize;
        private final AtomicLong lastAcquireNanos;
        private final AtomicInteger consecutiveFastAcquires;

        public DynamicRouteInfo(long initialBatchBytes) {
            this.currentBatchSize = new AtomicLong(initialBatchBytes);
            this.lastAcquireNanos = new AtomicLong(System.nanoTime());
            this.consecutiveFastAcquires = new AtomicInteger(0);
        }

        public long getCurrentBatchSize() {
            return currentBatchSize.get();
        }

        public void escalate(long maxBatchBytes) {
            currentBatchSize.updateAndGet(curr -> Math.min(maxBatchBytes, curr * 2));
        }

        public void throttleReset(long minBatchBytes) {
            currentBatchSize.set(minBatchBytes);
            consecutiveFastAcquires.set(0);
        }

        public int recordFastAcquire(long thresholdIntervalNanos) {
            long now = System.nanoTime();
            long prev = lastAcquireNanos.getAndSet(now);
            long elapsed = now - prev;
            if (elapsed <= thresholdIntervalNanos) {
                return consecutiveFastAcquires.incrementAndGet();
            } else {
                consecutiveFastAcquires.set(1);
                return 1;
            }
        }

        public int getConsecutiveFastAcquires() {
            return consecutiveFastAcquires.get();
        }
    }

    public long getTotalBytesTransferred() { return totalBytesTransferred.get(); }
    public long getTotalTransferErrors() { return totalTransferErrors.get(); }
    public DirectoryDiffEngine getDiffEngine() { return diffEngine; }
    public TarStreamPipeline getTarPipeline() { return tarPipeline; }
    public long getMinTokenBatchBytes() { return minTokenBatchBytes; }
    public long getMaxTokenBatchBytes() { return maxTokenBatchBytes; }
    public ConcurrentHashMap<String, DynamicRouteInfo> getDynamicRouteInfos() { return dynamicRouteInfos; }
    public static ConcurrentHashMap<String, DynamicRouteInfo> getSharedRouteInfos() { return SHARED_ROUTE_INFOS; }
    public static void resetSharedRouteInfos() { SHARED_ROUTE_INFOS.clear(); }

    public ReplicationSender(String workerId, OrchestratorGrpcClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize) {
        this(workerId, orchestratorClient, fsManager, bandwidthLimiter, chunkSize, false, null, null, null, false);
    }

    public ReplicationSender(String workerId, OrchestratorGrpcClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize,
                             boolean tlsEnabled, String trustCertCollectionPath,
                             String clientCertChainPath, String clientPrivateKeyPath,
                             boolean insecureSkipVerify) {
        this(workerId, orchestratorClient, fsManager, bandwidthLimiter, chunkSize, tlsEnabled,
                trustCertCollectionPath, clientCertChainPath, clientPrivateKeyPath, insecureSkipVerify,
                0L, 16 * 1024 * 1024L, 500);
    }

    public ReplicationSender(String workerId, OrchestratorGrpcClient orchestratorClient, HadoopFsManager fsManager,
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

    public ReplicationSender(String workerId, OrchestratorGrpcClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize,
                             boolean tlsEnabled, String trustCertCollectionPath,
                             String clientCertChainPath, String clientPrivateKeyPath,
                             boolean insecureSkipVerify,
                             long smallFileThresholdBytes, long bundleTargetSizeBytes, int maxBundleFiles,
                             CompressionCodec defaultCompressionCodec, int compressionLevel) {
        this(workerId, orchestratorClient, fsManager, bandwidthLimiter, chunkSize, tlsEnabled,
                trustCertCollectionPath, clientCertChainPath, clientPrivateKeyPath, insecureSkipVerify,
                smallFileThresholdBytes, bundleTargetSizeBytes, maxBundleFiles,
                defaultCompressionCodec, compressionLevel, null);
    }

    public ReplicationSender(String workerId, OrchestratorGrpcClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize,
                             boolean tlsEnabled, String trustCertCollectionPath,
                             String clientCertChainPath, String clientPrivateKeyPath,
                             boolean insecureSkipVerify,
                             long smallFileThresholdBytes, long bundleTargetSizeBytes, int maxBundleFiles,
                             CompressionCodec defaultCompressionCodec, int compressionLevel,
                             String agentSecret) {
        this(workerId, orchestratorClient, fsManager, bandwidthLimiter, chunkSize, tlsEnabled,
                trustCertCollectionPath, clientCertChainPath, clientPrivateKeyPath, insecureSkipVerify,
                smallFileThresholdBytes, bundleTargetSizeBytes, maxBundleFiles,
                defaultCompressionCodec, compressionLevel, agentSecret,
                DEFAULT_MIN_TOKEN_BATCH_BYTES, DEFAULT_MAX_TOKEN_BATCH_BYTES);
    }

    public ReplicationSender(String workerId, OrchestratorGrpcClient orchestratorClient, HadoopFsManager fsManager,
                             LocalBandwidthLimiter bandwidthLimiter, int chunkSize,
                             boolean tlsEnabled, String trustCertCollectionPath,
                             String clientCertChainPath, String clientPrivateKeyPath,
                             boolean insecureSkipVerify,
                             long smallFileThresholdBytes, long bundleTargetSizeBytes, int maxBundleFiles,
                             CompressionCodec defaultCompressionCodec, int compressionLevel,
                             String agentSecret,
                             long minTokenBatchBytes, long maxTokenBatchBytes) {
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
        this.agentSecret = agentSecret;
        this.minTokenBatchBytes = minTokenBatchBytes > 0 ? minTokenBatchBytes : DEFAULT_MIN_TOKEN_BATCH_BYTES;
        this.maxTokenBatchBytes = maxTokenBatchBytes >= this.minTokenBatchBytes ? maxTokenBatchBytes : Math.max(this.minTokenBatchBytes, DEFAULT_MAX_TOKEN_BATCH_BYTES);
        this.diffEngine = new DirectoryDiffEngine(this.fsManager, this.orchestratorClient, objectMapper,
                this.smallFileThresholdBytes, this.bundleTargetSizeBytes, this.maxBundleFiles);
        this.tarPipeline = new TarStreamPipeline(objectMapper);
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
            boolean autoCreate = Boolean.parseBoolean(System.getenv().getOrDefault("REPLICATOR_AUTO_CREATE_TEST_DATA", "false"));
            if (autoCreate && !sourcePath.startsWith("hdfs://") && !sourcePath.startsWith("s3a://") && !sourcePath.startsWith("file://")) {
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
            channel = getOrCreateChannel(targetAddress);
        } catch (Exception e) {
            String err = "Не удалось получить gRPC соединение к " + targetAddress + ": " + e.getMessage();
            logger.error(err, e);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, 0L, err));
            return false;
        }

        boolean isDir = fsManager.isDirectory(sourcePath, job.getExecutionPrincipal(), job.getRunAsServiceAccount());
        if (isDir) {
            return transferDirectory(job, channel);
        } else {
            return transferSingleFileWithSync(job, channel);
        }
    }

    /**
     * Этап анализа (Analyzer): обход HDFS-1, запрос манифеста HDFS-2,
     * вычисление in-memory diff и создание распределенного пула пофайловых задач в Оркестраторе.
     */
    public boolean analyzeAndCreateTaskPool(JobDto job, String targetAddress) {
        ManagedChannel channel;
        try {
            channel = getOrCreateChannel(targetAddress);
        } catch (Exception e) {
            String err = "Не удалось получить gRPC соединение к " + targetAddress + ": " + e.getMessage();
            logger.error(err, e);
            orchestratorClient.updateJobProgress(job.getId(), new UpdateJobRequest("FAILED", 0L, 0L, err));
            return false;
        }
        return diffEngine.analyzeAndCreateTaskPool(job, channel);
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
        String targetAddress = task.getTargetAddress();
        if (targetAddress == null || targetAddress.isBlank()) {
            throw new IllegalArgumentException("Целевой gRPC адрес репликации не указан для задачи " + taskId);
        }

        logger.info("Воркер '{}' начинает передачу файла задачи '{}': {} -> {} ({} байт) на {}",
                workerId, taskId, sourcePath, targetPath, fileSize, targetAddress);

        ManagedChannel channel = null;
        try {
            channel = getOrCreateChannel(targetAddress);

            boolean success = streamTaskFileToChannel(taskId, jobId, sourcePath, targetPath, fileSize,
                    task.getExecutionPrincipal(), task.getRunAsServiceAccount(),
                    task.getSourceClusterId(), task.getTargetClusterId(), task.getTaskToken(), channel);

            if (success) {
                totalBytesTransferred.addAndGet(fileSize);
                logger.info("Воркер '{}' успешно передал файл задачи '{}': {} ({} байт)", workerId, taskId, sourcePath, fileSize);
                orchestratorClient.completeTask(new CompleteTaskRequest(taskId, workerId, fileSize, "OK"));
                return true;
            } else {
                totalTransferErrors.incrementAndGet();
                logger.error("Воркер '{}' не смог передать файл задачи '{}': {}", workerId, taskId, sourcePath);
                orchestratorClient.failTask(new FailTaskRequest(taskId, workerId, "Ошибка передачи чанков на приемник"));
                return false;
            }
        } catch (Exception e) {
            evictChannel(targetAddress);
            totalTransferErrors.incrementAndGet();
            logger.error("Исключение при передаче задачи '{}': {}", taskId, e.getMessage(), e);
            orchestratorClient.failTask(new FailTaskRequest(taskId, workerId, e.getMessage()));
            return false;
        }
    }

    private boolean streamTaskFileToChannel(String taskId, String jobId, String sourcePath, String targetPath,
                                            long fileBytes, String executionPrincipal, Boolean runAsServiceAccount,
                                            String sourceClusterId, String targetClusterId,
                                            String taskToken,
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
            StreamReadinessGate readinessGate = StreamReadinessGate.attach(requestObserver);

            // Метаданные файла
            boolean asService = runAsServiceAccount != null && runAsServiceAccount;
            CompressionCodec effectiveCodec = WireCompressor.resolveCodecForPath(sourcePath, defaultCompressionCodec);
            FileMetadata.Builder metaBuilder = FileMetadata.newBuilder()
                    .setJobId(jobId)
                    .setTaskId(taskId != null ? taskId : "")
                    .setSourcePath(sourcePath)
                    .setTargetPath(targetPath)
                    .setTotalBytes(fileBytes)
                    .setRunAsServiceAccount(asService)
                    .setExecutionPrincipal(executionPrincipal != null ? executionPrincipal : "hdfs-replicator@REALM.LOCAL")
                    .setCompressionCodec(effectiveCodec);
            if (taskToken != null && !taskToken.isBlank()) {
                metaBuilder.setTaskToken(taskToken);
            }
            FileMetadata metadata = metaBuilder.build();

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
                    AgentMetrics.getInstance().recordCompression(payload.appliedCodec().name(), bytesRead, wireBytes);

                    // Локальный шейпинг полосы WAN по фактическому объему сжатых данных
                    if (bandwidthLimiter != null) {
                        bandwidthLimiter.throttle(wireBytes);
                    }

                    // Пакетный шейпинг токенов оркестратора (Hierarchical Token Bucket)
                    acquireNetworkTokens(wireBytes, sourceClusterId, targetClusterId);

                    FileChunk chunk = FileChunk.newBuilder()
                            .setJobId(jobId)
                            .setOffset(offset)
                            .setData(ByteString.copyFrom(payload.bytes()))
                            .setIsLastChunk(isLast)
                            .setCompressionCodec(payload.appliedCodec())
                            .setUncompressedSize(bytesRead)
                            .build();

                    // Flow Control: не отправляем, пока исходящий сокет не принимает данные,
                    // иначе очередь Netty растет со скоростью чтения источника
                    if (readinessGate != null) {
                        readinessGate.awaitReady(CHUNK_READY_TIMEOUT_MS);
                    }

                    long chunkSendStart = System.nanoTime();
                    requestObserver.onNext(TransferFileRequest.newBuilder().setChunk(chunk).build());
                    AgentMetrics.getInstance().recordChunkTransferLatency(System.nanoTime() - chunkSendStart);

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
        return diffEngine.fetchRemoteManifest(channel, targetDir, job);
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
            StreamReadinessGate readinessGate = StreamReadinessGate.attach(requestObserver);

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
                    AgentMetrics.getInstance().recordCompression(payload.appliedCodec().name(), bytesRead, wireBytes);

                    // Локальный шейпинг полосы WAN по фактическому объему сжатых данных
                    if (bandwidthLimiter != null) {
                        bandwidthLimiter.throttle(wireBytes);
                    }

                    // Пакетный шейпинг токенов оркестратора (Hierarchical Token Bucket)
                    acquireNetworkTokens(wireBytes, job.getSourceClusterId(), job.getTargetClusterId());

                    FileChunk chunk = FileChunk.newBuilder()
                            .setJobId(jobId)
                            .setOffset(offset)
                            .setData(ByteString.copyFrom(payload.bytes()))
                            .setIsLastChunk(isLast)
                            .setCompressionCodec(payload.appliedCodec())
                            .setUncompressedSize(bytesRead)
                            .build();

                    // Flow Control: не отправляем, пока исходящий сокет не принимает данные,
                    // иначе очередь Netty растет со скоростью чтения источника
                    if (readinessGate != null) {
                        readinessGate.awaitReady(CHUNK_READY_TIMEOUT_MS);
                    }

                    long chunkSendStart = System.nanoTime();
                    requestObserver.onNext(TransferFileRequest.newBuilder().setChunk(chunk).build());
                    AgentMetrics.getInstance().recordChunkTransferLatency(System.nanoTime() - chunkSendStart);

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
        return diffEngine.checkRemoteFile(channel, targetPath, job);
    }

    private static String combinePaths(String base, String relative) {
        return DataTransferServiceImpl.combinePaths(base, relative);
    }

    /**
     * Пакетный запрос токенов оркестратора (Hierarchical Token Bucket).
     * Адаптивно масштабирует размер батча (Dynamic Token Lease) от minTokenBatchBytes до maxTokenBatchBytes,
     * устраняя gRPC contention и шторм запросов на высокоскоростных каналах WAN.
     */
    public void acquireNetworkTokens(long wireBytes, String sourceClusterId, String targetClusterId) {
        acquireNetworkTokens(this.tokenBatchLease, this.dynamicRouteInfos, this.orchestratorClient, this.workerId,
                wireBytes, sourceClusterId, targetClusterId, this.minTokenBatchBytes, this.maxTokenBatchBytes);
    }

    public static void acquireNetworkTokens(ConcurrentHashMap<String, AtomicLong> tokenLeaseMap,
                                            OrchestratorGrpcClient orchestratorClient,
                                            String workerId,
                                            long wireBytes,
                                            String sourceClusterId,
                                            String targetClusterId) {
        acquireNetworkTokens(tokenLeaseMap, SHARED_ROUTE_INFOS, orchestratorClient, workerId,
                wireBytes, sourceClusterId, targetClusterId, DEFAULT_MIN_TOKEN_BATCH_BYTES, DEFAULT_MAX_TOKEN_BATCH_BYTES);
    }

    public static void acquireNetworkTokens(ConcurrentHashMap<String, AtomicLong> tokenLeaseMap,
                                            ConcurrentHashMap<String, DynamicRouteInfo> dynamicRouteInfoMap,
                                            OrchestratorGrpcClient orchestratorClient,
                                            String workerId,
                                            long wireBytes,
                                            String sourceClusterId,
                                            String targetClusterId,
                                            long minBatchBytes,
                                            long maxBatchBytes) {
        if (wireBytes <= 0 || orchestratorClient == null || workerId == null) {
            return;
        }
        ConcurrentHashMap<String, AtomicLong> leases = tokenLeaseMap != null ? tokenLeaseMap : new ConcurrentHashMap<>();
        ConcurrentHashMap<String, DynamicRouteInfo> routeInfos = dynamicRouteInfoMap != null ? dynamicRouteInfoMap : SHARED_ROUTE_INFOS;
        String routeKey = (sourceClusterId != null ? sourceClusterId : "") + "->" + (targetClusterId != null ? targetClusterId : "");
        AtomicLong lease = leases.computeIfAbsent(routeKey, k -> new AtomicLong(0));
        long safeMin = minBatchBytes > 0 ? minBatchBytes : DEFAULT_MIN_TOKEN_BATCH_BYTES;
        long safeMax = maxBatchBytes >= safeMin ? maxBatchBytes : Math.max(safeMin, DEFAULT_MAX_TOKEN_BATCH_BYTES);
        DynamicRouteInfo routeInfo = routeInfos.computeIfAbsent(routeKey, k -> new DynamicRouteInfo(safeMin));

        while (true) {
            long current = lease.get();
            if (current >= wireBytes) {
                if (lease.compareAndSet(current, current - wireBytes)) {
                    return; // Взято из локального пакета без вызова HTTP
                }
            } else {
                long currentBatch = routeInfo.getCurrentBatchSize();
                long requestBytes = Math.max(wireBytes, currentBatch);
                double waitSec = orchestratorClient.requestNetworkTokens(
                        new TokenRequest(workerId, requestBytes, sourceClusterId, targetClusterId)
                );
                if (waitSec > 0.05) {
                    // Троттлинг со стороны оркестратора: канал перегружен.
                    // Немедленно сбрасываем размер лизинга к минимуму (AIMD backoff)
                    routeInfo.throttleReset(safeMin);
                    try {
                        Thread.sleep((long) (waitSec * 1000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                } else {
                    // Канал свободен, оцениваем частоту запросов (интервал < 1000 мс)
                    int fastCount = routeInfo.recordFastAcquire(1_000_000_000L);
                    if (fastCount >= 2) {
                        routeInfo.escalate(safeMax);
                    }
                }
                long remainingToAdd = requestBytes - wireBytes;
                if (remainingToAdd > 0) {
                    lease.addAndGet(remainingToAdd);
                }
                return;
            }
        }
    }

    public ManagedChannel getOrCreateChannel(String targetAddress) throws javax.net.ssl.SSLException {
        if (targetAddress == null || targetAddress.isBlank()) {
            throw new IllegalArgumentException("Целевой gRPC адрес не может быть пустым");
        }
        String key = targetAddress.trim();
        ManagedChannel channel = channelPool.get(key);
        if (channel != null && !channel.isShutdown() && !channel.isTerminated()) {
            return channel;
        }
        synchronized (channelPool) {
            channel = channelPool.get(key);
            if (channel != null && !channel.isShutdown() && !channel.isTerminated()) {
                return channel;
            }
            ManagedChannel newChannel = createManagedChannel(key);
            channelPool.put(key, newChannel);
            return newChannel;
        }
    }

    /**
     * Исключение сбойного канала из пула при возникновении сетевых ошибок.
     */
    public void evictChannel(String targetAddress) {
        if (targetAddress == null) return;
        ManagedChannel channel = channelPool.remove(targetAddress.trim());
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
            try {
                channel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
        }
    }

    @Override
    public void close() {
        shutdownChannels();
    }

    /**
     * Корректное закрытие всех активных соединений в пуле каналов при остановке воркера.
     */
    public void shutdownChannels() {
        for (Map.Entry<String, ManagedChannel> entry : channelPool.entrySet()) {
            ManagedChannel ch = entry.getValue();
            if (ch != null && !ch.isShutdown()) {
                ch.shutdown();
                try {
                    ch.awaitTermination(2, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {}
            }
        }
        channelPool.clear();
        tokenBatchLease.clear();
    }

    /**
     * Создает gRPC ManagedChannel с учетом параметров TLS / plaintext и HTTP/2 keepalive.
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
            NettyChannelBuilder channelBuilder = NettyChannelBuilder.forTarget(cleanAddress)
                    .sslContext(sslBuilder.build());
            if (agentSecret != null && !agentSecret.isBlank()) {
                channelBuilder.intercept(org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor.createClientInterceptor(agentSecret));
            }
            return channelBuilder
                    .maxInboundMessageSize(64 * 1024 * 1024)
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .keepAliveTimeout(10, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .build();
        } else {
            NettyChannelBuilder channelBuilder = NettyChannelBuilder.forTarget(cleanAddress)
                    .usePlaintext();
            if (agentSecret != null && !agentSecret.isBlank()) {
                channelBuilder.intercept(org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor.createClientInterceptor(agentSecret));
            }
            return channelBuilder
                    .maxInboundMessageSize(64 * 1024 * 1024)
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .keepAliveTimeout(10, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .build();
        }
    }

    private TaskCreateItem createBundleTaskItem(String baseSourcePath, String baseTargetPath,
                                                List<HadoopFsManager.FileItem> bundleFiles, long totalBytes) {
        return diffEngine.createBundleTaskItem(baseSourcePath, baseTargetPath, bundleFiles, totalBytes);
    }

    /**
     * Потоковая передача пакета мелких файлов в виде единого непрерывного TAR-архива на лету.
     */
    public boolean transferBundleTarTask(TaskItemDto task) {
        String targetAddress = task.getTargetAddress();
        if (targetAddress == null || targetAddress.isBlank()) {
            throw new IllegalArgumentException("Целевой gRPC адрес репликации не указан для бандла " + task.getId());
        }
        try {
            ManagedChannel channel = getOrCreateChannel(targetAddress);
            return tarPipeline.transferBundleTarTask(task, channel, workerId, orchestratorClient, fsManager,
                    bandwidthLimiter, chunkSize, defaultCompressionCodec, compressionLevel, tokenBatchLease);
        } catch (Exception e) {
            evictChannel(targetAddress);
            logger.error("Исключение при передаче бандла TAR '{}': {}", task.getId(), e.getMessage(), e);
            orchestratorClient.failTask(new FailTaskRequest(task.getId(), workerId, e.getMessage()));
            return false;
        }
    }

    /**
     * Потоковый адаптер OutputStream, упаковывающий байты архива в gRPC чанки по 64 КБ с шейпингом.
     */
    public static class TarChunkingOutputStream extends TarStreamPipeline.TarChunkingOutputStream {
        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId) {
            super(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId);
        }

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId,
                                       CompressionCodec codec, int compressionLevel) {
            super(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId, codec, compressionLevel);
        }

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId,
                                       CompressionCodec codec, int compressionLevel,
                                       String sourceClusterId, String targetClusterId) {
            super(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId, codec, compressionLevel, sourceClusterId, targetClusterId);
        }

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId,
                                       CompressionCodec codec, int compressionLevel,
                                       String sourceClusterId, String targetClusterId,
                                       ConcurrentHashMap<String, AtomicLong> tokenBatchLease) {
            super(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId, codec, compressionLevel, sourceClusterId, targetClusterId, tokenBatchLease);
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
