package org.apache.hadoop.explorer.replicator.receiver;

import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.explorer.replicator.compression.WireCompressor;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.metrics.AgentMetrics;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * gRPC Servicer сервиса DataTransferService для приема файлов и атомарного коммита в HDFS/FS.
 */
public class DataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DataTransferServiceImpl.class);

    private final String stagingDir;
    private final HadoopFsManager fsManager;
    private final LocalBandwidthLimiter bandwidthLimiter;
    private final String keytabPath;
    private final ExecutorService bundleCommitExecutor;
    private final Set<String> knownTargetDirectories = ConcurrentHashMap.newKeySet();
    private final int bundleCommitConcurrency;
    private final int maxConcurrentTarStreams;
    private final int tarPipeBufferBytes;
    private final Semaphore tarStreamSemaphore;

    public DataTransferServiceImpl(
            String stagingDir,
            HadoopFsManager fsManager,
            LocalBandwidthLimiter bandwidthLimiter,
            String keytabPath,
            int bundleCommitConcurrency,
            int maxConcurrentTarStreams,
            int tarPipeBufferBytes
    ) {
        this.stagingDir = stagingDir != null ? stagingDir : "/tmp/staging";
        this.fsManager = fsManager;
        this.bandwidthLimiter = bandwidthLimiter;
        this.keytabPath = keytabPath;
        this.bundleCommitConcurrency = bundleCommitConcurrency > 0 ? bundleCommitConcurrency : 8;
        this.maxConcurrentTarStreams = maxConcurrentTarStreams > 0 ? maxConcurrentTarStreams : 16;
        this.tarPipeBufferBytes = tarPipeBufferBytes > 0 ? tarPipeBufferBytes : 2 * 1024 * 1024;
        this.tarStreamSemaphore = new Semaphore(this.maxConcurrentTarStreams, true);
        this.bundleCommitExecutor = Executors.newFixedThreadPool(this.bundleCommitConcurrency, r -> {
            Thread t = new Thread(r, "bundle-hdfs-committer");
            t.setDaemon(true);
            return t;
        });

        File dir = new File(this.stagingDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    public DataTransferServiceImpl(String stagingDir, HadoopFsManager fsManager, LocalBandwidthLimiter bandwidthLimiter, String keytabPath, int bundleCommitConcurrency) {
        this(stagingDir, fsManager, bandwidthLimiter, keytabPath, bundleCommitConcurrency, 16, 2 * 1024 * 1024);
    }

    public void recordTargetDirectory(String path) {
        if (path == null || path.isBlank()) return;
        String dir = path;
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash > 0) {
            dir = path.substring(0, lastSlash);
        }
        knownTargetDirectories.add(dir);
    }

    public int cleanOrphanedStaging(long ttlMillis) {
        int totalDeleted = 0;
        for (String dir : knownTargetDirectories) {
            try {
                totalDeleted += fsManager.cleanStagingFiles(dir, null, ttlMillis, null, true);
            } catch (Exception e) {
                logger.warn("Ошибка очистки осиротевших staging-файлов в {}: {}", dir, e.getMessage());
            }
        }
        if (totalDeleted > 0) {
            AgentMetrics.getInstance().recordStagingOrphansCleaned(totalDeleted);
        }
        return totalDeleted;
    }

    public Set<String> getKnownTargetDirectories() {
        return knownTargetDirectories;
    }

    public DataTransferServiceImpl(String stagingDir, HadoopFsManager fsManager, LocalBandwidthLimiter bandwidthLimiter, String keytabPath) {
        this(stagingDir, fsManager, bandwidthLimiter, keytabPath, 8);
    }

    public DataTransferServiceImpl(String stagingDir, HadoopFsManager fsManager, LocalBandwidthLimiter bandwidthLimiter) {
        this(stagingDir, fsManager, bandwidthLimiter, null, 8);
    }

    public int getMaxConcurrentTarStreams() {
        return maxConcurrentTarStreams;
    }

    public int getTarPipeBufferBytes() {
        return tarPipeBufferBytes;
    }

    public int getAvailableTarStreamSlots() {
        return tarStreamSemaphore.availablePermits();
    }

    public int getActiveTarStreamsCount() {
        return maxConcurrentTarStreams - tarStreamSemaphore.availablePermits();
    }

    @Override
    public void checkFile(CheckFileRequest request, StreamObserver<CheckFileResponse> responseObserver) {
        try {
            String path = request.getPath();
            boolean exists = fsManager.exists(path);
            long size = 0L;
            long mtime = 0L;
            boolean isDir = false;

            if (exists) {
                isDir = fsManager.isDirectory(path, request.getExecutionPrincipal(), request.getRunAsServiceAccount());
                if (!isDir) {
                    size = fsManager.getFileSize(path, request.getExecutionPrincipal(), request.getRunAsServiceAccount());
                    mtime = fsManager.getFileModificationTime(path, request.getExecutionPrincipal(), request.getRunAsServiceAccount());
                }
            }

            CheckFileResponse response = CheckFileResponse.newBuilder()
                    .setExists(exists)
                    .setSize(size)
                    .setModificationTime(mtime)
                    .setIsDirectory(isDir)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.warn("Ошибка при проверке файла {}: {}", request.getPath(), e.getMessage());
            responseObserver.onNext(CheckFileResponse.newBuilder()
                    .setExists(false)
                    .setSize(0L)
                    .setModificationTime(0L)
                    .setIsDirectory(false)
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getDirectoryManifest(DirectoryManifestRequest request, StreamObserver<DirectoryManifestResponse> responseObserver) {
        try {
            String path = request.getPath();
            if (HadoopFsManager.isIgnoredDirectoryPath(path)) {
                logger.info("Запрошенный каталог '{}' игнорируется согласно фильтру временных папок", path);
                responseObserver.onNext(DirectoryManifestResponse.newBuilder().setExists(true).build());
                responseObserver.onCompleted();
                return;
            }

            boolean exists = fsManager.exists(path);
            if (!exists) {
                responseObserver.onNext(DirectoryManifestResponse.newBuilder()
                        .setExists(false)
                        .build());
                responseObserver.onCompleted();
                return;
            }

            List<HadoopFsManager.FileItem> items = fsManager.listFilesRecursively(
                    path,
                    request.getExecutionPrincipal(),
                    request.getRunAsServiceAccount()
            );

            DirectoryManifestResponse.Builder builder = DirectoryManifestResponse.newBuilder().setExists(true);
            for (HadoopFsManager.FileItem item : items) {
                builder.addFiles(FileManifestEntry.newBuilder()
                        .setRelativePath(item.relativePath())
                        .setSize(item.size())
                        .setModificationTime(item.modificationTime())
                        .build());
            }

            logger.info("Сформирован манифест каталога '{}' для удаленного агента: {} файлов", path, items.size());
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.warn("Ошибка при получении манифеста каталога {}: {}", request.getPath(), e.getMessage());
            responseObserver.onNext(DirectoryManifestResponse.newBuilder()
                    .setExists(false)
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<TransferFileRequest> transferFile(StreamObserver<TransferFileResponse> responseObserver) {
        class TransferFileObserver implements StreamObserver<TransferFileRequest> {

            private FileMetadata metadata = null;
            private String stagingPath = null;
            private OutputStream outputStream = null;
            private long bytesWritten = 0;
            private MessageDigest sha256Digest = null;
            private final java.util.concurrent.atomic.AtomicBoolean stagingTracked = new java.util.concurrent.atomic.AtomicBoolean(false);

            {
                try {
                    sha256Digest = MessageDigest.getInstance("SHA-256");
                } catch (NoSuchAlgorithmException e) {
                    logger.error("Алгоритм SHA-256 не найден в JVM: {}", e.getMessage());
                }
            }

            @Override
            public void onNext(TransferFileRequest request) {
                try {
                    // 1. Прием метаданных файла (первое сообщение в потоке)
                    if (request.hasMetadata()) {
                        metadata = request.getMetadata();
                        recordTargetDirectory(metadata.getTargetPath());
                        // Прямой staging в целевой HDFS/ФС (targetPath._staging_<jobId>) без промежуточного локального диска
                        stagingPath = metadata.getTargetPath() + "._staging_" + metadata.getJobId();
                        // Предварительная очистка на случай оставшегося файла от предыдущей попытки
                        fsManager.deletePath(stagingPath, metadata.getExecutionPrincipal(), metadata.getRunAsServiceAccount());
                        outputStream = fsManager.openOutputStreamForWrite(
                                stagingPath,
                                metadata.getExecutionPrincipal(),
                                metadata.getRunAsServiceAccount()
                        );
                        AgentMetrics.getInstance().recordStagingFileStart();
                        stagingTracked.set(true);
                        logger.info("Начат прямой прием файла job_id={}: '{}' -> '{}' (staging: '{}')",
                                metadata.getJobId(), metadata.getSourcePath(), metadata.getTargetPath(), stagingPath);
                    }
                    // 2. Прием бинарного чанка данных
                    else if (request.hasChunk()) {
                        FileChunk chunk = request.getChunk();
                        if (outputStream == null || metadata == null) {
                            logger.error("Чанк получен до метаданных файла для job_id={}", chunk.getJobId());
                            responseObserver.onNext(TransferFileResponse.newBuilder()
                                    .setJobId(chunk.getJobId())
                                    .setSuccess(false)
                                    .setBytesWritten(0)
                                    .setMessage("Ошибка протокола: чанк передан до метаданных файла")
                                    .build());
                            responseObserver.onCompleted();
                            return;
                        }

                        ByteString data = chunk.getData();
                        if (!data.isEmpty()) {
                            byte[] rawBytes = data.toByteArray();
                            if (bandwidthLimiter != null) {
                                bandwidthLimiter.throttle(rawBytes.length);
                            }
                            byte[] bytes = WireCompressor.decompress(
                                    rawBytes,
                                    chunk.getCompressionCodec(),
                                    chunk.getUncompressedSize()
                            );
                            outputStream.write(bytes);
                            if (sha256Digest != null) {
                                sha256Digest.update(bytes);
                            }
                            bytesWritten += bytes.length;
                            AgentMetrics.getInstance().recordStagingBytes(bytes.length);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Ошибка при обработке чанка репликации: {}", e.getMessage(), e);
                    cleanup();
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Прерван gRPC поток передачи файла: {}", t.getMessage());
                cleanup();
            }

            @Override
            public void onCompleted() {
                if (metadata == null || outputStream == null) {
                    responseObserver.onNext(TransferFileResponse.newBuilder()
                            .setJobId("unknown")
                            .setSuccess(false)
                            .setBytesWritten(0)
                            .setMessage("Пустой поток: метаданные не были получены")
                            .build());
                    responseObserver.onCompleted();
                    return;
                }

                try {
                    outputStream.flush();
                    outputStream.close();
                    outputStream = null;

                    String checksum = sha256Digest != null ? bytesToHex(sha256Digest.digest()) : "";

                    // Атомарный rename из staging в целевой файл в HDFS/ФС
                    boolean renamed = fsManager.renameFile(
                            stagingPath,
                            metadata.getTargetPath(),
                            metadata.getExecutionPrincipal(),
                            metadata.getRunAsServiceAccount()
                    );

                    if (!renamed) {
                        throw new IOException("Не удалось выполнить renameFile: " + stagingPath + " -> " + metadata.getTargetPath());
                    }

                    if (stagingTracked.compareAndSet(true, false)) {
                        AgentMetrics.getInstance().recordStagingFileComplete(bytesWritten);
                    }

                    logger.info("Файл успешно принят и закоммичен в HDFS: job_id={}, байт={}, sha256={}, path='{}'",
                            metadata.getJobId(), bytesWritten, checksum, metadata.getTargetPath());

                    TransferFileResponse response = TransferFileResponse.newBuilder()
                            .setJobId(metadata.getJobId())
                            .setSuccess(true)
                            .setBytesWritten(bytesWritten)
                            .setTargetPath(metadata.getTargetPath())
                            .setChecksum(checksum)
                            .setMessage("Файл успешно принят и сохранен в: " + metadata.getTargetPath())
                            .build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    logger.error("Ошибка при атомарном коммите принятого файла: {}", e.getMessage(), e);
                    cleanup();
                    responseObserver.onNext(TransferFileResponse.newBuilder()
                            .setJobId(metadata.getJobId())
                            .setSuccess(false)
                            .setBytesWritten(bytesWritten)
                            .setMessage("Ошибка коммита файла: " + e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                }
            }

            void cleanup() {
                if (stagingTracked.compareAndSet(true, false)) {
                    AgentMetrics.getInstance().recordStagingFileComplete(bytesWritten);
                }
                if (outputStream != null) {
                    try {
                        outputStream.close();
                    } catch (IOException ignored) {}
                    outputStream = null;
                }
                if (stagingPath != null && metadata != null) {
                    fsManager.deletePath(stagingPath, metadata.getExecutionPrincipal(), metadata.getRunAsServiceAccount());
                }
            }
        }

        TransferFileObserver observer = new TransferFileObserver();
        if (responseObserver instanceof ServerCallStreamObserver<TransferFileResponse> serverCallStreamObserver) {
            serverCallStreamObserver.setOnCancelHandler(() -> {
                logger.warn("Клиент прервал (CANCELLED) gRPC вызов передачи файла");
                observer.cleanup();
            });
        }
        return observer;
    }

    @Override
    public StreamObserver<TarStreamRequest> transferTarStream(StreamObserver<TarStreamResponse> responseObserver) {
        class TarStreamObserver implements StreamObserver<TarStreamRequest> {

            private TarStreamMetadata metadata = null;
            private PipedOutputStream pipedOut = null;
            private PipedInputStream pipedIn = null;
            private CompletableFuture<TarStreamResponse> unpackFuture = null;
            private long bytesReceived = 0;
            private final Set<String> activeStagingPaths = ConcurrentHashMap.newKeySet();
            private final Map<String, Long> activeStagingSizes = new ConcurrentHashMap<>();
            private final AtomicBoolean permitAcquired = new AtomicBoolean(false);

            private void releasePermit() {
                if (permitAcquired.compareAndSet(true, false)) {
                    tarStreamSemaphore.release();
                    logger.debug("Освобожден слот семафора TAR-распаковки (доступно: {}/{})",
                            tarStreamSemaphore.availablePermits(), maxConcurrentTarStreams);
                }
            }

            @Override
            public void onNext(TarStreamRequest request) {
                try {
                    // 1. Метаданные бандла (первое сообщение в потоке)
                    if (request.hasMetadata()) {
                        metadata = request.getMetadata();
                        recordTargetDirectory(metadata.getBaseTargetPath());

                        // Ограничение пула памяти: захват слота семафора для TAR-распаковки
                        boolean acquired = false;
                        try {
                            acquired = tarStreamSemaphore.tryAcquire(60, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            logger.warn("Прервано ожидание слота памяти TAR-распаковки для task_id={}", metadata.getTaskId());
                        }

                        if (!acquired) {
                            String errMsg = "Пул памяти TAR-распаковки перегружен: превышен лимит параллельных потоков ("
                                    + maxConcurrentTarStreams + ") или тайм-аут ожидания слота";
                            logger.error(errMsg + " для task_id={}", metadata.getTaskId());
                            responseObserver.onNext(TarStreamResponse.newBuilder()
                                    .setTaskId(metadata.getTaskId())
                                    .setSuccess(false)
                                    .setMessage(errMsg)
                                    .build());
                            responseObserver.onCompleted();
                            return;
                        }
                        permitAcquired.set(true);

                        pipedOut = new PipedOutputStream();
                        pipedIn = new PipedInputStream(pipedOut, tarPipeBufferBytes);

                        final TarStreamMetadata meta = metadata;
                        final InputStream inStream = pipedIn;

                        // Ограничение параллелизма in-flight коммитов файлов одного бандла для сдерживания памяти
                        final Semaphore fileCommitLimiter = new Semaphore(Math.max(2, bundleCommitConcurrency * 2));

                        // Потоковая распаковка виртуального TAR-стрима и параллельный Zero-Staging коммит в HDFS
                        unpackFuture = CompletableFuture.supplyAsync(() -> {
                            List<TarFileCommitResult> results = new CopyOnWriteArrayList<>();
                            List<CompletableFuture<Void>> commitFutures = new ArrayList<>();
                            AtomicInteger committedCount = new AtomicInteger(0);
                            AtomicLong totalBytesWritten = new AtomicLong(0);

                            try (TarArchiveInputStream tarIn = new TarArchiveInputStream(new BufferedInputStream(inStream, 64 * 1024))) {
                                TarArchiveEntry entry;
                                while ((entry = tarIn.getNextTarEntry()) != null) {
                                    if (entry.isDirectory()) {
                                        continue;
                                    }
                                    String relPath = entry.getName();
                                    long entrySize = entry.getSize();
                                    ByteArrayOutputStream baos = new ByteArrayOutputStream((int) Math.max(0, Math.min(entrySize, 16 * 1024 * 1024)));
                                    IOUtils.copyBytes(tarIn, baos, 32768, false);
                                    byte[] fileData = baos.toByteArray();

                                    String targetPath = combinePaths(meta.getBaseTargetPath(), relPath);
                                    String stagingPath = targetPath + "._staging_" + meta.getTaskId();
                                    activeStagingPaths.add(stagingPath);
                                    activeStagingSizes.put(stagingPath, (long) fileData.length);
                                    AgentMetrics.getInstance().recordStagingFileStart();
                                    AgentMetrics.getInstance().recordStagingBytes(fileData.length);

                                    // Захватываем разрешение на коммит файла (сдерживает чтение из tarIn и pipe buffer)
                                    try {
                                        fileCommitLimiter.acquire();
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                        throw new IOException("Прервано ожидание слота коммита файла: " + relPath, ie);
                                    }

                                    CompletableFuture<Void> fut = CompletableFuture.runAsync(() -> {
                                        try {
                                            fsManager.writeDirect(
                                                    stagingPath,
                                                    fileData,
                                                    meta.getExecutionPrincipal(),
                                                    meta.getRunAsServiceAccount()
                                            );
                                            fsManager.renameFile(
                                                    stagingPath,
                                                    targetPath,
                                                    meta.getExecutionPrincipal(),
                                                    meta.getRunAsServiceAccount()
                                            );
                                            activeStagingPaths.remove(stagingPath);
                                            Long sz = activeStagingSizes.remove(stagingPath);
                                            if (sz != null) {
                                                AgentMetrics.getInstance().recordStagingFileComplete(sz);
                                            }
                                            committedCount.incrementAndGet();
                                            totalBytesWritten.addAndGet(fileData.length);
                                            results.add(TarFileCommitResult.newBuilder()
                                                    .setRelativePath(relPath)
                                                    .setTargetPath(targetPath)
                                                    .setBytesWritten(fileData.length)
                                                    .setSuccess(true)
                                                    .build());
                                        } catch (Exception e) {
                                            logger.error("Ошибка при прямом коммите файла {} в HDFS: {}", targetPath, e.getMessage(), e);
                                            try {
                                                fsManager.deletePath(stagingPath, meta.getExecutionPrincipal(), meta.getRunAsServiceAccount());
                                            } catch (Exception ignored) {}
                                            activeStagingPaths.remove(stagingPath);
                                            Long sz = activeStagingSizes.remove(stagingPath);
                                            if (sz != null) {
                                                AgentMetrics.getInstance().recordStagingFileComplete(sz);
                                            }
                                            results.add(TarFileCommitResult.newBuilder()
                                                    .setRelativePath(relPath)
                                                    .setTargetPath(targetPath)
                                                    .setBytesWritten(0)
                                                    .setSuccess(false)
                                                    .setErrorMessage(e.getMessage() != null ? e.getMessage() : "Unknown I/O error")
                                                    .build());
                                        } finally {
                                            fileCommitLimiter.release();
                                        }
                                    }, bundleCommitExecutor);
                                    commitFutures.add(fut);
                                }

                                CompletableFuture.allOf(commitFutures.toArray(new CompletableFuture[0])).join();

                                boolean allOk = results.stream().allMatch(TarFileCommitResult::getSuccess);
                                return TarStreamResponse.newBuilder()
                                        .setTaskId(meta.getTaskId())
                                        .setSuccess(allOk)
                                        .setFilesCommitted(committedCount.get())
                                        .setBytesWritten(totalBytesWritten.get())
                                        .setMessage(allOk ? "Все файлы виртуального TAR-стрима успешно сохранены напрямую в HDFS" : "Часть файлов завершилась ошибкой")
                                        .addAllFiles(results)
                                        .build();
                            } catch (Exception e) {
                                logger.error("Ошибка потоковой распаковки TAR-архива task_id={}: {}", meta.getTaskId(), e.getMessage(), e);
                                return TarStreamResponse.newBuilder()
                                        .setTaskId(meta.getTaskId())
                                        .setSuccess(false)
                                        .setFilesCommitted(committedCount.get())
                                        .setBytesWritten(totalBytesWritten.get())
                                        .setMessage("Ошибка распаковки TAR: " + e.getMessage())
                                        .addAllFiles(results)
                                        .build();
                            }
                        });

                        logger.info("Начат прием виртуального TAR-стрима task_id={}, job_id={}, файлов={}, суммарный объем={} (буфер: {} КБ, активно слотов: {}/{})",
                                metadata.getTaskId(), metadata.getJobId(), metadata.getTotalFiles(), metadata.getTotalBytes(),
                                tarPipeBufferBytes / 1024, maxConcurrentTarStreams - tarStreamSemaphore.availablePermits(), maxConcurrentTarStreams);
                    }
                    // 2. Чанк данных архива
                    else if (request.hasChunk()) {
                        TarStreamChunk chunk = request.getChunk();
                        if (pipedOut == null || metadata == null) {
                            logger.error("Чанк TAR получен до метаданных task_id={}", chunk.getTaskId());
                            cleanup();
                            responseObserver.onNext(TarStreamResponse.newBuilder()
                                    .setTaskId(chunk.getTaskId())
                                    .setSuccess(false)
                                    .setMessage("Ошибка протокола: чанк передан до метаданных бандла")
                                    .build());
                            responseObserver.onCompleted();
                            return;
                        }
                        ByteString data = chunk.getData();
                        if (!data.isEmpty()) {
                            byte[] rawBytes = data.toByteArray();
                            if (bandwidthLimiter != null) {
                                bandwidthLimiter.throttle(rawBytes.length);
                            }
                            byte[] bytes = WireCompressor.decompress(
                                    rawBytes,
                                    chunk.getCompressionCodec(),
                                    chunk.getUncompressedSize()
                            );
                            pipedOut.write(bytes);
                            bytesReceived += rawBytes.length;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Ошибка при обработке чанка TAR-стрима: {}", e.getMessage(), e);
                    cleanup();
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Прерван gRPC поток TAR-стрима: {}", t.getMessage());
                cleanup();
            }

            @Override
            public void onCompleted() {
                if (metadata == null || pipedOut == null || unpackFuture == null) {
                    cleanup();
                    responseObserver.onNext(TarStreamResponse.newBuilder()
                            .setTaskId(metadata != null ? metadata.getTaskId() : "unknown")
                            .setSuccess(false)
                            .setMessage("Пустой поток: метаданные бандла не были получены")
                            .build());
                    responseObserver.onCompleted();
                    return;
                }

                try {
                    pipedOut.flush();
                    pipedOut.close();
                    TarStreamResponse response = unpackFuture.get(120, TimeUnit.SECONDS);
                    logger.info("Виртуальный TAR-стрим task_id={} завершен: успешно={}, файлов={}, байт={}",
                            metadata.getTaskId(), response.getSuccess(), response.getFilesCommitted(), response.getBytesWritten());
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    logger.error("Ошибка ожидания распаковки TAR-стрима task_id={}: {}", metadata.getTaskId(), e.getMessage(), e);
                    responseObserver.onNext(TarStreamResponse.newBuilder()
                            .setTaskId(metadata.getTaskId())
                            .setSuccess(false)
                            .setMessage("Ошибка распаковки бандла: " + e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                } finally {
                    cleanup();
                }
            }

            void cleanup() {
                releasePermit();
                if (pipedOut != null) {
                    try {
                        pipedOut.close();
                    } catch (IOException ignored) {}
                    pipedOut = null;
                }
                if (pipedIn != null) {
                    try {
                        pipedIn.close();
                    } catch (IOException ignored) {}
                    pipedIn = null;
                }
                for (String staging : activeStagingPaths) {
                    try {
                        if (metadata != null) {
                            fsManager.deletePath(staging, metadata.getExecutionPrincipal(), metadata.getRunAsServiceAccount());
                        } else {
                            fsManager.deletePath(staging, null, true);
                        }
                        logger.debug("Очищен незавершенный staging-файл бандла: {}", staging);
                    } catch (Exception ignored) {}
                    Long sz = activeStagingSizes.remove(staging);
                    if (sz != null) {
                        AgentMetrics.getInstance().recordStagingFileComplete(sz);
                    }
                }
                activeStagingPaths.clear();
                activeStagingSizes.clear();
            }
        }

        TarStreamObserver observer = new TarStreamObserver();
        if (responseObserver instanceof ServerCallStreamObserver<TarStreamResponse> serverCallStreamObserver) {
            serverCallStreamObserver.setOnCancelHandler(() -> {
                logger.warn("Клиент прервал (CANCELLED) gRPC вызов виртуального TAR-стрима");
                observer.cleanup();
            });
        }
        return observer;
    }

    @Override
    public void renamePath(RenamePathRequest request, StreamObserver<RenamePathResponse> responseObserver) {
        String src = request.getSourcePath();
        String dst = request.getTargetPath();
        logger.info("[gRPC Receiver] Запрос атомарного переименования: '{}' -> '{}' (job_id={})", src, dst, request.getJobId());

        try {
            boolean success = fsManager.renameFile(src, dst, request.getExecutionPrincipal(), request.getRunAsServiceAccount());
            responseObserver.onNext(RenamePathResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(success)
                    .setMessage(success ? "Путь успешно переименован" : "Не удалось переименовать путь в целевой ФС")
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("[gRPC Receiver] Ошибка переименования '{}' -> '{}': {}", src, dst, e.getMessage(), e);
            responseObserver.onNext(RenamePathResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(false)
                    .setMessage("Ошибка переименования: " + e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void deletePath(DeletePathRequest request, StreamObserver<DeletePathResponse> responseObserver) {
        String path = request.getPath();
        logger.info("[gRPC Receiver] Запрос удаления пути: '{}' (recursive={}, job_id={})", path, request.getRecursive(), request.getJobId());

        try {
            if (path == null || path.trim().isEmpty() || path.trim().equals("/") || path.trim().equals("/user")
                    || path.trim().equals("/tmp") || path.trim().equals("/etc") || path.trim().equals("/system")) {
                throw new SecurityException("Запрещено удаление системного или корневого каталога: " + path);
            }
            boolean success = fsManager.deletePath(path, request.getRecursive(), request.getExecutionPrincipal(), request.getRunAsServiceAccount());
            responseObserver.onNext(DeletePathResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(success)
                    .setMessage(success ? "Путь успешно удален" : "Путь не найден или не удален")
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("[gRPC Receiver] Ошибка удаления '{}': {}", path, e.getMessage(), e);
            responseObserver.onNext(DeletePathResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setSuccess(false)
                    .setMessage("Ошибка удаления: " + e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    public static String combinePaths(String base, String rel) {
        if (base == null || base.isEmpty()) base = "/";
        if (rel == null || rel.isEmpty()) return base;

        // Защита от Path Traversal / Zip Slip (CWE-22):
        String cleanRel = rel.replace('\\', '/').replaceAll("^/+", "");
        java.nio.file.Path basePath = java.nio.file.Paths.get(base).normalize();
        java.nio.file.Path resolvedPath = basePath.resolve(cleanRel).normalize();

        if (!resolvedPath.startsWith(basePath)) {
            throw new SecurityException("Попытка Path Traversal (Zip Slip) обнаружена в TAR-записи: '"
                    + rel + "' пытается выйти за пределы базовой директории '" + base + "'");
        }

        String result = resolvedPath.toString().replace('\\', '/');
        if (base.startsWith("/") && !result.startsWith("/")) {
            result = "/" + result;
        }
        return result;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
