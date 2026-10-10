package org.apache.hadoop.explorer.replicator.receiver;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.explorer.replicator.compression.WireCompressor;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
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

    public DataTransferServiceImpl(String stagingDir, HadoopFsManager fsManager, LocalBandwidthLimiter bandwidthLimiter, String keytabPath, int bundleCommitConcurrency) {
        this.stagingDir = stagingDir != null ? stagingDir : "/tmp/staging";
        this.fsManager = fsManager;
        this.bandwidthLimiter = bandwidthLimiter;
        this.keytabPath = keytabPath;
        int threads = bundleCommitConcurrency > 0 ? bundleCommitConcurrency : 8;
        this.bundleCommitExecutor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "bundle-hdfs-committer");
            t.setDaemon(true);
            return t;
        });

        File dir = new File(this.stagingDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
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
        return new StreamObserver<>() {

            private FileMetadata metadata = null;
            private String stagingPath = null;
            private OutputStream outputStream = null;
            private long bytesWritten = 0;
            private MessageDigest sha256Digest = null;

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

            private void cleanup() {
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
        };
    }

    @Override
    public StreamObserver<TarStreamRequest> transferTarStream(StreamObserver<TarStreamResponse> responseObserver) {
        return new StreamObserver<>() {

            private TarStreamMetadata metadata = null;
            private PipedOutputStream pipedOut = null;
            private PipedInputStream pipedIn = null;
            private CompletableFuture<TarStreamResponse> unpackFuture = null;
            private long bytesReceived = 0;
            private final Set<String> activeStagingPaths = ConcurrentHashMap.newKeySet();

            @Override
            public void onNext(TarStreamRequest request) {
                try {
                    // 1. Метаданные бандла (первое сообщение в потоке)
                    if (request.hasMetadata()) {
                        metadata = request.getMetadata();
                        recordTargetDirectory(metadata.getBaseTargetPath());
                        pipedOut = new PipedOutputStream();
                        pipedIn = new PipedInputStream(pipedOut, 4 * 1024 * 1024);

                        final TarStreamMetadata meta = metadata;
                        final InputStream inStream = pipedIn;

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
                                            results.add(TarFileCommitResult.newBuilder()
                                                    .setRelativePath(relPath)
                                                    .setTargetPath(targetPath)
                                                    .setBytesWritten(0)
                                                    .setSuccess(false)
                                                    .setErrorMessage(e.getMessage() != null ? e.getMessage() : "Unknown I/O error")
                                                    .build());
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

                        logger.info("Начат прием виртуального TAR-стрима task_id={}, job_id={}, файлов={}, суммарный объем={}",
                                metadata.getTaskId(), metadata.getJobId(), metadata.getTotalFiles(), metadata.getTotalBytes());
                    }
                    // 2. Чанк данных архива
                    else if (request.hasChunk()) {
                        TarStreamChunk chunk = request.getChunk();
                        if (pipedOut == null || metadata == null) {
                            logger.error("Чанк TAR получен до метаданных task_id={}", chunk.getTaskId());
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
                    responseObserver.onNext(TarStreamResponse.newBuilder()
                            .setTaskId("unknown")
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
                    cleanup();
                    responseObserver.onNext(TarStreamResponse.newBuilder()
                            .setTaskId(metadata.getTaskId())
                            .setSuccess(false)
                            .setMessage("Ошибка распаковки бандла: " + e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                }
            }

            private void cleanup() {
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
                }
                activeStagingPaths.clear();
            }
        };
    }

    public static String combinePaths(String base, String rel) {
        if (base == null || base.isEmpty()) return rel;
        if (rel == null || rel.isEmpty()) return base;
        String cleanBase = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        String cleanRel = rel.startsWith("/") ? rel.substring(1) : rel;
        return cleanBase + "/" + cleanRel;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
