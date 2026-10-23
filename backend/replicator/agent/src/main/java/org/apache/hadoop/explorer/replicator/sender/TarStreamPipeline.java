package org.apache.hadoop.explorer.replicator.sender;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.compression.WireCompressor;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.metrics.AgentMetrics;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Конвейер потоковой сборки, сжатия и передачи виртуальных TAR-бандлов (Tar-Streaming Pipeline).
 * Упаковывает пачки мелких файлов на лету без промежуточного сохранения на локальный диск.
 */
public class TarStreamPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TarStreamPipeline.class);

    private final ObjectMapper objectMapper;

    public TarStreamPipeline(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
    }

    public TarStreamPipeline() {
        this(new ObjectMapper());
    }

    /**
     * Потоковая передача пакета мелких файлов в виде единого непрерывного TAR-архива на лету.
     */
    public boolean transferBundleTarTask(TaskItemDto task,
                                         ManagedChannel channel,
                                         String workerId,
                                         OrchestratorGrpcClient orchestratorClient,
                                         HadoopFsManager fsManager,
                                         LocalBandwidthLimiter bandwidthLimiter,
                                         int chunkSize,
                                         CompressionCodec defaultCompressionCodec,
                                         int compressionLevel,
                                         ConcurrentHashMap<String, AtomicLong> tokenBatchLease) {
        String taskId = task.getId();
        String jobId = task.getJobId();
        String targetPath = task.getTargetPath();
        long totalBytes = task.getFileSize();
        int fileCount = task.getFileCount();
        String targetAddress = task.getTargetAddress();

        logger.info("Воркер '{}' начинает потоковую передачу бандла TAR '{}': {} файлов ({} байт) в '{}' на {}",
                workerId, taskId, fileCount, totalBytes, targetPath, targetAddress);

        List<ReplicationSender.BundleFileEntry> entries;
        try {
            entries = objectMapper.readValue(task.getBundleManifest(), new TypeReference<List<ReplicationSender.BundleFileEntry>>() {});
        } catch (Exception e) {
            String err = "Ошибка десериализации bundleManifest для задачи " + taskId + ": " + e.getMessage();
            logger.error(err);
            orchestratorClient.failTask(new FailTaskRequest(taskId, workerId, err));
            return false;
        }

        StreamObserver<TarStreamRequest> requestObserver = null;
        try {
            DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);
            CompletableFuture<TarStreamResponse> responseFuture = new CompletableFuture<>();
            AtomicReference<ClientCallStreamObserver<TarStreamRequest>> clientObserverRef = new AtomicReference<>();
            ReentrantLock readyLock = new ReentrantLock();
            Condition readyCondition = readyLock.newCondition();

            requestObserver = stub.transferTarStream(new ClientResponseObserver<TarStreamRequest, TarStreamResponse>() {
                @Override
                public void beforeStart(ClientCallStreamObserver<TarStreamRequest> stream) {
                    clientObserverRef.set(stream);
                    stream.setOnReadyHandler(() -> {
                        readyLock.lock();
                        try {
                            readyCondition.signalAll();
                        } finally {
                            readyLock.unlock();
                        }
                    });
                }

                @Override
                public void onNext(TarStreamResponse value) {
                    responseFuture.complete(value);
                }

                @Override
                public void onError(Throwable t) {
                    responseFuture.completeExceptionally(t);
                    readyLock.lock();
                    try {
                        readyCondition.signalAll();
                    } finally {
                        readyLock.unlock();
                    }
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

            TarStreamMetadata.Builder metaBuilder = TarStreamMetadata.newBuilder()
                    .setJobId(jobId)
                    .setTaskId(taskId)
                    .setBaseTargetPath(targetPath)
                    .setTotalFiles(entries.size())
                    .setTotalBytes(totalBytes)
                    .setRunAsServiceAccount(asService)
                    .setExecutionPrincipal(principal)
                    .setCompressionCodec(tarCodec);
            if (task.getTaskToken() != null && !task.getTaskToken().isBlank()) {
                metaBuilder.setTaskToken(task.getTaskToken());
            }
            TarStreamMetadata metadata = metaBuilder.build();

            requestObserver.onNext(TarStreamRequest.newBuilder().setMetadata(metadata).build());

            // 2. Потоковая упаковка TAR на лету в памяти с чанкованием, сжатием, шейпингом и flow-control
            TarChunkingOutputStream chunkingOut = new TarChunkingOutputStream(
                    taskId,
                    requestObserver,
                    chunkSize,
                    bandwidthLimiter,
                    orchestratorClient,
                    workerId,
                    tarCodec,
                    compressionLevel,
                    task.getSourceClusterId(),
                    task.getTargetClusterId(),
                    tokenBatchLease,
                    clientObserverRef,
                    responseFuture,
                    readyLock,
                    readyCondition
            );

            try (TarArchiveOutputStream tarOut = new TarArchiveOutputStream(new BufferedOutputStream(chunkingOut, 32 * 1024))) {
                tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                tarOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

                for (ReplicationSender.BundleFileEntry entry : entries) {
                    if (!fsManager.exists(entry.getSourcePath(), principal, asService)) {
                        logger.warn("Файл бандла '{}' не найден в HDFS, пропуск до формирования записи TAR", entry.getSourcePath());
                        continue;
                    }

                    TarArchiveEntry tarEntry = new TarArchiveEntry(entry.getRelativePath());
                    tarEntry.setSize(entry.getSize());
                    tarOut.putArchiveEntry(tarEntry);

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
        private final OrchestratorGrpcClient orchestratorClient;
        private final String workerId;
        private final CompressionCodec codec;
        private final int compressionLevel;
        private final String sourceClusterId;
        private final String targetClusterId;
        private final ConcurrentHashMap<String, AtomicLong> tokenBatchLease;
        private final byte[] buffer;
        private int count = 0;
        private long offset = 0;
        private final AtomicReference<ClientCallStreamObserver<TarStreamRequest>> clientObserverRef;
        private final CompletableFuture<?> responseFuture;
        private final ReentrantLock readyLock;
        private final Condition readyCondition;

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId) {
            this(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId,
                    CompressionCodec.COMPRESSION_ZSTD, WireCompressor.DEFAULT_ZSTD_LEVEL, null, null);
        }

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId,
                                       CompressionCodec codec, int compressionLevel) {
            this(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId,
                    codec, compressionLevel, null, null);
        }

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId,
                                       CompressionCodec codec, int compressionLevel,
                                       String sourceClusterId, String targetClusterId) {
            this(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId,
                    codec, compressionLevel, sourceClusterId, targetClusterId, null);
        }

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId,
                                       CompressionCodec codec, int compressionLevel,
                                       String sourceClusterId, String targetClusterId,
                                       ConcurrentHashMap<String, AtomicLong> tokenBatchLease) {
            this(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId,
                    codec, compressionLevel, sourceClusterId, targetClusterId, tokenBatchLease, null, null, null);
        }

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId,
                                       CompressionCodec codec, int compressionLevel,
                                       String sourceClusterId, String targetClusterId,
                                       ConcurrentHashMap<String, AtomicLong> tokenBatchLease,
                                       AtomicReference<ClientCallStreamObserver<TarStreamRequest>> clientObserverRef,
                                       CompletableFuture<?> responseFuture,
                                       Object readyLock) {
            this(taskId, requestObserver, chunkSize, bandwidthLimiter, orchestratorClient, workerId,
                    codec, compressionLevel, sourceClusterId, targetClusterId, tokenBatchLease,
                    clientObserverRef, responseFuture,
                    readyLock instanceof ReentrantLock ? (ReentrantLock) readyLock : null,
                    null);
        }

        public TarChunkingOutputStream(String taskId, StreamObserver<TarStreamRequest> requestObserver,
                                       int chunkSize, LocalBandwidthLimiter bandwidthLimiter,
                                       OrchestratorGrpcClient orchestratorClient, String workerId,
                                       CompressionCodec codec, int compressionLevel,
                                       String sourceClusterId, String targetClusterId,
                                       ConcurrentHashMap<String, AtomicLong> tokenBatchLease,
                                       AtomicReference<ClientCallStreamObserver<TarStreamRequest>> clientObserverRef,
                                       CompletableFuture<?> responseFuture,
                                       ReentrantLock readyLock,
                                       Condition readyCondition) {
            this.taskId = taskId;
            this.requestObserver = requestObserver;
            this.chunkSize = chunkSize > 0 ? chunkSize : 64 * 1024;
            this.bandwidthLimiter = bandwidthLimiter;
            this.orchestratorClient = orchestratorClient;
            this.workerId = workerId;
            this.codec = codec != null ? codec : CompressionCodec.COMPRESSION_NONE;
            this.compressionLevel = compressionLevel > 0 ? compressionLevel : WireCompressor.DEFAULT_ZSTD_LEVEL;
            this.sourceClusterId = sourceClusterId;
            this.targetClusterId = targetClusterId;
            this.tokenBatchLease = tokenBatchLease != null ? tokenBatchLease : new ConcurrentHashMap<>();
            this.buffer = new byte[this.chunkSize];
            this.clientObserverRef = clientObserverRef;
            this.responseFuture = responseFuture;
            this.readyLock = readyLock != null ? readyLock : new ReentrantLock();
            this.readyCondition = readyCondition != null ? readyCondition : this.readyLock.newCondition();
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

            // Flow Control backpressure: ожидаем готовности сокета Netty без thread pinning на виртуальных потоках
            if (clientObserverRef != null && responseFuture != null && readyLock != null && readyCondition != null) {
                ClientCallStreamObserver<TarStreamRequest> stream = clientObserverRef.get();
                if (stream != null) {
                    readyLock.lock();
                    try {
                        while (!stream.isReady() && !responseFuture.isDone()) {
                            try {
                                readyCondition.await(100, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException("Поток отправки TAR был прерван при ожидании сокета", e);
                            }
                        }
                    } finally {
                        readyLock.unlock();
                    }
                }
            }

            WireCompressor.CompressedPayload payload = WireCompressor.compress(
                    buffer, 0, count, codec, compressionLevel
            );
            int wireBytes = payload.bytes().length;
            AgentMetrics.getInstance().recordCompression(payload.appliedCodec().name(), count, wireBytes);

            if (bandwidthLimiter != null) {
                bandwidthLimiter.throttle(wireBytes);
            }
            if (orchestratorClient != null && workerId != null) {
                ReplicationSender.acquireNetworkTokens(this.tokenBatchLease, this.orchestratorClient, this.workerId,
                        wireBytes, this.sourceClusterId, this.targetClusterId);
            }

            TarStreamChunk chunk = TarStreamChunk.newBuilder()
                    .setTaskId(taskId)
                    .setOffset(offset)
                    .setData(ByteString.copyFrom(payload.bytes()))
                    .setCompressionCodec(payload.appliedCodec())
                    .setUncompressedSize(count)
                    .build();

            long chunkSendStart = System.nanoTime();
            requestObserver.onNext(TarStreamRequest.newBuilder().setChunk(chunk).build());
            AgentMetrics.getInstance().recordChunkTransferLatency(System.nanoTime() - chunkSendStart);

            offset += count;
            count = 0;
        }

        @Override
        public void flush() throws IOException {
            flushBuffer();
        }
    }
}
