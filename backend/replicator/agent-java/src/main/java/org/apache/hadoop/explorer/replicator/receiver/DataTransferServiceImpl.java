package org.apache.hadoop.explorer.replicator.receiver;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * gRPC Servicer сервиса DataTransferService для приема файлов и атомарного коммита в HDFS/FS.
 */
public class DataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DataTransferServiceImpl.class);

    private final String stagingDir;
    private final HadoopFsManager fsManager;
    private final LocalBandwidthLimiter bandwidthLimiter;
    private final String keytabPath;

    public DataTransferServiceImpl(String stagingDir, HadoopFsManager fsManager, LocalBandwidthLimiter bandwidthLimiter, String keytabPath) {
        this.stagingDir = stagingDir != null ? stagingDir : "/tmp/staging";
        this.fsManager = fsManager;
        this.bandwidthLimiter = bandwidthLimiter;
        this.keytabPath = keytabPath;

        File dir = new File(this.stagingDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    public DataTransferServiceImpl(String stagingDir, HadoopFsManager fsManager, LocalBandwidthLimiter bandwidthLimiter) {
        this(stagingDir, fsManager, bandwidthLimiter, null);
    }

    @Override
    public StreamObserver<TransferFileRequest> transferFile(StreamObserver<TransferFileResponse> responseObserver) {
        return new StreamObserver<>() {

            private FileMetadata metadata = null;
            private File stagingFile = null;
            private FileOutputStream fileOutputStream = null;
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
                        String jobFileName = metadata.getJobId() + "_" + new File(metadata.getTargetPath()).getName();
                        stagingFile = new File(stagingDir, jobFileName);
                        if (stagingFile.getParentFile() != null) {
                            stagingFile.getParentFile().mkdirs();
                        }
                        fileOutputStream = new FileOutputStream(stagingFile);
                        logger.info("Начат прием файла job_id={}: '{}' -> '{}' (staging: '{}')",
                                metadata.getJobId(), metadata.getSourcePath(), metadata.getTargetPath(), stagingFile.getAbsolutePath());
                    }
                    // 2. Прием бинарного чанка данных
                    else if (request.hasChunk()) {
                        FileChunk chunk = request.getChunk();
                        if (fileOutputStream == null || metadata == null) {
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
                            byte[] bytes = data.toByteArray();
                            if (bandwidthLimiter != null) {
                                bandwidthLimiter.throttle(bytes.length);
                            }
                            fileOutputStream.write(bytes);
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
                if (metadata == null || stagingFile == null) {
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
                    if (fileOutputStream != null) {
                        fileOutputStream.flush();
                        fileOutputStream.close();
                        fileOutputStream = null;
                    }

                    String checksum = sha256Digest != null ? bytesToHex(sha256Digest.digest()) : "";

                    // Атомарный коммит принятого файла в HDFS или локальную ФС с doAs имперсонацией
                    String committedPath = fsManager.commitFile(
                            stagingFile.getAbsolutePath(),
                            metadata.getTargetPath(),
                            metadata.getExecutionPrincipal(),
                            metadata.getRunAsServiceAccount()
                    );

                    logger.info("Файл успешно принят и закоммичен: job_id={}, байт={}, sha256={}, path='{}'",
                            metadata.getJobId(), bytesWritten, checksum, committedPath);

                    TransferFileResponse response = TransferFileResponse.newBuilder()
                            .setJobId(metadata.getJobId())
                            .setSuccess(true)
                            .setBytesWritten(bytesWritten)
                            .setTargetPath(committedPath)
                            .setChecksum(checksum)
                            .setMessage("Файл успешно принят и сохранен в: " + committedPath)
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
                if (fileOutputStream != null) {
                    try {
                        fileOutputStream.close();
                    } catch (IOException ignored) {}
                    fileOutputStream = null;
                }
                if (stagingFile != null && stagingFile.exists()) {
                    stagingFile.delete();
                }
            }
        };
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
