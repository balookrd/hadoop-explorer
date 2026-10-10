package org.apache.hadoop.explorer.replicator.receiver;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование очистки HDFS Staging файлов при падении агентов и обрывах связи")
public class HdfsStagingCleanupTest {

    private File tempTargetDir;
    private HadoopFsManager fsManager;
    private DataTransferServiceImpl service;

    @BeforeEach
    void setUp() throws IOException {
        tempTargetDir = Files.createTempDirectory("staging-cleanup-test").toFile();
        fsManager = new HadoopFsManager("file:///", null, null);
        service = new DataTransferServiceImpl(
                new File(tempTargetDir, "staging").getAbsolutePath(),
                fsManager,
                null,
                null,
                4
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tempTargetDir != null && tempTargetDir.exists()) {
            Files.walk(tempTargetDir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Очистка staging-файлов по jobIdFilter: удаляются только файлы целевого задания")
    void testCleanStagingFilesByJobId() throws IOException {
        // Создаем боевой файл
        File prodFile = new File(tempTargetDir, "sales_2026.parquet");
        Files.writeString(prodFile.toPath(), "VALID_PRODUCTION_DATA");

        // Создаем staging-файлы для job-101 и job-102
        File stagingJob1 = new File(tempTargetDir, "sales_2026.parquet._staging_job-101");
        File stagingJob1Nested = new File(tempTargetDir, "nested/orders._staging_job-101");
        stagingJob1Nested.getParentFile().mkdirs();

        File stagingJob2 = new File(tempTargetDir, "sales_2026.parquet._staging_job-102");

        Files.writeString(stagingJob1.toPath(), "PARTIAL_STAGING_DATA_1");
        Files.writeString(stagingJob1Nested.toPath(), "PARTIAL_STAGING_DATA_NESTED");
        Files.writeString(stagingJob2.toPath(), "PARTIAL_STAGING_DATA_2");

        assertTrue(stagingJob1.exists());
        assertTrue(stagingJob1Nested.exists());
        assertTrue(stagingJob2.exists());

        // Вызываем очистку для job-101
        int deleted = fsManager.cleanStagingFiles(tempTargetDir.getAbsolutePath(), "job-101");

        assertEquals(2, deleted, "Должно быть удалено ровно 2 staging-файла для job-101");
        assertFalse(stagingJob1.exists(), "Staging файл job-101 должен быть удален");
        assertFalse(stagingJob1Nested.exists(), "Вложенный staging файл job-101 должен быть удален");
        assertTrue(stagingJob2.exists(), "Staging файл job-102 должен остаться нетронутым");
        assertTrue(prodFile.exists(), "Боевой файл должен остаться нетронутым");
    }

    @Test
    @DisplayName("Очистка осиротевших staging-файлов по TTL (olderThanMillis)")
    void testCleanStagingFilesByTtl() throws IOException {
        File oldStaging = new File(tempTargetDir, "data_archive.orc._staging_crashed_worker");
        File freshStaging = new File(tempTargetDir, "data_archive.orc._staging_active_worker");

        Files.writeString(oldStaging.toPath(), "OLD_ABANDONED_STAGING");
        Files.writeString(freshStaging.toPath(), "FRESH_ACTIVE_STAGING");

        // Искусственно устанавливаем mtime старого файла на 2 часа назад
        long twoHoursAgo = System.currentTimeMillis() - (2 * 60 * 60 * 1000L);
        oldStaging.setLastModified(twoHoursAgo);

        // Запускаем очистку с TTL = 1 час (3600000 мс)
        int deleted = fsManager.cleanStagingFiles(tempTargetDir.getAbsolutePath(), 60 * 60 * 1000L);

        assertEquals(1, deleted, "Должен быть удален только старый staging файл");
        assertFalse(oldStaging.exists(), "Старый файл должен быть удален");
        assertTrue(freshStaging.exists(), "Свежий файл должен остаться");
    }

    @Test
    @DisplayName("Реактивная очистка при обрыве связи (onError) в transferFile")
    void testReactiveCleanupOnStreamError() throws Exception {
        String targetPath = new File(tempTargetDir, "stream_break.parquet").getAbsolutePath();
        String stagingPath = targetPath + "._staging_job-network-fail";

        AtomicBoolean errorReceived = new AtomicBoolean(false);

        StreamObserver<TransferFileRequest> requestObserver = service.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {}

            @Override
            public void onError(Throwable t) {
                errorReceived.set(true);
            }

            @Override
            public void onCompleted() {}
        });

        // 1. Отправляем метаданные — файл staging создается
        requestObserver.onNext(TransferFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder()
                        .setJobId("job-network-fail")
                        .setSourcePath("/src/stream_break.parquet")
                        .setTargetPath(targetPath)
                        .setTotalBytes(1000)
                        .build())
                .build());

        // 2. Отправляем первый чанк данных
        requestObserver.onNext(TransferFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder()
                        .setJobId("job-network-fail")
                        .setOffset(0)
                        .setData(ByteString.copyFromUtf8("PARTIAL_DATA_CHUNK_1"))
                        .build())
                .build());

        File stagingFile = new File(stagingPath);
        assertTrue(stagingFile.exists(), "Staging файл должен существовать во время передачи");

        // 3. Имитируем обрыв сетевого соединения / падение клиента
        requestObserver.onError(new IOException("Connection reset by peer (WAN link broken)"));

        // Staging файл должен быть немедленно удален реактивным методом cleanup()
        assertFalse(stagingFile.exists(), "Staging файл должен быть удален реактивно при onError");
        assertFalse(new File(targetPath).exists(), "Целевой файл не должен быть создан");
    }

    @Test
    @DisplayName("Реактивная очистка незавершенных staging файлов при обрыве TAR-стрима")
    void testTarStreamReactiveCleanupOnError() throws Exception {
        String baseTarget = new File(tempTargetDir, "tar_fail_test").getAbsolutePath();
        String taskId = "task-tar-fail-1";

        StreamObserver<TarStreamRequest> requestObserver = service.transferTarStream(new StreamObserver<>() {
            @Override
            public void onNext(TarStreamResponse value) {}

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {}
        });

        // Отправляем метаданные бандла
        requestObserver.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId(taskId)
                        .setJobId("job-tar-fail")
                        .setBaseTargetPath(baseTarget)
                        .setTotalFiles(2)
                        .setTotalBytes(500)
                        .build())
                .build());

        // Формируем первый файл в tar-архиве
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (TarArchiveOutputStream tarOut = new TarArchiveOutputStream(baos)) {
            TarArchiveEntry entry = new TarArchiveEntry("file1.json");
            byte[] data = "{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8);
            entry.setSize(data.length);
            tarOut.putArchiveEntry(entry);
            tarOut.write(data);
            tarOut.closeArchiveEntry();
            tarOut.flush();
        }

        // Отправляем чанк
        requestObserver.onNext(TarStreamRequest.newBuilder()
                .setChunk(TarStreamChunk.newBuilder()
                        .setTaskId(taskId)
                        .setOffset(0)
                        .setData(ByteString.copyFrom(baos.toByteArray()))
                        .build())
                .build());

        // Имитируем аварийный сбой стрима
        requestObserver.onError(new IOException("gRPC transport error"));

        // Проверяем, что никаких ._staging_ файлов не осталось в baseTarget
        File baseDir = new File(baseTarget);
        if (baseDir.exists()) {
            File[] files = baseDir.listFiles();
            if (files != null) {
                for (File f : files) {
                    assertFalse(f.getName().contains("._staging_"), "Staging файлов не должно остаться: " + f.getName());
                }
            }
        }
    }
}
