package org.apache.hadoop.explorer.replicator.receiver;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.TarStreamChunk;
import org.apache.hadoop.explorer.replicator.generated.TarStreamMetadata;
import org.apache.hadoop.explorer.replicator.generated.TarStreamRequest;
import org.apache.hadoop.explorer.replicator.generated.TarStreamResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Поведение приемника при частичном сбое TAR-бандла.
 *
 * <p>Ключевая гарантия (DATA-LOSS): откат незавершенного бандла обязан затрагивать только
 * staging-файлы. Файлы, уже закоммиченные через rename, принадлежат целевому кластеру —
 * их удаление уничтожает и новую, и прежнюю версию пользовательских данных, а повторная
 * передача бандла идемпотентна и всё равно перезапишет их.
 */
@DisplayName("Частичный сбой TAR-бандла: сохранность закоммиченных файлов")
public class TarBundlePartialFailureTest {

    private File tempTargetDir;
    private DataTransferServiceImpl service;

    @BeforeEach
    void setUp() throws IOException {
        tempTargetDir = Files.createTempDirectory("tar-bundle-rollback-test").toFile();
        HadoopFsManager fsManager = new HadoopFsManager("file:///", null, null);
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

    private static byte[] tarWith(Map<String, String> files) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (TarArchiveOutputStream tarOut = new TarArchiveOutputStream(baos)) {
            for (Map.Entry<String, String> file : files.entrySet()) {
                byte[] data = file.getValue().getBytes(StandardCharsets.UTF_8);
                TarArchiveEntry entry = new TarArchiveEntry(file.getKey());
                entry.setSize(data.length);
                tarOut.putArchiveEntry(entry);
                tarOut.write(data);
                tarOut.closeArchiveEntry();
            }
            tarOut.finish();
        }
        return baos.toByteArray();
    }

    private static boolean awaitFile(File file, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (file.isFile() && file.length() > 0) {
                return true;
            }
            Thread.sleep(25);
        }
        return false;
    }

    @Test
    @DisplayName("Чанки, пришедшие из разных короткоживущих потоков, собираются в один бандл")
    void testChunksFromDistinctShortLivedThreadsAreAssembled() throws Exception {
        String baseTarget = new File(tempTargetDir, "vthreads").getAbsolutePath();
        String taskId = "task-vthread-bundle-1";

        StreamObserver<TarStreamRequest> requestObserver = service.transferTarStream(new StreamObserver<>() {
            @Override public void onNext(TarStreamResponse value) {}
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        Map<String, String> files = new LinkedHashMap<>();
        files.put("events_part1.json", "{\"id\":1}");
        files.put("events_part2.json", "{\"id\":2}");
        byte[] tar = tarWith(files);
        // Первый чанк намеренно крошечный: распаковщик успевает его дочитать и остаться
        // ждать на пустом буфере, пока поток-писатель уже мертв.
        int split = 64;

        // gRPC-сервер приемника сконфигурирован с newVirtualThreadPerTaskExecutor():
        // каждая порция сообщений приходит в своем потоке, а предыдущий сразу умирает.
        // Пауза между чанками дает распаковщику опустошить буфер — именно в этот момент
        // PipedInputStream бросал "Write end dead".
        inSeparateThread(() -> requestObserver.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId(taskId)
                        .setJobId("job-vthread-bundle")
                        .setBaseTargetPath(baseTarget)
                        .setTotalFiles(2)
                        .setTotalBytes(tar.length)
                        .build())
                .build()));

        inSeparateThread(() -> requestObserver.onNext(TarStreamRequest.newBuilder()
                .setChunk(TarStreamChunk.newBuilder()
                        .setTaskId(taskId)
                        .setOffset(0)
                        .setData(ByteString.copyFrom(tar, 0, split))
                        .build())
                .build()));

        Thread.sleep(1_000);

        inSeparateThread(() -> requestObserver.onNext(TarStreamRequest.newBuilder()
                .setChunk(TarStreamChunk.newBuilder()
                        .setTaskId(taskId)
                        .setOffset(split)
                        .setData(ByteString.copyFrom(tar, split, tar.length - split))
                        .build())
                .build()));

        inSeparateThread(requestObserver::onCompleted);

        File first = new File(baseTarget, "events_part1.json");
        File second = new File(baseTarget, "events_part2.json");
        assertTrue(awaitFile(first, 5_000),
                "Первый файл бандла не закоммичен: сборка потока сломана при смене потока-писателя");
        assertTrue(awaitFile(second, 5_000), "Второй файл бандла не закоммичен");
        assertEquals("{\"id\":1}", Files.readString(first.toPath()));
        assertEquals("{\"id\":2}", Files.readString(second.toPath()));
    }

    /** Отправка из потока, который сразу после вызова завершается. */
    private static void inSeparateThread(Runnable action) throws InterruptedException {
        Thread sender = new Thread(action, "grpc-vthread-sim");
        sender.start();
        sender.join(5_000);
    }

    @Test
    @DisplayName("Бандл из трех файлов: сбой второго не отменяет первый и третий")
    void testFailedFileInBundleDoesNotDiscardOthers() throws Exception {
        String baseTarget = new File(tempTargetDir, "warehouse").getAbsolutePath();
        String taskId = "task-bundle-middle-failure";

        Map<String, String> files = new LinkedHashMap<>();
        files.put("events_part1.json", "{\"id\":1}");
        files.put("blocked/events_part2.json", "{\"id\":2}");
        files.put("events_part3.json", "{\"id\":3}");
        byte[] tar = tarWith(files);

        // Второй файл лежит в подкаталоге, имя которого на приемнике уже занято обычным
        // файлом: создать каталог невозможно, значит не запишется именно этот файл
        assertTrue(new File(baseTarget).mkdirs(), "Не удалось создать целевой каталог");
        Files.writeString(new File(baseTarget, "blocked").toPath(), "занято");

        java.util.concurrent.atomic.AtomicReference<TarStreamResponse> result =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
        StreamObserver<TarStreamRequest> requestObserver = service.transferTarStream(new StreamObserver<>() {
            @Override
            public void onNext(TarStreamResponse value) {
                result.set(value);
            }

            @Override
            public void onError(Throwable t) {
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        });

        requestObserver.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId(taskId)
                        .setJobId("job-bundle-middle-failure")
                        .setBaseTargetPath(baseTarget)
                        .setTotalFiles(files.size())
                        .setTotalBytes(tar.length)
                        .build())
                .build());
        requestObserver.onNext(TarStreamRequest.newBuilder()
                .setChunk(TarStreamChunk.newBuilder()
                        .setTaskId(taskId)
                        .setOffset(0)
                        .setData(ByteString.copyFrom(tar))
                        .build())
                .build());
        requestObserver.onCompleted();

        assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS),
                "Приемник не завершил обработку бандла");

        File first = new File(baseTarget, "events_part1.json");
        File third = new File(baseTarget, "events_part3.json");
        assertTrue(first.isFile(), "Первый файл обязан остаться: сбой соседа по бандлу его не касается");
        assertTrue(third.isFile(), "Третий файл обязан быть записан: обработка бандла не должна "
                + "останавливаться на первом же сбое");
        assertEquals("{\"id\":1}", Files.readString(first.toPath()));
        assertEquals("{\"id\":3}", Files.readString(third.toPath()));

        assertNotNull(result.get(), "Приемник не прислал результат по бандлу");
        assertEquals(1, result.get().getFilesList().stream().filter(r -> !r.getSuccess()).count(),
                "Сбойный файл обязан быть отмечен в результатах, иначе задача отчитается об успехе "
                        + "и событие считается примененным: " + result.get().getFilesList());
        assertTrue(result.get().getFilesList().stream()
                        .anyMatch(r -> !r.getSuccess() && r.getRelativePath().contains("events_part2")),
                "В результатах должен быть помечен именно второй файл: " + result.get().getFilesList());
    }

    @Test
    @DisplayName("Обрыв стрима не удаляет файлы бандла, уже закоммиченные на приемнике")
    void testStreamErrorKeepsAlreadyCommittedFiles() throws Exception {
        String baseTarget = new File(tempTargetDir, "warehouse").getAbsolutePath();
        String taskId = "task-partial-bundle-1";

        StreamObserver<TarStreamRequest> requestObserver = service.transferTarStream(new StreamObserver<>() {
            @Override
            public void onNext(TarStreamResponse value) {}

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {}
        });

        // Бандл объявлен на 3 файла — третий не дойдет из-за обрыва связи
        requestObserver.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId(taskId)
                        .setJobId("job-partial-bundle")
                        .setBaseTargetPath(baseTarget)
                        .setTotalFiles(3)
                        .setTotalBytes(120)
                        .build())
                .build());

        Map<String, String> delivered = new LinkedHashMap<>();
        delivered.put("sales_part1.json", "{\"id\":1,\"amount\":100}");
        delivered.put("sales_part2.json", "{\"id\":2,\"amount\":200}");

        requestObserver.onNext(TarStreamRequest.newBuilder()
                .setChunk(TarStreamChunk.newBuilder()
                        .setTaskId(taskId)
                        .setOffset(0)
                        .setData(ByteString.copyFrom(tarWith(delivered)))
                        .build())
                .build());

        File committed1 = new File(baseTarget, "sales_part1.json");
        File committed2 = new File(baseTarget, "sales_part2.json");
        assertTrue(awaitFile(committed1, 5_000), "Первый файл бандла должен быть закоммичен приемником");
        assertTrue(awaitFile(committed2, 5_000), "Второй файл бандла должен быть закоммичен приемником");

        // Обрыв связи до прихода третьего файла
        requestObserver.onError(new IOException("gRPC transport error"));
        Thread.sleep(500);

        assertTrue(committed1.isFile(),
                "Закоммиченный файл sales_part1.json удален при откате — это потеря данных приемника");
        assertTrue(committed2.isFile(),
                "Закоммиченный файл sales_part2.json удален при откате — это потеря данных приемника");
        assertEquals("{\"id\":1,\"amount\":100}", Files.readString(committed1.toPath()));
        assertEquals("{\"id\":2,\"amount\":200}", Files.readString(committed2.toPath()));

        File baseDir = new File(baseTarget);
        File[] leftovers = baseDir.listFiles((dir, name) -> name.contains("._staging_"));
        assertTrue(leftovers == null || leftovers.length == 0,
                "Staging-файлы обязаны быть вычищены при откате");
    }
}
