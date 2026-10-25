package org.apache.hadoop.explorer.replicator.receiver;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Приемник в продуктивной конфигурации транспорта: Netty и виртуальные потоки.
 *
 * <p>Все остальные тесты gRPC построены на {@code InProcessServerBuilder} с
 * {@code directExecutor()}: вызов выполняется в потоке вызывающего, сообщения приходят строго
 * подряд и никакой поток между ними не умирает. Продуктивная конфигурация
 * ({@code NettyServerBuilder.executor(newVirtualThreadPerTaskExecutor())}) ведет себя иначе —
 * каждая порция сообщений обслуживается своим потоком, а предыдущий сразу завершается. Именно
 * на этом разошлись тесты и продуктив: распаковщик бандла читал данные из {@code PipedInputStream},
 * и как только поток-писатель умирал, чтение падало с «Write end dead», причем только при
 * паузе между чанками — то есть ровно под сетевой задержкой WAN, за которой это и предназначено.
 *
 * <p>Тест идет через настоящий сокет, с паузами между чанками, и проверяет, что бандл доходит
 * целиком. Паузы здесь не замедление ради замедления: без них дефект не воспроизводится.
 */
@DisplayName("Приемник на Netty с виртуальными потоками: передача с паузами между чанками")
class NettyVirtualThreadReceiverTest {

    /** Пауза между чанками: распаковщик успевает опустошить буфер и остаться ждать. */
    private static final long CHUNK_GAP_MS = 400;

    private File tempDir;
    private Server server;
    private ManagedChannel channel;
    private ExecutorService serverExecutor;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("netty-vthread-receiver").toFile();
        HadoopFsManager fsManager = new HadoopFsManager("file:///", null, null);
        DataTransferServiceImpl service = new DataTransferServiceImpl(
                new File(tempDir, "staging").getAbsolutePath(), fsManager, null, null, 4);

        // Ровно та конфигурация, с которой приемник работает в продуктиве
        serverExecutor = Executors.newVirtualThreadPerTaskExecutor();
        server = NettyServerBuilder.forPort(0)
                .executor(serverExecutor)
                .addService(service)
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build()
                .start();
        channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
        if (serverExecutor != null) {
            serverExecutor.shutdownNow();
        }
        if (tempDir != null && tempDir.exists()) {
            Files.walk(tempDir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Бандл из трех файлов доходит целиком, несмотря на паузы между чанками")
    void testTarBundleSurvivesChunkGaps() throws Exception {
        Map<String, String> files = new LinkedHashMap<>();
        files.put("events_part1.json", "{\"id\":1,\"payload\":\"first\"}");
        files.put("events_part2.json", "{\"id\":2,\"payload\":\"second\"}");
        files.put("events_part3.json", "{\"id\":3,\"payload\":\"third\"}");
        byte[] tar = tarWith(files);

        String baseTarget = new File(tempDir, "warehouse").getAbsolutePath();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<TarStreamResponse> response = new AtomicReference<>();

        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);
        StreamObserver<TarStreamRequest> request = stub.transferTarStream(new StreamObserver<>() {
            @Override
            public void onNext(TarStreamResponse value) {
                response.set(value);
            }

            @Override
            public void onError(Throwable t) {
                failure.set(t);
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        });

        request.onNext(TarStreamRequest.newBuilder()
                .setMetadata(TarStreamMetadata.newBuilder()
                        .setTaskId("task-netty-vthread-bundle")
                        .setJobId("job-netty-vthread")
                        .setBaseTargetPath(baseTarget)
                        .setTotalFiles(files.size())
                        .setTotalBytes(tar.length)
                        .build())
                .build());

        // Первый чанк намеренно крошечный: распаковщик дочитывает его и остается ждать
        // на пустом буфере, пока поток, доставивший чанк, уже завершен
        int[] splits = {64, 512, tar.length};
        int offset = 0;
        for (int split : splits) {
            int end = Math.min(split, tar.length);
            if (end <= offset) {
                continue;
            }
            Thread.sleep(CHUNK_GAP_MS);
            request.onNext(TarStreamRequest.newBuilder()
                    .setChunk(TarStreamChunk.newBuilder()
                            .setTaskId("task-netty-vthread-bundle")
                            .setOffset(offset)
                            .setData(ByteString.copyFrom(tar, offset, end - offset))
                            .build())
                    .build());
            offset = end;
        }
        request.onCompleted();

        assertTrue(done.await(60, TimeUnit.SECONDS), "Приемник не завершил передачу бандла");
        assertNull(failure.get(), () -> "Передача упала: " + failure.get());
        assertNotNull(response.get(), "Приемник не прислал результат");
        assertTrue(response.get().getSuccess(),
                "Приемник сообщил об ошибке: " + response.get().getMessage());

        for (Map.Entry<String, String> file : files.entrySet()) {
            File landed = new File(baseTarget, file.getKey());
            assertTrue(landed.isFile(), "Файл бандла не доехал: " + file.getKey());
            assertEquals(file.getValue(), Files.readString(landed.toPath(), StandardCharsets.UTF_8),
                    "Содержимое файла " + file.getKey() + " искажено");
        }
        assertEquals(files.size(), response.get().getFilesCommitted(),
                "Приемник закоммитил не все файлы бандла");
    }

    @Test
    @DisplayName("Одиночный файл доезжает с верной контрольной суммой при паузах между чанками")
    void testSingleFileSurvivesChunkGaps() throws Exception {
        byte[] payload = new byte[256 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        String checksum = sha256(payload);
        String targetPath = new File(tempDir, "warehouse/large_file.bin").getAbsolutePath();

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<TransferFileResponse> response = new AtomicReference<>();

        DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);
        StreamObserver<TransferFileRequest> request = stub.transferFile(new StreamObserver<>() {
            @Override
            public void onNext(TransferFileResponse value) {
                response.set(value);
            }

            @Override
            public void onError(Throwable t) {
                failure.set(t);
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        });

        request.onNext(TransferFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder()
                        .setTaskId("task-netty-vthread-file")
                        .setJobId("job-netty-vthread")
                        .setTargetPath(targetPath)
                        .setTotalBytes(payload.length)
                        .setChecksum(checksum)
                        .build())
                .build());

        int chunkSize = payload.length / 4;
        for (int offset = 0; offset < payload.length; offset += chunkSize) {
            int length = Math.min(chunkSize, payload.length - offset);
            Thread.sleep(CHUNK_GAP_MS);
            boolean last = offset + length >= payload.length;
            request.onNext(TransferFileRequest.newBuilder()
                    .setChunk(FileChunk.newBuilder()
                            .setJobId("job-netty-vthread")
                            .setOffset(offset)
                            .setData(ByteString.copyFrom(payload, offset, length))
                            .setIsLastChunk(last)
                            .build())
                    .build());
        }
        request.onCompleted();

        assertTrue(done.await(60, TimeUnit.SECONDS), "Приемник не завершил передачу файла");
        assertNull(failure.get(), () -> "Передача упала: " + failure.get());
        assertNotNull(response.get(), "Приемник не прислал результат");
        assertTrue(response.get().getSuccess(),
                "Приемник сообщил об ошибке: " + response.get().getMessage());

        File landed = new File(targetPath);
        assertTrue(landed.isFile(), "Файл не доехал до целевого пути");
        assertEquals(checksum, sha256(Files.readAllBytes(landed.toPath())),
                "Контрольная сумма доехавшего файла не совпала");
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

    private static String sha256(byte[] data) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        StringBuilder hex = new StringBuilder();
        for (byte b : digest.digest(data)) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}
