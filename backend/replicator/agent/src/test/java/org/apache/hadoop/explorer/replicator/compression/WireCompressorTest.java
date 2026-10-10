package org.apache.hadoop.explorer.replicator.compression;

import org.apache.hadoop.explorer.replicator.generated.CompressionCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class WireCompressorTest {

    @Test
    @DisplayName("Zstandard: успешное сжатие текстовых логов/JSON и восстановление")
    void testZstdCompressAndDecompress() {
        String sampleJson = "{\"log_id\":\"abc-123\",\"level\":\"INFO\",\"message\":\"Replicating block 485923 to cluster DC2\",\"timestamp\":1690000000000}".repeat(50);
        byte[] original = sampleJson.getBytes(StandardCharsets.UTF_8);

        WireCompressor.CompressedPayload payload = WireCompressor.compress(
                original, 0, original.length, CompressionCodec.COMPRESSION_ZSTD, 3
        );

        assertEquals(CompressionCodec.COMPRESSION_ZSTD, payload.appliedCodec());
        assertTrue(payload.bytes().length < original.length / 2,
                "Zstd должен сжать повторяющийся JSON более чем на 50% (исходный: " + original.length + ", сжатый: " + payload.bytes().length + ")");
        assertEquals(original.length, payload.uncompressedSize());

        byte[] restored = WireCompressor.decompress(payload.bytes(), payload.appliedCodec(), payload.uncompressedSize());
        assertArrayEquals(original, restored);
    }

    @Test
    @DisplayName("LZ4: быстрое сжатие и восстановление")
    void testLz4CompressAndDecompress() {
        String logData = "2026-10-10 10:00:00 [Worker-1] INFO Replication batch processed successfully bytes=1048576\n".repeat(60);
        byte[] original = logData.getBytes(StandardCharsets.UTF_8);

        WireCompressor.CompressedPayload payload = WireCompressor.compress(
                original, 0, original.length, CompressionCodec.COMPRESSION_LZ4, 0
        );

        assertEquals(CompressionCodec.COMPRESSION_LZ4, payload.appliedCodec());
        assertTrue(payload.bytes().length < original.length / 2,
                "LZ4 должен сжать текстовый лог более чем на 50%");
        assertEquals(original.length, payload.uncompressedSize());

        byte[] restored = WireCompressor.decompress(payload.bytes(), payload.appliedCodec(), payload.uncompressedSize());
        assertArrayEquals(original, restored);
    }

    @Test
    @DisplayName("COMPRESSION_NONE: прозрачный пропуск данных")
    void testNoneCodec() {
        byte[] original = "Simple uncompressed string payload".getBytes(StandardCharsets.UTF_8);

        WireCompressor.CompressedPayload payload = WireCompressor.compress(
                original, 0, original.length, CompressionCodec.COMPRESSION_NONE, 0
        );

        assertEquals(CompressionCodec.COMPRESSION_NONE, payload.appliedCodec());
        assertArrayEquals(original, payload.bytes());

        byte[] restored = WireCompressor.decompress(payload.bytes(), CompressionCodec.COMPRESSION_NONE, payload.uncompressedSize());
        assertArrayEquals(original, restored);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/warehouse/tables/orders.parquet",
            "/data/events/year=2026/month=10/data.ORC",
            "hdfs://dc1:8020/logs/2026-10-10.snappy",
            "/backup/archive.tar.gz",
            "/warehouse/customer.gz",
            "/tmp/dump.lz4",
            "/hdfs/data.zst",
            "/warehouse/analytics.avro"
    })
    @DisplayName("Автоматическое определение уже сжатых форматов (Parquet, ORC, Snappy, Gz и др.)")
    void testPrecompressedFileDetection(String path) {
        assertTrue(WireCompressor.isPrecompressedPath(path), "Файл должен определяться как уже сжатый: " + path);
        assertEquals(
                CompressionCodec.COMPRESSION_NONE,
                WireCompressor.resolveCodecForPath(path, CompressionCodec.COMPRESSION_ZSTD),
                "Для уже сжатого файла кодек должен автоматически сбрасываться в NONE"
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/logs/app.log",
            "/warehouse/external/sales.csv",
            "/data/json/events.json",
            "/dumps/database_backup.sql",
            "/text/readme.txt"
    })
    @DisplayName("Несжатые текстовые форматы сохраняют запрашиваемый кодек сжатия")
    void testUncompressedFileFormatsRetainCodec(String path) {
        assertFalse(WireCompressor.isPrecompressedPath(path));
        assertEquals(
                CompressionCodec.COMPRESSION_ZSTD,
                WireCompressor.resolveCodecForPath(path, CompressionCodec.COMPRESSION_ZSTD)
        );
        assertEquals(
                CompressionCodec.COMPRESSION_LZ4,
                WireCompressor.resolveCodecForPath(path, CompressionCodec.COMPRESSION_LZ4)
        );
    }

    @Test
    @DisplayName("Адаптивный fallback в COMPRESSION_NONE при несжимаемых случайных данных")
    void testIncompressibleDataFallsBackToNone() {
        byte[] randomBytes = new byte[1024];
        new Random(42).nextBytes(randomBytes);

        // Для случайных псевдошумовых данных сжатие LZ4 не уменьшит размер
        WireCompressor.CompressedPayload payload = WireCompressor.compress(
                randomBytes, 0, randomBytes.length, CompressionCodec.COMPRESSION_LZ4, 0
        );

        assertEquals(CompressionCodec.COMPRESSION_NONE, payload.appliedCodec(),
                "Если сжатие не дало профита, кодек должен откатиться в COMPRESSION_NONE");
        assertArrayEquals(randomBytes, payload.bytes());
    }

    @Test
    @DisplayName("Парсинг названий кодеков")
    void testParseCodec() {
        assertEquals(CompressionCodec.COMPRESSION_ZSTD, WireCompressor.parseCodec("zstd"));
        assertEquals(CompressionCodec.COMPRESSION_ZSTD, WireCompressor.parseCodec("ZSTANDARD"));
        assertEquals(CompressionCodec.COMPRESSION_LZ4, WireCompressor.parseCodec("lz4"));
        assertEquals(CompressionCodec.COMPRESSION_LZ4, WireCompressor.parseCodec("LZ4"));
        assertEquals(CompressionCodec.COMPRESSION_NONE, WireCompressor.parseCodec("none"));
        assertEquals(CompressionCodec.COMPRESSION_NONE, WireCompressor.parseCodec("off"));
        assertEquals(CompressionCodec.COMPRESSION_ZSTD, WireCompressor.parseCodec(null));
    }
}
