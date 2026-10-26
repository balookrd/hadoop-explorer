package org.apache.hadoop.explorer.replicator.compression;

import org.apache.hadoop.explorer.replicator.generated.CompressionCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Предел размера распакованного чанка.
 *
 * <p>Защита от decompression bomb стояла на 64 МБ, при том что чанк провода — 64 КБ
 * (`CHUNK_SIZE_BYTES`). Отправитель объявлял в заголовке чанка размер в тысячу реальных, и
 * приемник выделял под него массив: десяток параллельных потоков съедал heap, хотя формально
 * «защита» была. Предел должен следовать из договоренного размера чанка, а не стоять на три
 * порядка выше.
 */
@DisplayName("SEC: предел распакованного чанка следует из размера чанка")
class ChunkSizeGuardTest {

    private static final byte[] DUMMY_COMPRESSED = {1, 2, 3, 4};

    @AfterEach
    void resetLimit() {
        WireCompressor.configureMaxUncompressedChunkSize(0);
    }

    @Test
    @DisplayName("По умолчанию чанк в 2 МБ отвергается")
    void defaultLimitShouldRejectMegabyteChunks() {
        assertEquals(1024 * 1024, WireCompressor.getMaxUncompressedChunkSize(),
                "По умолчанию предел — 1 МБ, с запасом к реальным 64 КБ");

        SecurityException zstd = assertThrows(SecurityException.class, () ->
                WireCompressor.decompress(DUMMY_COMPRESSED, CompressionCodec.COMPRESSION_ZSTD, 2 * 1024 * 1024));
        assertTrue(zstd.getMessage().contains("2097152"), "В отказе должен быть заявленный размер");

        assertThrows(SecurityException.class, () ->
                WireCompressor.decompress(DUMMY_COMPRESSED, CompressionCodec.COMPRESSION_LZ4, 2 * 1024 * 1024));
    }

    @Test
    @DisplayName("Предел растет вместе с настроенным размером чанка")
    void limitShouldFollowConfiguredChunkSize() {
        WireCompressor.configureMaxUncompressedChunkSize(4 * 1024 * 1024);

        assertEquals(16 * 1024 * 1024, WireCompressor.getMaxUncompressedChunkSize(),
                "Запас — четыре размера чанка: заголовки TAR и выравнивание не должны ломать передачу");
        assertThrows(SecurityException.class, () ->
                WireCompressor.decompress(DUMMY_COMPRESSED, CompressionCodec.COMPRESSION_ZSTD, 17 * 1024 * 1024),
                "Сверх запаса чанк все равно отвергается");
    }

    @Test
    @DisplayName("Маленький настроенный чанк не опускает предел ниже минимума")
    void limitShouldNotFallBelowFloor() {
        WireCompressor.configureMaxUncompressedChunkSize(8 * 1024);

        assertEquals(1024 * 1024, WireCompressor.getMaxUncompressedChunkSize(),
                "Нижняя граница предела — 1 МБ");
    }

    @Test
    @DisplayName("Предел ограничен сверху независимо от настройки")
    void limitShouldBeCappedFromAbove() {
        WireCompressor.configureMaxUncompressedChunkSize(512 * 1024 * 1024);

        assertEquals(64 * 1024 * 1024, WireCompressor.getMaxUncompressedChunkSize(),
                "Верхняя граница предела — 64 МБ: иначе защита снова перестает защищать");
    }

    @Test
    @DisplayName("Объявленный размер больше фактического отвергается (zstd)")
    void zstdShouldRejectDeclaredSizeLargerThanActual() {
        byte[] payload = new byte[16 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 97);
        }
        WireCompressor.CompressedPayload compressed =
                WireCompressor.compress(payload, 0, payload.length, CompressionCodec.COMPRESSION_ZSTD, 3);

        // Отправитель объявил вдвое больше, чем в кадре: без сверки приемник вернул бы массив
        // заявленного размера с хвостом нулей и записал бы его как данные
        RuntimeException error = assertThrows(RuntimeException.class, () ->
                WireCompressor.decompress(compressed.bytes(), compressed.appliedCodec(), payload.length * 2));
        assertTrue(error.getMessage() != null && error.getMessage().contains(String.valueOf(payload.length)),
                "В отказе должен быть фактический размер: " + error.getMessage());
    }

    @Test
    @DisplayName("Чанк реального размера проходит без помех")
    void realChunkShouldPass() {
        byte[] payload = new byte[64 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }

        WireCompressor.CompressedPayload compressed =
                WireCompressor.compress(payload, 0, payload.length, CompressionCodec.COMPRESSION_ZSTD, 3);
        byte[] restored = WireCompressor.decompress(compressed.bytes(), compressed.appliedCodec(), payload.length);

        assertArrayEquals(payload, restored, "Чанк провода должен распаковываться без ограничений");
    }
}
