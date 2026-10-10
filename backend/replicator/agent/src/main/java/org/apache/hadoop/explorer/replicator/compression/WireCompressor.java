package org.apache.hadoop.explorer.replicator.compression;

import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.hadoop.explorer.replicator.generated.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;

/**
 * Сервис прозрачного потокового сжатия и декомпрессии чанков (Wire Compression)
 * на базе алгоритмов Zstandard (zstd) и LZ4 в gRPC-канале репликатора.
 * <p>
 * Обеспечивает экономию 40-80% пропускной способности WAN каналов межЦОДного обмена,
 * автоматически отключая сжатие для файлов, которые уже сжаты на уровне формата
 * (Parquet, ORC, Snappy, Gzip, Zstd, LZ4, Zip, Bzip2, Avro).
 */
public class WireCompressor {

    private static final Logger logger = LoggerFactory.getLogger(WireCompressor.class);

    private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
    public static final int DEFAULT_ZSTD_LEVEL = 3;

    /**
     * Список расширений файлов, которые уже сжаты и не требуют повторной компрессии в канале.
     */
    private static final Set<String> PRECOMPRESSED_EXTENSIONS = Set.of(
            ".parquet", ".parq",
            ".orc",
            ".snappy", ".sz",
            ".gz", ".gzip", ".tgz",
            ".zst", ".zstd",
            ".lz4",
            ".bz2", ".bzip2",
            ".zip", ".jar", ".war",
            ".xz",
            ".7z",
            ".avro"
    );

    /**
     * Результат сжатия порции данных.
     *
     * @param bytes            бинарные данные (сжатые либо исходные, если сжатие не дало эффекта)
     * @param appliedCodec     фактически примененный кодек (COMPRESSION_NONE если сжатие не дало профита)
     * @param uncompressedSize размер исходных несжатых данных
     */
    public record CompressedPayload(byte[] bytes, CompressionCodec appliedCodec, int uncompressedSize) {
    }

    /**
     * Проверяет, является ли файл уже сжатым форматом данных.
     *
     * @param path путь к файлу
     * @return true, если файл имеет известное расширение сжатого формата
     */
    public static boolean isPrecompressedPath(String path) {
        if (path == null || path.isBlank()) {
            return false;
        }
        String lower = path.toLowerCase(Locale.ROOT);
        for (String ext : PRECOMPRESSED_EXTENSIONS) {
            if (lower.endsWith(ext)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Определяет эффективный кодек сжатия для конкретного файла:
     * если файл уже сжат (Parquet, ORC, Gz и т.д.), возвращает COMPRESSION_NONE.
     *
     * @param path           путь к файлу
     * @param configuredCodec кодек, заданный в конфигурации
     * @return эффективный кодек для передачи файла
     */
    public static CompressionCodec resolveCodecForPath(String path, CompressionCodec configuredCodec) {
        if (configuredCodec == null || configuredCodec == CompressionCodec.COMPRESSION_NONE || configuredCodec == CompressionCodec.UNRECOGNIZED) {
            return CompressionCodec.COMPRESSION_NONE;
        }
        if (isPrecompressedPath(path)) {
            logger.debug("Сжатие на лету отключено для файла с предварительно сжатым форматом: '{}'", path);
            return CompressionCodec.COMPRESSION_NONE;
        }
        return configuredCodec;
    }

    /**
     * Преобразует строковое название алгоритма сжатия в enum protobuf.
     *
     * @param name название ("zstd", "lz4", "none" и др.)
     * @return protobuf CompressionCodec
     */
    public static CompressionCodec parseCodec(String name) {
        if (name == null || name.isBlank()) {
            return CompressionCodec.COMPRESSION_ZSTD;
        }
        String clean = name.trim().toLowerCase(Locale.ROOT);
        return switch (clean) {
            case "zstd", "zstandard" -> CompressionCodec.COMPRESSION_ZSTD;
            case "lz4" -> CompressionCodec.COMPRESSION_LZ4;
            case "none", "off", "disable", "uncompressed" -> CompressionCodec.COMPRESSION_NONE;
            default -> {
                logger.warn("Неизвестный кодек сжатия '{}', используется Zstandard по умолчанию", name);
                yield CompressionCodec.COMPRESSION_ZSTD;
            }
        };
    }

    /**
     * Сжимает порцию данных с заданным кодеком.
     * Если сжатие не дало сокращения объема, возвращается исходный массив с кодеком COMPRESSION_NONE.
     *
     * @param data      буфер с исходными данными
     * @param offset    начальное смещение
     * @param length    длина порции данных
     * @param codec     запрашиваемый кодек сжатия
     * @param zstdLevel уровень сжатия Zstd (от 1 до 22, по умолчанию 3)
     * @return CompressedPayload с результатом
     */
    public static CompressedPayload compress(byte[] data, int offset, int length, CompressionCodec codec, int zstdLevel) {
        if (codec == null || codec == CompressionCodec.COMPRESSION_NONE || codec == CompressionCodec.UNRECOGNIZED || length <= 0) {
            byte[] raw = (offset == 0 && length == data.length) ? data : Arrays.copyOfRange(data, offset, offset + length);
            return new CompressedPayload(raw, CompressionCodec.COMPRESSION_NONE, length);
        }

        try {
            if (codec == CompressionCodec.COMPRESSION_ZSTD) {
                byte[] src = (offset == 0 && length == data.length) ? data : Arrays.copyOfRange(data, offset, offset + length);
                int level = (zstdLevel > 0) ? zstdLevel : DEFAULT_ZSTD_LEVEL;
                byte[] compressed = Zstd.compress(src, level);

                // Если сжатый размер больше или равен исходному - передаем несжатым
                if (compressed.length >= length) {
                    return new CompressedPayload(src, CompressionCodec.COMPRESSION_NONE, length);
                }
                return new CompressedPayload(compressed, CompressionCodec.COMPRESSION_ZSTD, length);
            } else if (codec == CompressionCodec.COMPRESSION_LZ4) {
                LZ4Compressor compressor = LZ4_FACTORY.fastCompressor();
                int maxCompressedLength = compressor.maxCompressedLength(length);
                byte[] temp = new byte[maxCompressedLength];
                int compressedLength = compressor.compress(data, offset, length, temp, 0, maxCompressedLength);

                if (compressedLength >= length) {
                    byte[] src = (offset == 0 && length == data.length) ? data : Arrays.copyOfRange(data, offset, offset + length);
                    return new CompressedPayload(src, CompressionCodec.COMPRESSION_NONE, length);
                }
                byte[] compressed = Arrays.copyOf(temp, compressedLength);
                return new CompressedPayload(compressed, CompressionCodec.COMPRESSION_LZ4, length);
            }
        } catch (Exception e) {
            logger.warn("Ошибка при сжатии данных кодеком {}: {}. Отправка в несжатом виде", codec, e.getMessage());
            byte[] raw = (offset == 0 && length == data.length) ? data : Arrays.copyOfRange(data, offset, offset + length);
            return new CompressedPayload(raw, CompressionCodec.COMPRESSION_NONE, length);
        }

        byte[] raw = (offset == 0 && length == data.length) ? data : Arrays.copyOfRange(data, offset, offset + length);
        return new CompressedPayload(raw, CompressionCodec.COMPRESSION_NONE, length);
    }

    /**
     * Распаковывает полученный чанк данных.
     *
     * @param compressedData   сжатые (или несжатые) бинарные данные
     * @param codec            кодек сжатия
     * @param uncompressedSize ожидаемый размер несжатых данных
     * @return распакованный байтовый массив
     */
    public static byte[] decompress(byte[] compressedData, CompressionCodec codec, int uncompressedSize) {
        if (codec == null || codec == CompressionCodec.COMPRESSION_NONE || codec == CompressionCodec.UNRECOGNIZED) {
            return compressedData;
        }
        if (uncompressedSize <= 0) {
            return compressedData;
        }

        try {
            if (codec == CompressionCodec.COMPRESSION_ZSTD) {
                return Zstd.decompress(compressedData, uncompressedSize);
            } else if (codec == CompressionCodec.COMPRESSION_LZ4) {
                byte[] restored = new byte[uncompressedSize];
                LZ4SafeDecompressor decompressor = LZ4_FACTORY.safeDecompressor();
                int decompressedLen = decompressor.decompress(compressedData, 0, compressedData.length, restored, 0);
                if (decompressedLen != uncompressedSize) {
                    throw new IllegalStateException("LZ4 декомпрессия вернула " + decompressedLen + " байт вместо ожидаемых " + uncompressedSize);
                }
                return restored;
            }
        } catch (Exception e) {
            throw new RuntimeException("Не удалось распаковать чанк данных кодеком " + codec + ": " + e.getMessage(), e);
        }

        return compressedData;
    }
}
