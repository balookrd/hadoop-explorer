package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Высокопроизводительный Zero-Copy транскодер Wire-форматов Schema Registry.
 * <p>
 * Преобразует служебные заголовки сообщений Kafka между независимыми инстансами Schema Registry
 * (Confluent <-> Confluent, HWX <-> HWX, HWX <-> Confluent) без дорогостоящей десериализации Avro payload.
 */
public class ZeroCopyWireTranscoder {

    private static final Logger log = LoggerFactory.getLogger(ZeroCopyWireTranscoder.class);

    private static final byte CONFLUENT_MAGIC = 0x00;
    private static final byte HWX_PROTOCOL_V2 = 0x02;

    private static final int CONFLUENT_HDR_LEN = 5; // 1 байт Magic (0x00) + 4 байта Int Schema ID
    private static final int HWX_HDR_LEN = 9;       // 1 байт Protocol (0x02) + 8 байт Long Version ID

    private final SchemaIdResolver schemaResolver;

    public ZeroCopyWireTranscoder(SchemaIdResolver schemaResolver) {
        this.schemaResolver = Objects.requireNonNull(schemaResolver, "schemaResolver must not be null");
    }

    /**
     * Выполняет транскодинг заголовка полезной нагрузки в целевой формат.
     *
     * @param topic        имя топика Kafka
     * @param payload      исходный массив байт (может быть null для Tombstone)
     * @param sourceFormat формат исходной записи
     * @param targetFormat требуемый формат целевого кластера
     * @return преобразованный байтовый массив или исходный, если изменения не требуются
     */
    public byte[] transcode(String topic, byte[] payload, PayloadFormat sourceFormat, PayloadFormat targetFormat) {
        if (payload == null) {
            return null; // Tombstone-запись удалена, транскодинг не требуется
        }

        // Если один из форматов Passthrough — отдаем байты без модификации
        if (sourceFormat == PayloadFormat.PAYLOAD_PASSTHROUGH || targetFormat == PayloadFormat.PAYLOAD_PASSTHROUGH) {
            return payload;
        }

        // 1. Confluent -> Confluent (разные Schema ID в независимых кластерах)
        if (sourceFormat == PayloadFormat.PAYLOAD_CONFLUENT_AVRO && targetFormat == PayloadFormat.PAYLOAD_CONFLUENT_AVRO) {
            return transcodeConfluentToConfluent(topic, payload);
        }

        // 2. Hortonworks -> Confluent
        if (sourceFormat == PayloadFormat.PAYLOAD_HWX_AVRO && targetFormat == PayloadFormat.PAYLOAD_CONFLUENT_AVRO) {
            return transcodeHwxToConfluent(topic, payload);
        }

        // 3. Confluent -> Hortonworks
        if (sourceFormat == PayloadFormat.PAYLOAD_CONFLUENT_AVRO && targetFormat == PayloadFormat.PAYLOAD_HWX_AVRO) {
            return transcodeConfluentToHwx(topic, payload);
        }

        // 4. Hortonworks -> Hortonworks (разные Version ID в независимых кластерах)
        if (sourceFormat == PayloadFormat.PAYLOAD_HWX_AVRO && targetFormat == PayloadFormat.PAYLOAD_HWX_AVRO) {
            return transcodeHwxToHwx(topic, payload);
        }

        return payload;
    }

    private byte[] transcodeConfluentToConfluent(String topic, byte[] payload) {
        if (payload == null || payload.length < CONFLUENT_HDR_LEN) {
            return payload;
        }

        int sourceId = ByteBuffer.wrap(payload, 1, 4).getInt();
        int targetId = schemaResolver.resolveConfluentToConfluent(topic, sourceId);

        if (sourceId == targetId) {
            return payload; // ID полностью совпали, замена не требуется
        }

        // Zero-copy: in-place замена ID схемы в бинарном заголовке без дублирования байтов в куче
        payload[1] = (byte) (targetId >>> 24);
        payload[2] = (byte) (targetId >>> 16);
        payload[3] = (byte) (targetId >>> 8);
        payload[4] = (byte) targetId;
        return payload;
    }

    private byte[] transcodeHwxToHwx(String topic, byte[] payload) {
        if (payload == null || payload.length < HWX_HDR_LEN) {
            return payload;
        }

        long sourceId = ByteBuffer.wrap(payload, 1, 8).getLong();
        long targetId = schemaResolver.resolveHwxToHwx(topic, sourceId);

        if (sourceId == targetId) {
            return payload;
        }

        // Zero-copy: in-place замена 8-байтного Long ID без клонирования payload
        ByteBuffer.wrap(payload, 1, 8).putLong(targetId);
        return payload;
    }

    private byte[] transcodeHwxToConfluent(String topic, byte[] payload) {
        if (payload.length < HWX_HDR_LEN) {
            return payload;
        }

        long hwxVersionId = ByteBuffer.wrap(payload, 1, 8).getLong();
        int confluentId = schemaResolver.resolveHwxToConfluent(topic, hwxVersionId);

        int avroBodyLen = payload.length - HWX_HDR_LEN;
        byte[] target = new byte[CONFLUENT_HDR_LEN + avroBodyLen];

        target[0] = CONFLUENT_MAGIC;
        target[1] = (byte) (confluentId >>> 24);
        target[2] = (byte) (confluentId >>> 16);
        target[3] = (byte) (confluentId >>> 8);
        target[4] = (byte) confluentId;

        System.arraycopy(payload, HWX_HDR_LEN, target, CONFLUENT_HDR_LEN, avroBodyLen);
        return target;
    }

    private byte[] transcodeConfluentToHwx(String topic, byte[] payload) {
        if (payload.length < CONFLUENT_HDR_LEN) {
            return payload;
        }

        int confluentId = ByteBuffer.wrap(payload, 1, 4).getInt();
        long hwxVersionId = schemaResolver.resolveConfluentToHwx(topic, confluentId);

        int avroBodyLen = payload.length - CONFLUENT_HDR_LEN;
        byte[] target = new byte[HWX_HDR_LEN + avroBodyLen];

        target[0] = HWX_PROTOCOL_V2;
        ByteBuffer.wrap(target, 1, 8).putLong(hwxVersionId);

        System.arraycopy(payload, CONFLUENT_HDR_LEN, target, HWX_HDR_LEN, avroBodyLen);
        return target;
    }
}
