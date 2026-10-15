package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Классификатор формата полезной нагрузки Kafka-сообщений.
 * <p>
 * Позволяет разделять топики со Schema Registry (Confluent/Hortonworks) и обычные топики
 * (JSON, Plain Text, Protobuf без реестра, Raw binary), а также корректно обрабатывать
 * Tombstone-записи (null payload) без обращения к реестрам схем.
 */
public class PayloadFormatClassifier {

    private static final Logger log = LoggerFactory.getLogger(PayloadFormatClassifier.class);

    public static final byte CONFLUENT_MAGIC_BYTE = 0x00;
    public static final byte HWX_PROTOCOL_V2 = 0x02;
    public static final byte HWX_PROTOCOL_V3 = 0x03;

    public static final int CONFLUENT_MIN_HEADER_SIZE = 5; // 1 magic + 4 byte ID
    public static final int HWX_MIN_HEADER_SIZE = 9;       // 1 protocol + 8 byte ID

    /**
     * Определяет реальный формат полезной нагрузки с учетом конфигурации топика и сигнатуры байтов.
     *
     * @param payload          байтовый массив полезной нагрузки (может быть null для tombstones)
     * @param configuredFormat формат, заданный пользователем в конфигурации топика
     * @return вычисленный формат записи
     */
    public PayloadFormat classify(byte[] payload, PayloadFormat configuredFormat) {
        if (payload == null || payload.length == 0) {
            return PayloadFormat.PAYLOAD_PASSTHROUGH;
        }

        // Если для топика жестко задан прямой проброс (JSON, Plain Text, Protobuf)
        if (configuredFormat == PayloadFormat.PAYLOAD_PASSTHROUGH) {
            return PayloadFormat.PAYLOAD_PASSTHROUGH;
        }

        // Если данных недостаточно для заголовка схемы — это гарантированно passthrough
        if (payload.length < CONFLUENT_MIN_HEADER_SIZE) {
            return PayloadFormat.PAYLOAD_PASSTHROUGH;
        }

        // Режим автоматического определения (Sniffing)
        if (configuredFormat == PayloadFormat.PAYLOAD_AUTO_DETECT) {
            byte firstByte = payload[0];
            if (firstByte == CONFLUENT_MAGIC_BYTE) {
                return PayloadFormat.PAYLOAD_CONFLUENT_AVRO;
            } else if ((firstByte == HWX_PROTOCOL_V2 || firstByte == HWX_PROTOCOL_V3)
                    && payload.length >= HWX_MIN_HEADER_SIZE) {
                return PayloadFormat.PAYLOAD_HWX_AVRO;
            } else {
                // Произвольный JSON, Raw binary или String
                return PayloadFormat.PAYLOAD_PASSTHROUGH;
            }
        }

        return configuredFormat;
    }

    /**
     * Проверяет, является ли запись потенциальным сообщением со схемой (Confluent или HWX).
     */
    public boolean hasSchemaHeader(byte[] payload) {
        if (payload == null || payload.length < CONFLUENT_MIN_HEADER_SIZE) {
            return false;
        }
        byte first = payload[0];
        return first == CONFLUENT_MAGIC_BYTE ||
                ((first == HWX_PROTOCOL_V2 || first == HWX_PROTOCOL_V3) && payload.length >= HWX_MIN_HEADER_SIZE);
    }
}
