package org.apache.hadoop.explorer.replicator.kafka;

import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.schema.CachedSchemaRegistryResolver;
import org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование Zero-Copy транскодера Wire-форматов Schema Registry")
class ZeroCopyWireTranscoderTest {

    private CachedSchemaRegistryResolver resolver;
    private ZeroCopyWireTranscoder transcoder;

    @BeforeEach
    void setUp() {
        resolver = new CachedSchemaRegistryResolver();
        transcoder = new ZeroCopyWireTranscoder(resolver);
    }

    @Test
    @DisplayName("Confluent -> Confluent: подмена ID с 1 на 42 при несовпадении независимых реестров")
    void testConfluentToConfluentIdTranslation() {
        resolver.putConfluentToConfluent(1, 42);

        byte[] avroBody = "avro-binary-payload".getBytes();
        byte[] input = new byte[5 + avroBody.length];
        input[0] = 0x00;
        ByteBuffer.wrap(input, 1, 4).putInt(1);
        System.arraycopy(avroBody, 0, input, 5, avroBody.length);

        byte[] output = transcoder.transcode("orders", input, PayloadFormat.PAYLOAD_CONFLUENT_AVRO, PayloadFormat.PAYLOAD_CONFLUENT_AVRO);

        assertNotNull(output);
        assertEquals(input.length, output.length);
        assertEquals(0x00, output[0]);
        assertEquals(42, ByteBuffer.wrap(output, 1, 4).getInt(), "Schema ID должен стать 42");

        // Проверяем, что тело Avro в точности сохранилось
        byte[] outputAvroBody = Arrays.copyOfRange(output, 5, output.length);
        assertArrayEquals(avroBody, outputAvroBody);
    }

    @Test
    @DisplayName("Hortonworks -> Confluent: конвертация 9-байтного заголовка в 5-байтный")
    void testHwxToConfluentTranscoding() {
        long hwxVersionId = 100500L;
        int confluentId = 77;
        resolver.putHwxToConfluent(hwxVersionId, confluentId);

        byte[] avroBody = "some-complex-avro-data".getBytes();
        byte[] hwxPayload = new byte[9 + avroBody.length];
        hwxPayload[0] = 0x02; // HWX protocol v2
        ByteBuffer.wrap(hwxPayload, 1, 8).putLong(hwxVersionId);
        System.arraycopy(avroBody, 0, hwxPayload, 9, avroBody.length);

        byte[] confluentPayload = transcoder.transcode("users", hwxPayload, PayloadFormat.PAYLOAD_HWX_AVRO, PayloadFormat.PAYLOAD_CONFLUENT_AVRO);

        assertNotNull(confluentPayload);
        assertEquals(5 + avroBody.length, confluentPayload.length, "Длина должна уменьшиться ровно на 4 байта (9 -> 5)");
        assertEquals(0x00, confluentPayload[0], "Magic byte Confluent должен быть 0x00");
        assertEquals(confluentId, ByteBuffer.wrap(confluentPayload, 1, 4).getInt());

        byte[] resultAvro = Arrays.copyOfRange(confluentPayload, 5, confluentPayload.length);
        assertArrayEquals(avroBody, resultAvro, "Бинарное тело Avro должно остаться полностью неизменным");
    }

    @Test
    @DisplayName("Confluent -> Hortonworks: конвертация 5-байтного заголовка в 9-байтный")
    void testConfluentToHwxTranscoding() {
        int confluentId = 15;
        long hwxVersionId = 8888L;
        resolver.putHwxToConfluent(hwxVersionId, confluentId); // обратный маппинг сохраняется внутри

        byte[] avroBody = "payment-event-avro-bytes".getBytes();
        byte[] confluentPayload = new byte[5 + avroBody.length];
        confluentPayload[0] = 0x00;
        ByteBuffer.wrap(confluentPayload, 1, 4).putInt(confluentId);
        System.arraycopy(avroBody, 0, confluentPayload, 5, avroBody.length);

        byte[] hwxPayload = transcoder.transcode("payments", confluentPayload, PayloadFormat.PAYLOAD_CONFLUENT_AVRO, PayloadFormat.PAYLOAD_HWX_AVRO);

        assertNotNull(hwxPayload);
        assertEquals(9 + avroBody.length, hwxPayload.length, "Длина должна увеличиться на 4 байта (5 -> 9)");
        assertEquals(0x02, hwxPayload[0], "HWX protocol v2 должен быть 0x02");
        assertEquals(hwxVersionId, ByteBuffer.wrap(hwxPayload, 1, 8).getLong());

        byte[] resultAvro = Arrays.copyOfRange(hwxPayload, 9, hwxPayload.length);
        assertArrayEquals(avroBody, resultAvro);
    }

    @Test
    @DisplayName("Hortonworks -> Hortonworks: замена 8-байтного Long ID при несовпадении реестров")
    void testHwxToHwxTranslation() {
        resolver.putHwxToHwx(100L, 200L);

        byte[] avroBody = "log-event".getBytes();
        byte[] input = new byte[9 + avroBody.length];
        input[0] = 0x02;
        ByteBuffer.wrap(input, 1, 8).putLong(100L);
        System.arraycopy(avroBody, 0, input, 9, avroBody.length);

        byte[] output = transcoder.transcode("logs", input, PayloadFormat.PAYLOAD_HWX_AVRO, PayloadFormat.PAYLOAD_HWX_AVRO);

        assertNotNull(output);
        assertEquals(input.length, output.length);
        assertEquals(0x02, output[0]);
        assertEquals(200L, ByteBuffer.wrap(output, 1, 8).getLong());
    }

    @Test
    @DisplayName("Tombstone (null) и Passthrough остаются без изменений")
    void testTombstoneAndPassthrough() {
        assertNull(transcoder.transcode("orders", null, PayloadFormat.PAYLOAD_CONFLUENT_AVRO, PayloadFormat.PAYLOAD_HWX_AVRO));

        byte[] json = "{\"msg\":\"hi\"}".getBytes();
        assertSame(json, transcoder.transcode("chat", json, PayloadFormat.PAYLOAD_PASSTHROUGH, PayloadFormat.PAYLOAD_PASSTHROUGH));
    }
}
