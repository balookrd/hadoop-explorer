package org.apache.hadoop.explorer.replicator.kafka;

import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование классификатора форматов (PayloadFormatClassifier)")
class PayloadFormatClassifierTest {

    private final PayloadFormatClassifier classifier = new PayloadFormatClassifier();

    @Test
    @DisplayName("Tombstone (null payload) классифицируется как PASSTHROUGH")
    void testTombstoneClassification() {
        assertEquals(PayloadFormat.PAYLOAD_PASSTHROUGH, classifier.classify(null, PayloadFormat.PAYLOAD_AUTO_DETECT));
        assertEquals(PayloadFormat.PAYLOAD_PASSTHROUGH, classifier.classify(new byte[0], PayloadFormat.PAYLOAD_AUTO_DETECT));
    }

    @Test
    @DisplayName("Чистый JSON/текст при автодетекции классифицируется как PASSTHROUGH")
    void testPlainJsonClassification() {
        byte[] json = "{\"user\":\"alice\",\"amount\":100}".getBytes();
        assertEquals(PayloadFormat.PAYLOAD_PASSTHROUGH, classifier.classify(json, PayloadFormat.PAYLOAD_AUTO_DETECT));
    }

    @Test
    @DisplayName("Confluent Avro (0x00 + 4 байта ID) распознается автоматически")
    void testConfluentAvroClassification() {
        byte[] payload = new byte[15];
        payload[0] = 0x00; // Confluent magic byte
        ByteBuffer.wrap(payload, 1, 4).putInt(42);

        assertEquals(PayloadFormat.PAYLOAD_CONFLUENT_AVRO, classifier.classify(payload, PayloadFormat.PAYLOAD_AUTO_DETECT));
    }

    @Test
    @DisplayName("Hortonworks Avro (0x02 + 8 байт ID) распознается автоматически")
    void testHwxAvroClassification() {
        byte[] payload = new byte[20];
        payload[0] = 0x02; // HWX protocol v2
        ByteBuffer.wrap(payload, 1, 8).putLong(100500L);

        assertEquals(PayloadFormat.PAYLOAD_HWX_AVRO, classifier.classify(payload, PayloadFormat.PAYLOAD_AUTO_DETECT));
    }

    @Test
    @DisplayName("Топик с явным PASSTHROUGH не трогает даже сообщения с 0x00")
    void testExplicitPassthroughOverride() {
        byte[] payload = new byte[10];
        payload[0] = 0x00;

        assertEquals(PayloadFormat.PAYLOAD_PASSTHROUGH, classifier.classify(payload, PayloadFormat.PAYLOAD_PASSTHROUGH));
    }
}
