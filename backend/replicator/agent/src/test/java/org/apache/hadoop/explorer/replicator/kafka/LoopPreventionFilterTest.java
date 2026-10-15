package org.apache.hadoop.explorer.replicator.kafka;

import org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecordHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование фильтра предотвращения циклов (LoopPreventionFilter)")
class LoopPreventionFilterTest {

    @Test
    @DisplayName("Пропуск нового локального сообщения без заголовка Origin")
    void testShouldReplicateNewLocalMessage() {
        LoopPreventionFilter filter = new LoopPreventionFilter("DC-East");
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 100L, "key".getBytes(), "value".getBytes());

        assertTrue(filter.shouldReplicate(record));

        List<KafkaRecordHeader> enriched = filter.enrichHeaders(record.headers());
        assertEquals(2, enriched.size());

        boolean hasOrigin = enriched.stream().anyMatch(h ->
                LoopPreventionFilter.HEADER_ORIGIN.equals(h.getKey()) &&
                "DC-East".equals(h.getValue().toStringUtf8()));
        boolean hasHop = enriched.stream().anyMatch(h ->
                LoopPreventionFilter.HEADER_HOP.equals(h.getKey()) &&
                "1".equals(h.getValue().toStringUtf8()));

        assertTrue(hasOrigin);
        assertTrue(hasHop);
    }

    @Test
    @DisplayName("Отсечение эхо-сообщения, вернувшегося в тот же ДЦ")
    void testDropEchoMessage() {
        LoopPreventionFilter filter = new LoopPreventionFilter("DC-East");

        RecordHeaders headers = new RecordHeaders();
        headers.add(LoopPreventionFilter.HEADER_ORIGIN, "DC-East".getBytes(StandardCharsets.UTF_8));
        headers.add(LoopPreventionFilter.HEADER_HOP, "1".getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 101L, 0L, null, 0, 0,
                "key".getBytes(), "value".getBytes(), headers, null);

        assertFalse(filter.shouldReplicate(record), "Эхо-сообщение должно быть отброшено!");
    }

    @Test
    @DisplayName("Пропуск сообщения из другого ДЦ и инкремент hop")
    void testAcceptRemoteMessageAndIncrementHop() {
        LoopPreventionFilter filter = new LoopPreventionFilter("DC-East");

        RecordHeaders headers = new RecordHeaders();
        headers.add(LoopPreventionFilter.HEADER_ORIGIN, "DC-West".getBytes(StandardCharsets.UTF_8));
        headers.add(LoopPreventionFilter.HEADER_HOP, "1".getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 102L, 0L, null, 0, 0,
                "key".getBytes(), "value".getBytes(), headers, null);

        assertTrue(filter.shouldReplicate(record));

        List<KafkaRecordHeader> enriched = filter.enrichHeaders(record.headers());
        boolean hasHop2 = enriched.stream().anyMatch(h ->
                LoopPreventionFilter.HEADER_HOP.equals(h.getKey()) &&
                "2".equals(h.getValue().toStringUtf8()));
        boolean hasOriginWest = enriched.stream().anyMatch(h ->
                LoopPreventionFilter.HEADER_ORIGIN.equals(h.getKey()) &&
                "DC-West".equals(h.getValue().toStringUtf8()));

        assertTrue(hasHop2, "Hop должен инкрементироваться до 2");
        assertTrue(hasOriginWest, "Origin должен сохраниться как DC-West");
    }
}
