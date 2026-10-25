package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Выбор резолвера схем и форматов полезной нагрузки для демона Kafka.
 *
 * <p>Демон подставлял в транскодер заглушку: все четыре метода {@code SchemaIdResolver}
 * возвращали исходный идентификатор, а форматы источника и приемника были жестко заданы как
 * {@code PAYLOAD_PASSTHROUGH}. Настоящий {@code CachedSchemaRegistryResolver} в рабочем коде не
 * создавался, поэтому параметры Schema Registry ни на что не влияли, а при разных
 * идентификаторах схем в кластерах потребители на целевой стороне не разобрали бы сообщения.
 *
 * <p>Заглушка при этом остается законным режимом: если реестры не настроены, переносить байты
 * как есть — единственное безопасное поведение.
 */
@DisplayName("Kafka: выбор резолвера схем и форматов")
class KafkaSchemaTranscodingTest {

    @Test
    @DisplayName("Без адресов реестров остается passthrough")
    void testPassthroughWithoutRegistries() {
        SchemaIdResolver resolver = KafkaSchemaTranscoding.resolver(
                null, "CONFLUENT", null, "CONFLUENT", null, null);

        assertFalse(resolver instanceof CachedSchemaRegistryResolver,
                "Без адресов реестров резолвер обращаться никуда не может");
        assertEquals(42, resolver.resolveConfluentToConfluent("orders", 42),
                "Passthrough обязан возвращать исходный идентификатор схемы");
        assertEquals(7L, resolver.resolveHwxToHwx("orders", 7L));
    }

    @Test
    @DisplayName("Односторонняя настройка реестра не включает трансляцию")
    void testOneSidedRegistryStaysPassthrough() {
        SchemaIdResolver resolver = KafkaSchemaTranscoding.resolver(
                "http://sr-dc1:8081", "CONFLUENT", "  ", "CONFLUENT", null, null);

        assertFalse(resolver instanceof CachedSchemaRegistryResolver,
                "Транслировать идентификаторы можно только зная оба реестра: иначе в целевой "
                        + "кластер уйдут чужие идентификаторы схем");
    }

    @Test
    @DisplayName("С обоими адресами включается настоящий резолвер с кэшем")
    void testRealResolverWithBothRegistries() {
        SchemaIdResolver resolver = KafkaSchemaTranscoding.resolver(
                "http://sr-dc1:8081", "CONFLUENT", "http://sr-dc2:8081", "CONFLUENT", null, null);

        assertInstanceOf(CachedSchemaRegistryResolver.class, resolver,
                "При заданных реестрах обязан использоваться кэширующий резолвер, иначе "
                        + "параметры Schema Registry ни на что не влияют");
    }

    @Test
    @DisplayName("При выключенном транскодинге формат всегда passthrough")
    void testFormatIsPassthroughWhenDisabled() {
        assertEquals(PayloadFormat.PAYLOAD_PASSTHROUGH,
                KafkaSchemaTranscoding.format("CONFLUENT_AVRO", false),
                "Выключенный транскодинг не должен менять байты сообщения");
    }

    @Test
    @DisplayName("Формат разбирается из конфигурации")
    void testFormatIsParsedFromConfiguration() {
        assertEquals(PayloadFormat.PAYLOAD_CONFLUENT_AVRO,
                KafkaSchemaTranscoding.format("CONFLUENT_AVRO", true));
        assertEquals(PayloadFormat.PAYLOAD_CONFLUENT_AVRO,
                KafkaSchemaTranscoding.format("confluent", true));
        assertEquals(PayloadFormat.PAYLOAD_HWX_AVRO,
                KafkaSchemaTranscoding.format("hwx", true));
        assertEquals(PayloadFormat.PAYLOAD_HWX_AVRO,
                KafkaSchemaTranscoding.format("PAYLOAD_HWX_AVRO", true));
    }

    @Test
    @DisplayName("Неизвестный формат при включенном транскодинге отдается автоопределению")
    void testUnknownFormatFallsBackToAutoDetect() {
        assertEquals(PayloadFormat.PAYLOAD_AUTO_DETECT,
                KafkaSchemaTranscoding.format("avro-something", true),
                "Неизвестное имя формата не повод терять транскодинг: классификатор определит "
                        + "формат по заголовку сообщения");
        assertEquals(PayloadFormat.PAYLOAD_AUTO_DETECT,
                KafkaSchemaTranscoding.format(null, true));
    }
}
