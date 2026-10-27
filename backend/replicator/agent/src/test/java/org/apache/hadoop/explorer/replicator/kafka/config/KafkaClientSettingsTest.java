package org.apache.hadoop.explorer.replicator.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Обязательные параметры клиентов Kafka.
 *
 * <p>Пределы poll-цикла и батчинга не задавались вовсе, поэтому действовали дефолты
 * клиента: {@code max.poll.records = 500} делал недостижимым настроенный размер батча,
 * а {@code max.poll.interval.ms = 5 мин} при деградации WAN приводил к выбрасыванию
 * консьюмера из группы и ребаланс-шторму на всех воркерах. Параметры должны быть заданы
 * явно и согласованно с размером батча репликации.
 */
@DisplayName("Настройки клиентов Kafka")
class KafkaClientSettingsTest {

    private static final String BOOTSTRAP = "kafka-dc1:9092";

    @Test
    @DisplayName("Консьюмер получает явные пределы poll-цикла")
    void testConsumerPollLimitsAreExplicit() {
        Properties props = KafkaClientSettings.consumerProps(BOOTSTRAP, "replicator-dc1", 2000);

        assertEquals(2000, props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                "max.poll.records обязан соответствовать размеру батча репликации, "
                        + "иначе батч упирается в дефолтные 500 записей");
        assertNotNull(props.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                "max.poll.interval.ms должен быть задан явно: при медленном WAN дефолт "
                        + "выбрасывает консьюмера из группы и вызывает ребаланс-шторм");
        assertNotNull(props.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                "session.timeout.ms должен быть задан явно");
        assertNotNull(props.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG),
                "heartbeat.interval.ms должен быть согласован с session.timeout.ms");
    }

    @Test
    @DisplayName("Автокоммит выключен, чтение с начала топика")
    void testConsumerKeepsManualCommitSemantics() {
        Properties props = KafkaClientSettings.consumerProps(BOOTSTRAP, "replicator-dc1", 2000);

        assertEquals("false", props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
                "Оффсеты коммитятся только после подтверждения доставки приемником");
        assertEquals("earliest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(BOOTSTRAP, props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("replicator-dc1", props.get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    @DisplayName("heartbeat.interval.ms меньше session.timeout.ms")
    void testHeartbeatIntervalFitsSessionTimeout() {
        Properties props = KafkaClientSettings.consumerProps(BOOTSTRAP, "replicator-dc1", 2000);

        int sessionTimeout = (int) props.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        int heartbeat = (int) props.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        assertTrue(heartbeat * 3 <= sessionTimeout,
                "Брокер требует heartbeat.interval.ms не больше трети session.timeout.ms, получено "
                        + heartbeat + " и " + sessionTimeout);
    }

    @Test
    @DisplayName("Продюсер получает явные параметры батчинга")
    void testProducerBatchingIsExplicit() {
        Properties props = KafkaClientSettings.producerProps(BOOTSTRAP);

        assertNotNull(props.get(ProducerConfig.LINGER_MS_CONFIG),
                "linger.ms должен быть задан явно: дефолтный 0 отправляет записи по одной");
        assertNotNull(props.get(ProducerConfig.BATCH_SIZE_CONFIG),
                "batch.size должен быть задан явно под межцодовый трафик");
        assertNotNull(props.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
                "max.in.flight.requests.per.connection ограничивает окно неподтвержденных запросов");
    }

    @Test
    @DisplayName("Продюсер сохраняет идемпотентность и acks=all")
    void testProducerKeepsDeliveryGuarantees() {
        Properties props = KafkaClientSettings.producerProps(BOOTSTRAP);

        assertEquals("true", props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG));
        assertEquals("zstd", props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    }

    @Test
    @DisplayName("Идемпотентный продюсер допускает не больше пяти запросов в полете")
    void testInFlightRequestsStayWithinIdempotenceLimit() {
        Properties props = KafkaClientSettings.producerProps(BOOTSTRAP);

        int inFlight = (int) props.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        assertTrue(inFlight >= 1 && inFlight <= 5,
                "При enable.idempotence=true Kafka требует max.in.flight <= 5, получено " + inFlight);
    }

    @Test
    @DisplayName("Стартовая позиция задачи превращается в auto.offset.reset")
    void testOffsetResetForStartPosition() {
        assertEquals("latest", KafkaClientSettings.offsetResetFor("LATEST"));
        assertEquals("latest", KafkaClientSettings.offsetResetFor(" latest "));
        assertEquals("earliest", KafkaClientSettings.offsetResetFor("EARLIEST"));
        // Задачи, созданные до появления поля, сохраняют прежнее поведение
        assertEquals("earliest", KafkaClientSettings.offsetResetFor(""));
        assertEquals("earliest", KafkaClientSettings.offsetResetFor(null));
    }

    @Test
    @DisplayName("Консьюмер репликации использует инкрементальный ребаланс")
    void testCooperativeStickyAssignor() {
        java.util.Properties props = KafkaClientSettings.consumerProps("b:9092", "g", 1000);
        assertEquals(org.apache.kafka.clients.consumer.CooperativeStickyAssignor.class.getName(),
                props.get(org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                "Eager-ребаланс отзывает все партиции группы при каждом перезапуске пула");
    }

    @Test
    @DisplayName("group.instance.id уникален по агенту, задаче и потоку и допустим для брокера")
    void testGroupInstanceId() {
        assertEquals("kafka-agent-dc1-kafka-job-1-0", KafkaClientSettings.groupInstanceId("kafka-agent-dc1", "kafka-job-1", 0));
        assertEquals("agent_a_b-job-3", KafkaClientSettings.groupInstanceId("agent a/b", "job", 3));
    }

    @Test
    @DisplayName("Лимиты размеров согласованы: батч и наибольшая запись помещаются в сообщение gRPC")
    void testSizeLimitsAreConsistent() {
        Properties props = KafkaClientSettings.producerProps("broker:9092");
        int maxRequest = (int) props.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        long buffer = (long) props.get(ProducerConfig.BUFFER_MEMORY_CONFIG);
        assertTrue(maxRequest > 1024 * 1024, "Запись крупнее 1 МБ не должна отвергаться продюсером приемника");
        assertTrue(buffer >= maxRequest, "Буфер продюсера обязан вмещать наибольшую запись");
        assertTrue(KafkaClientSettings.GRPC_MAX_MESSAGE_BYTES > maxRequest,
                "Батч из одной наибольшей записи обязан помещаться в сообщение gRPC");
        assertTrue(KafkaClientSettings.MAX_BATCH_BYTES < KafkaClientSettings.GRPC_MAX_MESSAGE_BYTES);
    }
}
