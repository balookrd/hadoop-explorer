package org.apache.hadoop.explorer.replicator.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

/**
 * Настройки клиентов Kafka для межцодовой репликации.
 *
 * <p>Собрано в одном месте, потому что пределы poll-цикла и батчинга раньше не задавались
 * вовсе: действовали дефолты клиента. {@code max.poll.records = 500} делал недостижимым
 * настроенный размер батча репликации, а {@code max.poll.interval.ms = 5 мин} при деградации
 * WAN приводил к выбрасыванию консьюмера из группы — на 8-32 консьюмерах это давало
 * самоподдерживающийся ребаланс-шторм.
 */
public final class KafkaClientSettings {

    /** Поток отправки может простаивать на троттлинге, поэтому порог терпимости высокий. */
    private static final int MAX_POLL_INTERVAL_MS = 15 * 60 * 1000;
    private static final int SESSION_TIMEOUT_MS = 45_000;
    private static final int HEARTBEAT_INTERVAL_MS = 15_000;
    private static final int MAX_PARTITION_FETCH_BYTES = 2 * 1024 * 1024;
    private static final int FETCH_MAX_WAIT_MS = 50;

    /** Межцодовый трафик выигрывает от крупных батчей даже за счет небольшой задержки. */
    private static final int LINGER_MS = 25;
    private static final int BATCH_SIZE_BYTES = 512 * 1024;
    /** Kafka допускает не больше пяти запросов в полете при enable.idempotence=true. */
    private static final int MAX_IN_FLIGHT_REQUESTS = 5;

    private KafkaClientSettings() {
    }

    /**
     * Настройки консьюмера источника.
     *
     * @param maxBatchRecords размер батча репликации: {@code max.poll.records} выставляется
     *                        под него, иначе батч упирается в дефолтные 500 записей
     */
    public static Properties consumerProps(String bootstrapServers, String groupId, int maxBatchRecords) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, FETCH_MAX_WAIT_MS);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, MAX_PARTITION_FETCH_BYTES);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.max(1, maxBatchRecords));
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, HEARTBEAT_INTERVAL_MS);
        return props;
    }

    /**
     * Значение {@code auto.offset.reset} для стартовой позиции задачи.
     *
     * <p>Действует, только пока у группы задачи нет закоммиченных оффсетов, то есть при первом
     * запуске. Пустая позиция — {@code earliest}: так работали задачи, созданные до появления
     * поля, и для них поведение не меняется.
     */
    public static String offsetResetFor(String startPosition) {
        return "LATEST".equalsIgnoreCase(startPosition == null ? "" : startPosition.trim()) ? "latest" : "earliest";
    }

    /** Настройки консьюмера для служебных операций (разрешение времени, сканирование групп). */
    public static Properties utilityConsumerProps(String bootstrapServers, String groupId) {
        Properties props = consumerProps(bootstrapServers, groupId, 500);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        return props;
    }

    /** Настройки продюсера приемника. */
    public static Properties producerProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE_BYTES);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS);
        return props;
    }
}
