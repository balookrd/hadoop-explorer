package org.apache.hadoop.explorer.replicator.kafka.filter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Тестирование фильтра черного списка консьюмер-групп (ConsumerGroupBlacklistFilter)")
class ConsumerGroupBlacklistFilterTest {

    private final ConsumerGroupBlacklistFilter filter = ConsumerGroupBlacklistFilter.defaultFilter();

    @Test
    @DisplayName("Исключение собственных групп репликатора")
    void shouldExcludeReplicatorOwnGroups() {
        assertTrue(filter.isExcluded("kafka-replicator-dc1"));
        assertTrue(filter.isExcluded("kafka-replicator-time-resolver-dc1"));
        assertTrue(filter.isExcluded("kafka-replicator-agent-dc2"));
        assertTrue(filter.isExcluded("replicator-worker-01"));
        assertTrue(filter.isExcluded("hadoop-replicator-job-42"));

        assertFalse(filter.isAllowed("kafka-replicator-dc1"));
    }

    @Test
    @DisplayName("Исключение системных групп брокера Kafka")
    void shouldExcludeSystemKafkaGroups() {
        assertTrue(filter.isExcluded("__consumer_offsets"));
        assertTrue(filter.isExcluded("__transaction_state"));
        assertTrue(filter.isExcluded("__internal_group"));
    }

    @Test
    @DisplayName("Исключение служебных инфраструктурных групп (Schema Registry, Kafka Connect, Exporter)")
    void shouldExcludeInfrastructureGroups() {
        assertTrue(filter.isExcluded("schema-registry-dc1"));
        assertTrue(filter.isExcluded("schema-registry-cluster-prod"));
        assertTrue(filter.isExcluded("connect-distributed-cluster"));
        assertTrue(filter.isExcluded("connect-sink-hdfs"));
        assertTrue(filter.isExcluded("console-consumer-48192"));
        assertTrue(filter.isExcluded("kafka-exporter-metrics"));
    }

    @Test
    @DisplayName("Разрешение обычных бизнес-групп приложений")
    void shouldAllowBusinessConsumerGroups() {
        assertTrue(filter.isAllowed("orders-processing-group"));
        assertTrue(filter.isAllowed("billing-service-v2"));
        assertTrue(filter.isAllowed("fraud-detector"));
        assertTrue(filter.isAllowed("analytics-dw-stream"));
        assertTrue(filter.isAllowed("retail-orders-worker"));

        assertFalse(filter.isExcluded("orders-processing-group"));
    }

    @Test
    @DisplayName("Обработка null и пустых значений как excluded")
    void shouldExcludeNullOrBlankGroups() {
        assertTrue(filter.isExcluded(null));
        assertTrue(filter.isExcluded(""));
        assertTrue(filter.isExcluded("   "));
    }

    @Test
    @DisplayName("Фильтрация коллекций групп: разделение на allowed и excluded")
    void shouldFilterCollectionsProperly() {
        List<String> rawGroups = List.of(
                "billing-service",
                "kafka-replicator-dc1",
                "orders-group",
                "console-consumer-12345",
                "__consumer_offsets",
                "analytics-stream",
                "schema-registry-test"
        );

        List<String> allowed = filter.filterAllowed(rawGroups);
        assertEquals(List.of("billing-service", "orders-group", "analytics-stream"), allowed);

        Set<String> excluded = filter.filterExcluded(rawGroups);
        assertEquals(Set.of("kafka-replicator-dc1", "console-consumer-12345", "__consumer_offsets", "schema-registry-test"), excluded);
    }

    @Test
    @DisplayName("Поддержка кастомного регулярного выражения")
    void shouldSupportCustomRegex() {
        ConsumerGroupBlacklistFilter customFilter = new ConsumerGroupBlacklistFilter("^(custom-agent-.*|test-.*)$");

        assertTrue(customFilter.isExcluded("custom-agent-worker"));
        assertTrue(customFilter.isExcluded("test-temp-group"));
        assertTrue(customFilter.isAllowed("kafka-replicator-dc1")); // В кастомном не исключается
        assertTrue(customFilter.isAllowed("orders-service"));
    }
}
