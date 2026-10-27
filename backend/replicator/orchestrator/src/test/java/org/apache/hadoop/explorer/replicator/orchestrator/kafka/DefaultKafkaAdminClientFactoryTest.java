package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Протокол Kafka-клиентов оркестратора берется из топологии кластера.
 *
 * <p>Клиент знал только SASL_PLAINTEXT: к брокерам с TLS (SASL_SSL) оркестратор не
 * подключался, и discovery и автосоздание топиков на таких кластерах не работали.
 */
@DisplayName("Kafka AdminClient: протокол и truststore из топологии")
class DefaultKafkaAdminClientFactoryTest {

    private static ReplicatorProperties.ClusterConfig cluster(String id, String bootstrap, String protocol) {
        ReplicatorProperties.ClusterConfig cfg = new ReplicatorProperties.ClusterConfig(id, id, id, null, "h", 1);
        cfg.setKafkaBootstrapServers(bootstrap);
        cfg.setKafkaSecurityProtocol(protocol);
        return cfg;
    }

    @Test
    @DisplayName("Без протокола в топологии: SASL_PLAINTEXT при Kerberos, иначе PLAINTEXT")
    void testDefaultProtocol() {
        assertEquals("SASL_PLAINTEXT", DefaultKafkaAdminClientFactory.securityProtocol(null, true));
        assertEquals("PLAINTEXT", DefaultKafkaAdminClientFactory.securityProtocol(null, false));
        assertEquals("SASL_PLAINTEXT",
                DefaultKafkaAdminClientFactory.securityProtocol(cluster("c", "b:9092", null), true));
    }

    @Test
    @DisplayName("Протокол из топологии; при Kerberos SSL поднимается до SASL_SSL")
    void testProtocolFromTopology() {
        assertEquals("SASL_SSL",
                DefaultKafkaAdminClientFactory.securityProtocol(cluster("c", "b:9092", "sasl_ssl"), true));
        assertEquals("SSL", DefaultKafkaAdminClientFactory.securityProtocol(cluster("c", "b:9092", "SSL"), false));
        assertEquals("SASL_SSL", DefaultKafkaAdminClientFactory.securityProtocol(cluster("c", "b:9092", "SSL"), true));
        assertThrows(IllegalArgumentException.class,
                () -> DefaultKafkaAdminClientFactory.securityProtocol(cluster("c", "b:9092", "TLS"), true));
    }

    @Test
    @DisplayName("Кластер находится по любому общему брокеру, без учета регистра и пробелов")
    void testClusterIsFoundByBroker() {
        ReplicatorProperties props = new ReplicatorProperties();
        props.setClusters(List.of(
                cluster("dc1", "Kafka-1.prod:9093, kafka-2.prod:9093", "SASL_SSL"),
                cluster("dc2", "kafka-dr:9092", null)));
        DefaultKafkaAdminClientFactory factory = new DefaultKafkaAdminClientFactory();
        factory.setTopologyRegistry(new TopologyRegistry(props));

        assertEquals("dc1", factory.findCluster("kafka-2.prod:9093").getId());
        assertEquals("dc1", factory.findCluster("kafka-1.prod:9093,kafka-3.prod:9093").getId());
        assertEquals("dc2", factory.findCluster("kafka-dr:9092").getId());
        assertNull(factory.findCluster("unknown:9092"));
    }
}
