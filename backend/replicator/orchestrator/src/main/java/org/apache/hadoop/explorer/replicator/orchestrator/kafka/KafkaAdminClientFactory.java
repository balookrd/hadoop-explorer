package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.kafka.clients.admin.AdminClient;

/**
 * Фабрика создания экземпляров AdminClient для доступа к брокерам Apache Kafka
 * с поддержкой Kerberos SASL/GSSAPI.
 */
public interface KafkaAdminClientFactory {

    AdminClient createAdminClient(String bootstrapServers,
                                  boolean kerberosEnabled,
                                  String principal,
                                  String keytabPath,
                                  String serviceName);
}
