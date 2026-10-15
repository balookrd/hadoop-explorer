package org.apache.hadoop.explorer.replicator.kafka.agent;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.hadoop.explorer.replicator.kafka.offset.SafetyCommitManager;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.apache.hadoop.explorer.replicator.kafka.service.KafkaDataTransferServiceImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Точка входа отдельного демона kafka-replicator-agent.
 * <p>
 * Изолирован от HDFS-воркеров, запускается в зоне Kafka-брокеров и обслуживает gRPC Data Plane
 * для двунаправленной репликации и трансляции оффсетов.
 */
public class KafkaReplicatorAgentMain {

    private static final Logger log = LoggerFactory.getLogger(KafkaReplicatorAgentMain.class);

    private static final int DEFAULT_PORT = 50052;

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = DEFAULT_PORT;
        String bootstrapServers = "localhost:9092";
        String dcId = "DC1";
        org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig krbConfig =
                new org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig();

        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            } else if ("--bootstrap-servers".equals(args[i]) && i + 1 < args.length) {
                bootstrapServers = args[++i];
            } else if ("--dc-id".equals(args[i]) && i + 1 < args.length) {
                dcId = args[++i];
            } else if ("--kerberos-enabled".equals(args[i])) {
                krbConfig.setEnabled(true);
            } else if ("--principal".equals(args[i]) && i + 1 < args.length) {
                krbConfig.setPrincipal(args[++i]);
            } else if ("--keytab".equals(args[i]) && i + 1 < args.length) {
                krbConfig.setKeytabPath(args[++i]);
            } else if ("--service-name".equals(args[i]) && i + 1 < args.length) {
                krbConfig.setServiceName(args[++i]);
            } else if ("--security-protocol".equals(args[i]) && i + 1 < args.length) {
                krbConfig.setSecurityProtocol(args[++i]);
            } else if ("--krb5-conf".equals(args[i]) && i + 1 < args.length) {
                krbConfig.setKrb5ConfPath(args[++i]);
            } else if ("--sr-kerberos-enabled".equals(args[i])) {
                krbConfig.setSrKerberosEnabled(true);
            } else if ("--sr-service-name".equals(args[i]) && i + 1 < args.length) {
                krbConfig.setSrServiceName(args[++i]);
            }
        }

        log.info("Запуск kafka-replicator-agent [DC={}, Port={}, Kafka={}, Kerberos={}, SR-Kerberos={}]",
                dcId, port, bootstrapServers, krbConfig.isEnabled(), krbConfig.isSrKerberosEnabled());

        // 1. Инициализация продюсера
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
        prodProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(prodProps, krbConfig);

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(prodProps);
        KafkaBatchProducer batchProducer = new KafkaBatchProducer(producer);

        // 2. Инициализация AdminClient и консьюмера для резолва времени
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(adminProps, krbConfig);
        AdminClient adminClient = AdminClient.create(adminProps);

        Properties resolverProps = new Properties();
        resolverProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        resolverProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        resolverProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        resolverProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-replicator-time-resolver-" + dcId);
        org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(resolverProps, krbConfig);
        KafkaConsumer<byte[], byte[]> resolverConsumer = new KafkaConsumer<>(resolverProps);

        SafetyCommitManager safetyCommitManager = new SafetyCommitManager(adminClient, resolverConsumer);

        // 3. Запуск gRPC-сервера
        KafkaDataTransferServiceImpl service = new KafkaDataTransferServiceImpl(batchProducer, safetyCommitManager, dcId);

        Server server = ServerBuilder.forPort(port)
                .addService(service)
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build()
                .start();

        log.info("gRPC сервер kafka-replicator-agent успешно запущен на порту {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Остановка kafka-replicator-agent...");
            try {
                server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                producer.close();
                adminClient.close();
                resolverConsumer.close();
            } catch (Exception e) {
                log.error("Ошибка при остановке агента: {}", e.getMessage());
            }
        }));

        server.awaitTermination();
    }
}
