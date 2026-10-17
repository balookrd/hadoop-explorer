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
import java.util.Arrays;
import java.util.List;
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
        String targetDcId = "DC2";
        String targetHost = "localhost";
        int targetPort = DEFAULT_PORT;
        String targetEndpoints = "";
        String role = "dual";
        String topics = "";
        int consumerThreads = 8;
        int batchSize = 2000;
        String groupId = null;

        org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig krbConfig =
                new org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig();

        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            } else if ("--bootstrap-servers".equals(args[i]) && i + 1 < args.length) {
                bootstrapServers = args[++i];
            } else if ("--dc-id".equals(args[i]) && i + 1 < args.length) {
                dcId = args[++i];
            } else if ("--target-dc".equals(args[i]) && i + 1 < args.length) {
                targetDcId = args[++i];
            } else if ("--target-host".equals(args[i]) && i + 1 < args.length) {
                targetHost = args[++i];
            } else if ("--target-port".equals(args[i]) && i + 1 < args.length) {
                targetPort = Integer.parseInt(args[++i]);
            } else if ("--target-endpoints".equals(args[i]) && i + 1 < args.length) {
                targetEndpoints = args[++i];
            } else if ("--role".equals(args[i]) && i + 1 < args.length) {
                role = args[++i].toLowerCase();
            } else if ("--topics".equals(args[i]) && i + 1 < args.length) {
                topics = args[++i];
            } else if ("--consumer-threads".equals(args[i]) && i + 1 < args.length) {
                consumerThreads = Integer.parseInt(args[++i]);
            } else if ("--batch-size".equals(args[i]) && i + 1 < args.length) {
                batchSize = Integer.parseInt(args[++i]);
            } else if ("--group-id".equals(args[i]) && i + 1 < args.length) {
                groupId = args[++i];
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

        if (groupId == null || groupId.isBlank()) {
            groupId = "kafka-replicator-" + dcId;
        }

        String targetsDesc = !targetEndpoints.isBlank() ? targetEndpoints : (targetHost + ":" + targetPort);
        log.info("Запуск kafka-replicator-agent [Role={}, DC={}, Port={}, Targets={}, Threads={}, BatchSize={}, Kerberos={}]",
                role, dcId, port, targetsDesc, consumerThreads, batchSize, krbConfig.isEnabled());

        Server server = null;
        KafkaProducer<byte[], byte[]> producer = null;
        AdminClient adminClient = null;
        KafkaConsumer<byte[], byte[]> resolverConsumer = null;
        org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool consumerPool = null;
        org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient replicationClient = null;

        // 1. Инициализация приемника (Receiver / Dual)
        if ("receiver".equals(role) || "dual".equals(role)) {
            Properties prodProps = new Properties();
            prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            prodProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
            prodProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(prodProps, krbConfig);

            producer = new KafkaProducer<>(prodProps);
            KafkaBatchProducer batchProducer = new KafkaBatchProducer(producer);

            Properties adminProps = new Properties();
            adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(adminProps, krbConfig);
            adminClient = AdminClient.create(adminProps);

            Properties resolverProps = new Properties();
            resolverProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            resolverProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            resolverProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            resolverProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-replicator-time-resolver-" + dcId);
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(resolverProps, krbConfig);
            resolverConsumer = new KafkaConsumer<>(resolverProps);

            SafetyCommitManager safetyCommitManager = new SafetyCommitManager(adminClient, resolverConsumer);
            KafkaDataTransferServiceImpl service = new KafkaDataTransferServiceImpl(batchProducer, safetyCommitManager, dcId);

            server = ServerBuilder.forPort(port)
                    .addService(service)
                    .maxInboundMessageSize(64 * 1024 * 1024)
                    .build()
                    .start();

            log.info("gRPC сервер kafka-replicator-agent успешно запущен на порту {}", port);
        }

        // 2. Инициализация отправителя (Sender / Dual)
        if (("sender".equals(role) || "dual".equals(role)) && !topics.isBlank()) {
            List<String> topicList = Arrays.stream(topics.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();

            List<String> targetList;
            if (!targetEndpoints.isBlank()) {
                targetList = Arrays.stream(targetEndpoints.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .toList();
            } else {
                targetList = List.of(targetHost + ":" + targetPort);
            }

            log.info("Инициализация пула консьюмеров отправителя: топики={}, потоков={}, targets={}",
                    topicList, consumerThreads, targetList);

            replicationClient = new org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient(targetList);

            List<KafkaConsumer<byte[], byte[]>> consumers = new java.util.ArrayList<>();
            for (int i = 0; i < consumerThreads; i++) {
                Properties consProps = new Properties();
                consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
                consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
                consProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                consProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048576); // 1 MB
                consProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50);
                consProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 2097152); // 2 MB
                org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(consProps, krbConfig);

                KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consProps);
                consumer.subscribe(topicList);
                consumers.add(consumer);
            }

            org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter loopFilter =
                    new org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter(dcId);
            org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier classifier =
                    new org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier();
            org.apache.hadoop.explorer.replicator.kafka.schema.SchemaIdResolver passthroughResolver =
                    new org.apache.hadoop.explorer.replicator.kafka.schema.SchemaIdResolver() {
                        @Override public int resolveConfluentToConfluent(String topic, int sourceId) { return sourceId; }
                        @Override public int resolveHwxToConfluent(String topic, long hwxVersionId) { return (int) hwxVersionId; }
                        @Override public long resolveConfluentToHwx(String topic, int confluentId) { return confluentId; }
                        @Override public long resolveHwxToHwx(String topic, long sourceHwxId) { return sourceHwxId; }
                    };
            org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder transcoder =
                    new org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder(passthroughResolver);

            consumerPool = new org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool(
                    consumers,
                    replicationClient,
                    loopFilter,
                    classifier,
                    transcoder,
                    dcId,
                    targetDcId,
                    org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat.PAYLOAD_PASSTHROUGH,
                    org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat.PAYLOAD_PASSTHROUGH
            );
            consumerPool.setMaxBatchSize(batchSize);
            consumerPool.start();
            log.info("Пул консьюмеров отправителя успешно запущен");
        }

        final Server finalServer = server;
        final KafkaProducer<byte[], byte[]> finalProducer = producer;
        final AdminClient finalAdminClient = adminClient;
        final KafkaConsumer<byte[], byte[]> finalResolverConsumer = resolverConsumer;
        final org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool finalPool = consumerPool;
        final org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient finalClient = replicationClient;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Остановка kafka-replicator-agent...");
            try {
                if (finalPool != null) {
                    finalPool.close();
                }
                if (finalClient != null) {
                    finalClient.close();
                }
                if (finalServer != null) {
                    finalServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                }
                if (finalProducer != null) {
                    finalProducer.close();
                }
                if (finalAdminClient != null) {
                    finalAdminClient.close();
                }
                if (finalResolverConsumer != null) {
                    finalResolverConsumer.close();
                }
            } catch (Exception e) {
                log.error("Ошибка при остановке агента: {}", e.getMessage());
            }
        }));

        if (server != null) {
            server.awaitTermination();
        } else {
            while (true) {
                Thread.sleep(10000);
            }
        }
    }
}
