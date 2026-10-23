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

import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.generated.control.KafkaJobItemProto;
import org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
        String orchestratorGrpc = System.getenv("ORCHESTRATOR_GRPC_ADDRESSES");
        if (orchestratorGrpc == null || orchestratorGrpc.isBlank()) {
            orchestratorGrpc = System.getenv("ORCHESTRATOR_GRPC_ADDRESS");
        }
        if (orchestratorGrpc == null || orchestratorGrpc.isBlank()) {
            orchestratorGrpc = System.getenv("ORCHESTRATOR_GRPC_URL");
        }
        String agentId = System.getenv("AGENT_ID");
        String agentSecret = System.getenv("AGENT_SECRET");
        String advertisedGrpcAddress = System.getenv("ADVERTISED_GRPC_ADDRESS");
        Double maxBandwidthMbS = null;
        String envBw = System.getenv("AGENT_MAX_BANDWIDTH_MB_S");
        if (envBw == null || envBw.isBlank()) {
            envBw = System.getenv("MAX_BANDWIDTH_MB_S");
        }
        if (envBw != null && !envBw.isBlank()) {
            try {
                maxBandwidthMbS = Double.parseDouble(envBw);
            } catch (NumberFormatException ignored) {}
        }
        boolean tlsEnabled = "true".equalsIgnoreCase(System.getenv("TLS_ENABLED"));
        String tlsCertChain = System.getenv("TLS_CERT_CHAIN");
        String tlsPrivateKey = System.getenv("TLS_PRIVATE_KEY");
        String tlsTrustCert = System.getenv("TLS_TRUST_CERT");

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
            } else if (("--max-bandwidth-mb-s".equals(args[i]) || "--bandwidth".equals(args[i])) && i + 1 < args.length) {
                maxBandwidthMbS = Double.parseDouble(args[++i]);
            } else if ("--batch-size".equals(args[i]) && i + 1 < args.length) {
                batchSize = Integer.parseInt(args[++i]);
            } else if ("--group-id".equals(args[i]) && i + 1 < args.length) {
                groupId = args[++i];
            } else if (("--orchestrator-grpc".equals(args[i]) || "--orchestrator-grpc-addresses".equals(args[i])) && i + 1 < args.length) {
                orchestratorGrpc = args[++i];
            } else if ("--agent-id".equals(args[i]) && i + 1 < args.length) {
                agentId = args[++i];
            } else if ("--agent-secret".equals(args[i]) && i + 1 < args.length) {
                agentSecret = args[++i];
            } else if ("--advertised-grpc-address".equals(args[i]) && i + 1 < args.length) {
                advertisedGrpcAddress = args[++i];
            } else if ("--tls-enabled".equals(args[i])) {
                tlsEnabled = true;
                if (i + 1 < args.length && ("true".equalsIgnoreCase(args[i + 1]) || "false".equalsIgnoreCase(args[i + 1]))) {
                    tlsEnabled = Boolean.parseBoolean(args[++i]);
                }
            } else if ("--tls-cert-chain".equals(args[i]) && i + 1 < args.length) {
                tlsCertChain = args[++i];
            } else if ("--tls-private-key".equals(args[i]) && i + 1 < args.length) {
                tlsPrivateKey = args[++i];
            } else if ("--tls-trust-cert".equals(args[i]) && i + 1 < args.length) {
                tlsTrustCert = args[++i];
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
        if (agentId == null || agentId.isBlank()) {
            agentId = "kafka-agent-" + dcId.toLowerCase() + "-" + port;
        }
        if (advertisedGrpcAddress == null || advertisedGrpcAddress.isBlank()) {
            advertisedGrpcAddress = "localhost:" + port;
        }

        String targetsDesc = !targetEndpoints.isBlank() ? targetEndpoints : (targetHost + ":" + targetPort);
        log.info("Запуск kafka-replicator-agent [AgentId={}, Role={}, DC={}, Port={}, Targets={}, Threads={}, BatchSize={}, Kerberos={}, TLS={}, Orchestrator={}]",
                agentId, role, dcId, port, targetsDesc, consumerThreads, batchSize, krbConfig.isEnabled(), tlsEnabled, orchestratorGrpc);

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

            var serverBuilder = ServerBuilder.forPort(port)
                    .addService(service)
                    .maxInboundMessageSize(64 * 1024 * 1024);

            if (tlsEnabled) {
                if (tlsCertChain != null && !tlsCertChain.isBlank() && tlsPrivateKey != null && !tlsPrivateKey.isBlank()) {
                    serverBuilder.useTransportSecurity(new java.io.File(tlsCertChain), new java.io.File(tlsPrivateKey));
                    log.info("Включен gRPC TLS для сервера (сертификат: {})", tlsCertChain);
                } else {
                    log.warn("TLS включен (--tls-enabled), но сертификат или приватный ключ не указаны!");
                }
            }

            if (agentSecret != null && !agentSecret.isBlank()) {
                serverBuilder.intercept(new AgentGrpcAuthInterceptor(agentSecret));
                log.info("Включена gRPC L7 аутентификация сервера через x-agent-secret");
            }

            server = serverBuilder.build().start();

            log.info("gRPC сервер kafka-replicator-agent успешно запущен на порту {}", port);
        }

        // 2. Инициализация отправителя (Sender / Dual)
        boolean hasCliTopics = !topics.isBlank();
        boolean hasOrchestrator = orchestratorGrpc != null && !orchestratorGrpc.isBlank();
        if (("sender".equals(role) || "dual".equals(role)) && (hasCliTopics || hasOrchestrator)) {
            List<String> topicList = hasCliTopics
                    ? Arrays.stream(topics.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .toList()
                    : List.of();

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

            replicationClient = new org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient(
                    targetList, agentSecret, tlsEnabled, tlsTrustCert);

            List<KafkaConsumer<byte[], byte[]>> consumers = new java.util.ArrayList<>();
            for (int i = 0; i < consumerThreads; i++) {
                Properties consProps = new Properties();
                consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
                consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
                consProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                consProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
                consProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50);
                consProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 2097152); // 2 MB
                org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(consProps, krbConfig);

                KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consProps);
                if (!topicList.isEmpty()) {
                    consumer.subscribe(topicList);
                }
                consumers.add(consumer);
            }

            org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter loopFilter =
                    new org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter(dcId, targetDcId);
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
            if (maxBandwidthMbS != null && maxBandwidthMbS > 0) {
                consumerPool.setBandwidthLimitMb(maxBandwidthMbS);
                log.info("Установлен локальный лимит полосы пропускания агента: {} МБ/с", maxBandwidthMbS);
            }
            consumerPool.start();
            log.info("Пул консьюмеров отправителя успешно запущен");
        }

        // 3. Подключение к Orchestrator Control Plane
        OrchestratorGrpcClient orchestratorClient = null;
        ScheduledExecutorService heartbeatExecutor = null;

        if (orchestratorGrpc != null && !orchestratorGrpc.isBlank()) {
            log.info("Регистрация в Orchestrator Control Plane: {} (agentId={})", orchestratorGrpc, agentId);
            orchestratorClient = new OrchestratorGrpcClient(orchestratorGrpc, agentSecret);
            AgentRegisterRequest regReq = new AgentRegisterRequest(
                    agentId,
                    dcId,
                    role,
                    ("receiver".equals(role) || "dual".equals(role)) ? advertisedGrpcAddress : null,
                    null,
                    (maxBandwidthMbS != null && maxBandwidthMbS > 0) ? maxBandwidthMbS : 100.0
            );
            boolean regSuccess = orchestratorClient.register(regReq);
            log.info("Результат регистрации агента '{}' в оркестраторе: {}", agentId, regSuccess);

            heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "kafka-agent-heartbeat");
                t.setDaemon(true);
                return t;
            });

            final OrchestratorGrpcClient finalOrch = orchestratorClient;
            final String finalAgentId = agentId;
            final String finalDcId = dcId;
            final String finalRole = role;
            final String finalAdvAddr = advertisedGrpcAddress;
            final Double finalMaxBandwidthMbS = maxBandwidthMbS;
            final org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool finalPool = consumerPool;
            final org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient innerClient = replicationClient;

            heartbeatExecutor.scheduleAtFixedRate(() -> {
                try {
                    AgentHeartbeatRequest hbReq = new AgentHeartbeatRequest(
                            finalAgentId,
                            finalDcId,
                            finalAdvAddr,
                            0,
                            finalRole
                    );
                    if (finalMaxBandwidthMbS != null && finalMaxBandwidthMbS > 0) {
                        hbReq.setBandwidthLimitMbS(finalMaxBandwidthMbS);
                    }
                    if (finalPool != null) {
                        hbReq.setKafkaMessagesReplicated(finalPool.getTotalMessagesReplicated());
                        hbReq.setKafkaBytesReplicated(finalPool.getTotalBytesReplicated());
                        hbReq.setKafkaCurrentLagMs(finalPool.getCurrentLagMs());
                    }
                    finalOrch.heartbeat(hbReq);

                    // Опрос Control Plane для динамического обнаружения задач и обновления топиков
                    if (finalPool != null) {
                        try {
                            List<KafkaJobItemProto> activeJobs = finalOrch.getActiveKafkaJobs(finalDcId);
                            if (activeJobs != null) {
                                List<String> dynamicTopics = activeJobs.stream()
                                        .flatMap(j -> j.getTopicsList().stream())
                                        .filter(t -> t != null && !t.isBlank())
                                        .distinct()
                                        .toList();
                                if (!dynamicTopics.isEmpty()) {
                                    finalPool.updateSubscribedTopics(dynamicTopics);
                                }

                                // Динамическое обновление лимита полосы (HTB) с учетом локального предела хоста
                                double maxBw = activeJobs.stream()
                                        .mapToDouble(KafkaJobItemProto::getBandwidthLimitMb)
                                        .filter(b -> b > 0)
                                        .max()
                                        .orElse(0.0);
                                if (maxBw > 0) {
                                    double effectiveBw = (finalMaxBandwidthMbS != null && finalMaxBandwidthMbS > 0)
                                            ? Math.min(finalMaxBandwidthMbS, maxBw)
                                            : maxBw;
                                    finalPool.setBandwidthLimitMb(effectiveBw);
                                }

                                // Динамическое обновление пула целевых агентов-приемников
                                List<String> dynamicEndpoints = activeJobs.stream()
                                        .flatMap(j -> j.getTargetEndpointsList().stream())
                                        .filter(ep -> ep != null && !ep.isBlank())
                                        .distinct()
                                        .toList();
                                if (!dynamicEndpoints.isEmpty() && innerClient != null) {
                                    innerClient.updateEndpoints(dynamicEndpoints);
                                }
                            }
                        } catch (Exception jobEx) {
                            log.debug("Не удалось обновить активные топики Kafka из оркестратора: {}", jobEx.getMessage());
                        }
                    }
                } catch (Exception e) {
                    log.debug("Ошибка отправки heartbeat в оркестратор: {}", e.getMessage());
                }
            }, 1, 3, TimeUnit.SECONDS);
        }

        final Server finalServer = server;
        final KafkaProducer<byte[], byte[]> finalProducer = producer;
        final AdminClient finalAdminClient = adminClient;
        final KafkaConsumer<byte[], byte[]> finalResolverConsumer = resolverConsumer;
        final org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool finalPool = consumerPool;
        final org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient finalClient = replicationClient;
        final OrchestratorGrpcClient finalOrchestratorClient = orchestratorClient;
        final ScheduledExecutorService finalHeartbeatExecutor = heartbeatExecutor;
        final String finalShutdownAgentId = agentId;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Остановка kafka-replicator-agent...");
            try {
                if (finalHeartbeatExecutor != null) {
                    finalHeartbeatExecutor.shutdownNow();
                }
                if (finalOrchestratorClient != null) {
                    try {
                        finalOrchestratorClient.unregister(finalShutdownAgentId);
                    } catch (Exception e) {
                        log.debug("Ошибка unregister агента: {}", e.getMessage());
                    }
                    finalOrchestratorClient.close();
                }
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
