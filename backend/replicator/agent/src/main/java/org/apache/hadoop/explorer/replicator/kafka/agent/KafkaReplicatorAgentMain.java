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
        // Перенос смещений консьюмер-групп: по умолчанию выключен, включается интервалом
        long offsetSyncIntervalSec = parseLongOrDefault(System.getenv("OFFSET_SYNC_INTERVAL_SEC"), 0);
        String consumerGroupsCsv = System.getenv("CONSUMER_GROUPS");
        // Трансляция идентификаторов схем: включается адресами обоих реестров
        String sourceSrUrl = System.getenv("SOURCE_SCHEMA_REGISTRY_URL");
        String sourceSrType = System.getenv("SOURCE_SCHEMA_REGISTRY_TYPE");
        String targetSrUrl = System.getenv("TARGET_SCHEMA_REGISTRY_URL");
        String targetSrType = System.getenv("TARGET_SCHEMA_REGISTRY_TYPE");
        boolean schemaTranscoding = "true".equalsIgnoreCase(System.getenv("SCHEMA_TRANSCODING_ENABLED"));
        String payloadFormatName = System.getenv("PAYLOAD_FORMAT");
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
            } else if ("--consumer-groups".equals(args[i]) && i + 1 < args.length) {
                consumerGroupsCsv = args[++i];
            } else if ("--offset-sync-interval-sec".equals(args[i]) && i + 1 < args.length) {
                offsetSyncIntervalSec = parseLongOrDefault(args[++i], offsetSyncIntervalSec);
            } else if ("--source-schema-registry-url".equals(args[i]) && i + 1 < args.length) {
                sourceSrUrl = args[++i];
            } else if ("--source-schema-registry-type".equals(args[i]) && i + 1 < args.length) {
                sourceSrType = args[++i];
            } else if ("--target-schema-registry-url".equals(args[i]) && i + 1 < args.length) {
                targetSrUrl = args[++i];
            } else if ("--target-schema-registry-type".equals(args[i]) && i + 1 < args.length) {
                targetSrType = args[++i];
            } else if ("--schema-transcoding".equals(args[i]) && i + 1 < args.length) {
                schemaTranscoding = "true".equalsIgnoreCase(args[++i]);
            } else if ("--payload-format".equals(args[i]) && i + 1 < args.length) {
                payloadFormatName = args[++i];
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

        // Трассировка поднимается до первых вызовов, иначе первые батчи и регистрация
        // в оркестраторе в трассу не попадут
        final var telemetry = org.apache.hadoop.explorer.replicator.telemetry.AgentTelemetry
                .startIfConfigured("kafka-replicator-agent", agentId, dcId);

        AutoCloseable cliOffsetSync = null;
        Server server = null;
        // Статус для стандартной health-пробы gRPC: нужен и при старте, и при остановке
        final var health = new org.apache.hadoop.explorer.replicator.telemetry.GrpcHealth();
        KafkaProducer<byte[], byte[]> producer = null;
        AdminClient adminClient = null;
        KafkaConsumer<byte[], byte[]> resolverConsumer = null;
        org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool consumerPool = null;
        KafkaJobPoolManager jobPoolManager = null;
        org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient replicationClient = null;

        // 1. Инициализация приемника (Receiver / Dual)
        if ("receiver".equals(role) || "dual".equals(role)) {
            Properties prodProps = org.apache.hadoop.explorer.replicator.kafka.config.KafkaClientSettings
                    .producerProps(bootstrapServers);
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(prodProps, krbConfig);

            producer = new KafkaProducer<>(prodProps);
            KafkaBatchProducer batchProducer = new KafkaBatchProducer(producer);

            Properties adminProps = new Properties();
            adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(adminProps, krbConfig);
            adminClient = AdminClient.create(adminProps);

            Properties resolverProps = org.apache.hadoop.explorer.replicator.kafka.config.KafkaClientSettings
                    .utilityConsumerProps(bootstrapServers, "kafka-replicator-time-resolver-" + dcId);
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder.applyKerberos(resolverProps, krbConfig);
            resolverConsumer = new KafkaConsumer<>(resolverProps);

            SafetyCommitManager safetyCommitManager = new SafetyCommitManager(adminClient, resolverConsumer);
            // Приемник транскодирует идентификаторы схем в реестр своего кластера. Раньше он
            // создавался вовсе без резолвера, то есть заголовки схем переносились как есть.
            org.apache.hadoop.explorer.replicator.kafka.schema.SchemaIdResolver receiverResolver =
                    org.apache.hadoop.explorer.replicator.kafka.schema.KafkaSchemaTranscoding.resolver(
                            sourceSrUrl, sourceSrType, targetSrUrl, targetSrType, krbConfig, krbConfig);
            KafkaDataTransferServiceImpl service = new KafkaDataTransferServiceImpl(
                    batchProducer, safetyCommitManager, dcId,
                    KafkaDataTransferServiceImpl.DEFAULT_PIPELINE_WINDOW,
                    receiverResolver, null, null);

            var serverBuilder = ServerBuilder.forPort(port)
                    .addService(service)
                    .addService(health.service())
                    .maxInboundMessageSize(64 * 1024 * 1024);

            if (tlsEnabled) {
                if (tlsCertChain != null && !tlsCertChain.isBlank() && tlsPrivateKey != null && !tlsPrivateKey.isBlank()) {
                    serverBuilder.useTransportSecurity(new java.io.File(tlsCertChain), new java.io.File(tlsPrivateKey));
                    log.info("Включен gRPC TLS для сервера (сертификат: {})", tlsCertChain);
                } else {
                    log.warn("TLS включен (--tls-enabled), но сертификат или приватный ключ не указаны!");
                }
            }

            // Интерцептор ставится всегда: без него приемник Kafka-батчей принимал бы
            // анонимную запись в целевые топики и коммит оффсетов чужих групп.
            serverBuilder.intercept(AgentGrpcAuthInterceptor.forServer(agentSecret));
            // Добавляется последним, поэтому выполняется первым: в трассу попадают и
            // отклоненные аутентификацией вызовы
            serverBuilder.intercept(org.apache.hadoop.explorer.replicator.telemetry.GrpcTracing.serverInterceptor());
            if (agentSecret != null && !agentSecret.isBlank()) {
                log.info("Включена gRPC L7 аутентификация сервера через x-agent-secret");
            } else {
                log.error("x-agent-secret не задан: приемник Kafka-батчей запущен в режиме Fail-Closed, "
                        + "все входящие вызовы будут отклоняться");
            }

            server = serverBuilder.build().start();
            health.serving();
            health.serving("replicator.kafka.KafkaDataTransferService");

            log.info("gRPC сервер kafka-replicator-agent успешно запущен на порту {}", port);
        }

        // 2. Инициализация отправителя (Sender / Dual)
        // Статический пул нужен только топикам, заданным в командной строке. Задачи Control
        // Plane обслуживает KafkaJobPoolManager своими пулами: пул без --topics, который
        // переподписывался на топики задач, читал их второй consumer-группой, и каждая запись
        // уходила в целевой кластер дважды, а после снятия задачи продолжал репликацию.
        boolean hasCliTopics = !topics.isBlank();
        if (("sender".equals(role) || "dual".equals(role)) && hasCliTopics) {
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
                Properties consProps = org.apache.hadoop.explorer.replicator.kafka.config.KafkaClientSettings
                        .consumerProps(bootstrapServers, groupId, batchSize);
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
            org.apache.hadoop.explorer.replicator.kafka.schema.SchemaIdResolver schemaResolver =
                    org.apache.hadoop.explorer.replicator.kafka.schema.KafkaSchemaTranscoding.resolver(
                            sourceSrUrl, sourceSrType, targetSrUrl, targetSrType, krbConfig, krbConfig);
            org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder transcoder =
                    new org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder(schemaResolver);
            org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat wireFormat =
                    org.apache.hadoop.explorer.replicator.kafka.schema.KafkaSchemaTranscoding
                            .format(payloadFormatName, schemaTranscoding);

            consumerPool = new org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool(
                    consumers,
                    replicationClient,
                    loopFilter,
                    classifier,
                    transcoder,
                    dcId,
                    targetDcId,
                    wireFormat,
                    wireFormat
            );
            consumerPool.setMaxBatchSize(batchSize);
            if (maxBandwidthMbS != null && maxBandwidthMbS > 0) {
                consumerPool.setBandwidthLimitMb(maxBandwidthMbS);
                log.info("Установлен локальный лимит полосы пропускания агента: {} МБ/с", maxBandwidthMbS);
            }
            consumerPool.start();
            log.info("Пул консьюмеров отправителя успешно запущен");

            cliOffsetSync = startOffsetSync(bootstrapServers, krbConfig, replicationClient, consumerPool,
                    parseCsv(consumerGroupsCsv), dcId, targetDcId, offsetSyncIntervalSec);
        }

        // 3. Подключение к Orchestrator Control Plane
        OrchestratorGrpcClient orchestratorClient = null;
        ScheduledExecutorService heartbeatExecutor = null;

        if (orchestratorGrpc != null && !orchestratorGrpc.isBlank()) {
            // Задачи, приходящие из Control Plane, обслуживаются отдельными пулами:
            // своя consumer-группа на задачу вместо общей группы на весь агент
            jobPoolManager = new KafkaJobPoolManager(dcId, jobPoolFactory(
                    bootstrapServers, krbConfig, agentSecret, tlsEnabled, tlsTrustCert, maxBandwidthMbS,
                    sourceSrUrl, sourceSrType, targetSrUrl, targetSrType, payloadFormatName));
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
            final KafkaJobPoolManager finalJobPools = jobPoolManager;
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
                    // Метрики агента — сумма по всем его пулам: статическому и пулам задач
                    long messages = finalJobPools != null ? finalJobPools.totalMessagesReplicated() : 0L;
                    long bytes = finalJobPools != null ? finalJobPools.totalBytesReplicated() : 0L;
                    long lagMs = finalJobPools != null ? finalJobPools.maxLagMs() : 0L;
                    if (finalPool != null) {
                        messages += finalPool.getTotalMessagesReplicated();
                        bytes += finalPool.getTotalBytesReplicated();
                        lagMs = Math.max(lagMs, finalPool.getCurrentLagMs());
                    }
                    hbReq.setKafkaMessagesReplicated(messages);
                    hbReq.setKafkaBytesReplicated(bytes);
                    hbReq.setKafkaCurrentLagMs(lagMs);
                    if (finalJobPools != null) {
                        hbReq.setKafkaJobStatsSupported(true);
                        hbReq.setKafkaJobStats(finalJobPools.jobStats());
                    }
                    finalOrch.heartbeat(hbReq);

                    // Опрос Control Plane: состав задач отражается на составе пулов
                    try {
                        // null — оркестратор не ответил: пулы продолжают работать как есть,
                        // останавливает их только успешный ответ без задачи
                        List<KafkaJobItemProto> activeJobs = finalOrch.getActiveKafkaJobs(finalDcId);
                        if (activeJobs != null && finalJobPools != null) {
                            finalJobPools.sync(activeJobs);
                        }
                    } catch (Exception jobEx) {
                        log.debug("Не удалось синхронизировать задачи Kafka из оркестратора: {}", jobEx.getMessage());
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
        final KafkaJobPoolManager finalJobPoolsForShutdown = jobPoolManager;
        final AutoCloseable finalCliOffsetSync = cliOffsetSync;

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
                if (finalJobPoolsForShutdown != null) {
                    finalJobPoolsForShutdown.close();
                }
                if (finalPool != null) {
                    finalPool.close();
                }
                if (finalClient != null) {
                    finalClient.close();
                }
                if (finalServer != null) {
                    // Проба обязана узнать об остановке до закрытия порта, иначе отправители
                    // продолжат отсылать батчи на останавливающийся узел
                    health.shutdown();
                    finalServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                }
                if (finalProducer != null) {
                    finalProducer.close();
                }
                if (finalCliOffsetSync != null) {
                    finalCliOffsetSync.close();
                }
                if (telemetry != null) {
                    telemetry.close();
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


    private static String firstNonBlank(String preferred, String fallback) {
        return (preferred != null && !preferred.isBlank()) ? preferred : fallback;
    }

    private static long parseLongOrDefault(String value, long fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static List<String> parseCsv(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return java.util.Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(part -> !part.isEmpty())
                .toList();
    }

    /**
     * Запуск переноса смещений консьюмер-групп для одного пула.
     *
     * <p>Возвращает {@code null}, когда перенос выключен (интервал не задан) или переносить
     * нечего (группы не указаны) — это осознанный выбор оператора, а не сбой. Сканеру нужен свой
     * AdminClient источника, поэтому он закрывается вместе с переносом.
     */
    private static AutoCloseable startOffsetSync(
            String bootstrapServers,
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig krbConfig,
            org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient client,
            org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool pool,
            List<String> groups,
            String sourceDc,
            String targetDc,
            long intervalSeconds) {

        if (intervalSeconds <= 0 || groups == null || groups.isEmpty()) {
            if (intervalSeconds > 0) {
                log.info("Перенос смещений включен, но список консьюмер-групп пуст: переносить нечего");
            }
            return null;
        }

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder
                .applyKerberos(adminProps, krbConfig);
        AdminClient scannerAdmin = AdminClient.create(adminProps);

        org.apache.hadoop.explorer.replicator.kafka.offset.ConsumerGroupOffsetScanner scanner =
                new org.apache.hadoop.explorer.replicator.kafka.offset.ConsumerGroupOffsetScanner(
                        scannerAdmin, sourceDc, targetDc);

        org.apache.hadoop.explorer.replicator.kafka.offset.ConsumerGroupOffsetSynchronizer synchronizer =
                new org.apache.hadoop.explorer.replicator.kafka.offset.ConsumerGroupOffsetSynchronizer(
                        scanner,
                        client,
                        () -> groups,
                        () -> java.util.Set.copyOf(pool.getSubscribedTopics()),
                        new org.apache.hadoop.explorer.replicator.kafka.offset
                                .ConsumerGroupOffsetSynchronizer.ReplicationProgress() {
                            @Override
                            public java.util.Map<org.apache.kafka.common.TopicPartition, Long> replicatedOffsets() {
                                return pool.getConfirmedOffsets();
                            }

                            @Override
                            public java.util.Map<org.apache.kafka.common.TopicPartition, Long> lastTimestamps() {
                                return pool.getLastObservedTimestamps();
                            }
                        },
                        10);
        synchronizer.start(intervalSeconds);

        return () -> {
            synchronizer.close();
            try {
                scannerAdmin.close(java.time.Duration.ofSeconds(2));
            } catch (Exception ignored) {
                // закрытие вспомогательного клиента не должно мешать остановке демона
            }
        };
    }

    /**
     * Фабрика пулов задач: у каждой задачи свой консьюмер-пул, своя consumer-группа,
     * свой клиент приемников и свой peer-DC для фильтра петель.
     */
    private static java.util.function.Function<KafkaJobPoolManager.JobPoolSpec,
            KafkaJobPoolManager.JobPipeline> jobPoolFactory(
            String bootstrapServers,
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig krbConfig,
            String agentSecret,
            boolean tlsEnabled,
            String tlsTrustCert,
            Double hostBandwidthLimitMbS,
            String sourceSrUrl,
            String sourceSrType,
            String targetSrUrl,
            String targetSrType,
            String payloadFormatName) {
        return spec -> {
            org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient client =
                    new org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient(
                            spec.targetEndpoints(), agentSecret, tlsEnabled, tlsTrustCert);

            List<KafkaConsumer<byte[], byte[]>> consumers = new java.util.ArrayList<>();
            for (int i = 0; i < spec.consumerThreads(); i++) {
                Properties props = org.apache.hadoop.explorer.replicator.kafka.config.KafkaClientSettings
                        .consumerProps(bootstrapServers, spec.groupId(), spec.batchSize());
                props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        org.apache.hadoop.explorer.replicator.kafka.config.KafkaClientSettings
                                .offsetResetFor(spec.startPosition()));
                org.apache.hadoop.explorer.replicator.kafka.security.KafkaSecurityConfigBuilder
                        .applyKerberos(props, krbConfig);
                KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(spec.topics());
                consumers.add(consumer);
            }

            // Транскодинг схем и формат — настройки самой задачи: интерфейс задает их на задачу,
            // а адреса реестров остаются конфигурацией узла
            // Реестры приходят от оркестратора из топологии выбранных кластеров; переменные
            // окружения узла остаются запасным вариантом для топиков из командной строки
            String jobSourceSrUrl = firstNonBlank(spec.sourceSchemaRegistryUrl(), sourceSrUrl);
            String jobSourceSrType = firstNonBlank(spec.sourceSchemaRegistryType(), sourceSrType);
            String jobTargetSrUrl = firstNonBlank(spec.targetSchemaRegistryUrl(), targetSrUrl);
            String jobTargetSrType = firstNonBlank(spec.targetSchemaRegistryType(), targetSrType);

            org.apache.hadoop.explorer.replicator.kafka.schema.SchemaIdResolver schemaResolver =
                    spec.schemaTranscoding()
                            ? org.apache.hadoop.explorer.replicator.kafka.schema.KafkaSchemaTranscoding.resolver(
                                    jobSourceSrUrl, jobSourceSrType, jobTargetSrUrl, jobTargetSrType,
                                    krbConfig, krbConfig)
                            : org.apache.hadoop.explorer.replicator.kafka.schema.KafkaSchemaTranscoding
                                    .passthroughResolver();
            org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat wireFormat =
                    org.apache.hadoop.explorer.replicator.kafka.schema.KafkaSchemaTranscoding
                            .format(payloadFormatName, spec.schemaTranscoding());

            org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool pool =
                    new org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool(
                            consumers,
                            client,
                            new org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter(
                                    spec.sourceDc(), spec.targetDc()),
                            new org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier(),
                            new org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder(schemaResolver),
                            spec.sourceDc(),
                            spec.targetDc(),
                            wireFormat,
                            wireFormat);
            pool.setMaxBatchSize(spec.batchSize());
            pool.updateSubscribedTopics(spec.topics());

            // Потолок узла и доля задачи хранятся раздельно: доля меняется на живом пуле, а
            // потолок узла должен действовать и после ее смены, и при ее отсутствии
            if (hostBandwidthLimitMbS != null && hostBandwidthLimitMbS > 0) {
                pool.setHostBandwidthCapMb(hostBandwidthLimitMbS);
            }
            pool.setBandwidthLimitMb(spec.bandwidthLimitMb());

            // Перенос смещений групп этой задачи: свои группы, свой целевой кластер, свой клиент
            AutoCloseable offsetSync = startOffsetSync(bootstrapServers, krbConfig, client, pool,
                    spec.consumerGroups(), spec.sourceDc(), spec.targetDc(),
                    spec.offsetSyncIntervalSec());

            return new KafkaJobPoolManager.JobPipeline(pool, offsetSync);
        };
    }
}
