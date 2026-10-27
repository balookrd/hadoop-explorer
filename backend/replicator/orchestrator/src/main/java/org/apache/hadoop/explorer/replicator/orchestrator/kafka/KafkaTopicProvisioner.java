package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Сервис предварительной проверки и автоматического создания топиков Kafka на целевом брокере
 * с полным сохранением топологии партиционирования (количество партиций, фактор репликации).
 */
@Component
public class KafkaTopicProvisioner {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicProvisioner.class);

    private final KafkaAdminClientFactory adminClientFactory;

    public KafkaTopicProvisioner() {
        this(new DefaultKafkaAdminClientFactory());
    }

    public KafkaTopicProvisioner(KafkaAdminClientFactory adminClientFactory) {
        this.adminClientFactory = adminClientFactory != null ? adminClientFactory : new DefaultKafkaAdminClientFactory();
    }

    /**
     * Белый список критических и безопасных параметров топика Kafka,
     * которые необходимо переносить при автоматическом создании топика на целевом кластере.
     */
    private static final Set<String> REPLICATED_CONFIG_KEYS = Set.of(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.COMPRESSION_TYPE_CONFIG,
            TopicConfig.DELETE_RETENTION_MS_CONFIG,
            TopicConfig.FILE_DELETE_DELAY_MS_CONFIG,
            TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG,
            TopicConfig.FLUSH_MS_CONFIG,
            TopicConfig.INDEX_INTERVAL_BYTES_CONFIG,
            TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG,
            TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
            TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG,
            TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG,
            TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG,
            TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,
            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
            TopicConfig.PREALLOCATE_CONFIG,
            TopicConfig.RETENTION_BYTES_CONFIG,
            TopicConfig.RETENTION_MS_CONFIG,
            TopicConfig.SEGMENT_BYTES_CONFIG,
            TopicConfig.SEGMENT_INDEX_BYTES_CONFIG,
            TopicConfig.SEGMENT_JITTER_MS_CONFIG,
            TopicConfig.SEGMENT_MS_CONFIG,
            TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG
    );

    /**
     * Извлекает из конфигурации исходного топика параметры, подлежащие репликации.
     */
    private Map<String, String> extractReplicableConfigs(Config topicConfig) {
        if (topicConfig == null) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (ConfigEntry entry : topicConfig.entries()) {
            if (entry.value() == null || entry.value().isBlank() || entry.isReadOnly()) {
                continue;
            }
            boolean isDynamic = entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;
            boolean isWhitelisted = REPLICATED_CONFIG_KEYS.contains(entry.name());
            if (isDynamic || (isWhitelisted && !entry.isDefault())) {
                result.put(entry.name(), entry.value());
            }
        }
        return result;
    }

    /**
     * Выполняет предварительную проверку и автосоздание топиков на целевом брокере.
     *
     * @param sourceBootstrapServers адрес исходного Kafka-кластера
     * @param sourceKerberosEnabled  флаг Kerberos для источника
     * @param sourcePrincipal        Kerberos principal источника
     * @param sourceKeytabPath       путь к keytab источника
     * @param sourceServiceName      имя сервиса Kerberos источника
     * @param targetBootstrapServers адрес целевого Kafka-кластера
     * @param targetKerberosEnabled  флаг Kerberos для целевого кластера
     * @param targetPrincipal        Kerberos principal целевого кластера
     * @param targetKeytabPath       путь к keytab целевого кластера
     * @param targetServiceName      имя сервиса Kerberos целевого кластера
     * @param topicNames             список имен топиков для синхронизации
     * @param failOnError            выбрасывать ли исключение при недоступности брокеров
     * @return результат проверки и создания топиков
     */
    public TopicProvisioningResult provisionTopics(
            String sourceBootstrapServers,
            boolean sourceKerberosEnabled,
            String sourcePrincipal,
            String sourceKeytabPath,
            String sourceServiceName,
            String targetBootstrapServers,
            boolean targetKerberosEnabled,
            String targetPrincipal,
            String targetKeytabPath,
            String targetServiceName,
            List<String> topicNames,
            boolean failOnError
    ) {
        if (topicNames == null || topicNames.isEmpty()) {
            return TopicProvisioningResult.empty();
        }

        List<String> uniqueTopics = topicNames.stream()
                .filter(t -> t != null && !t.isBlank())
                .distinct()
                .toList();

        if (uniqueTopics.isEmpty()) {
            return TopicProvisioningResult.empty();
        }

        List<String> createdTopics = new ArrayList<>();
        List<String> expandedTopics = new ArrayList<>();
        List<String> matchedTopics = new ArrayList<>();

        log.info("Запуск предварительной проверки и автосоздания {} топиков [{} -> {}]",
                uniqueTopics.size(), sourceBootstrapServers, targetBootstrapServers);

        AdminClient sourceAdmin = null;
        AdminClient targetAdmin = null;
        try {
            sourceAdmin = adminClientFactory.createAdminClient(
                    sourceBootstrapServers, sourceKerberosEnabled, sourcePrincipal, sourceKeytabPath, sourceServiceName);
            targetAdmin = adminClientFactory.createAdminClient(
                    targetBootstrapServers, targetKerberosEnabled, targetPrincipal, targetKeytabPath, targetServiceName);

            // 1. Получаем описание топологии топиков на источнике
            Map<String, TopicDescription> sourceDescriptions =
                    sourceAdmin.describeTopics(uniqueTopics).allTopicNames().get(8, TimeUnit.SECONDS);

            // 1.1 Получаем детальные конфигурации топиков на источнике (cleanup.policy, retention, min.insync.replicas и др.)
            Map<ConfigResource, Config> sourceConfigs = Collections.emptyMap();
            try {
                List<ConfigResource> configResources = uniqueTopics.stream()
                        .map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t))
                        .toList();
                DescribeConfigsResult descConfigsResult = sourceAdmin.describeConfigs(configResources);
                if (descConfigsResult != null && descConfigsResult.all() != null) {
                    sourceConfigs = descConfigsResult.all().get(8, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                log.warn("Не удалось получить конфигурации топиков с исходного брокера (будут использованы дефолтные параметры): {}", e.getMessage());
            }

            // 2. Получаем список существующих топиков на целевом брокере
            Set<String> existingTargetTopics =
                    targetAdmin.listTopics(new ListTopicsOptions().listInternal(false)).names().get(8, TimeUnit.SECONDS);

            List<NewTopic> toCreate = new ArrayList<>();
            Map<String, NewPartitions> toExpand = new HashMap<>();

            for (String topic : uniqueTopics) {
                TopicDescription srcDesc = sourceDescriptions.get(topic);
                if (srcDesc == null) {
                    log.warn("Топик '{}' не обнаружен на исходном брокере [{}]", topic, sourceBootstrapServers);
                    continue;
                }

                int srcPartitions = srcDesc.partitions().size();
                short replicationFactor = (short) Math.max(1,
                        srcDesc.partitions().isEmpty() ? 1 : srcDesc.partitions().get(0).replicas().size());

                if (!existingTargetTopics.contains(topic)) {
                    // Топик отсутствует на целевом кластере — создаем с идентичными параметрами и конфигурацией
                    NewTopic newTopic = new NewTopic(topic, srcPartitions, replicationFactor);
                    Config srcCfg = sourceConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, topic));
                    Map<String, String> topicConfigsToApply = extractReplicableConfigs(srcCfg);
                    if (!topicConfigsToApply.isEmpty()) {
                        newTopic.configs(topicConfigsToApply);
                    }
                    toCreate.add(newTopic);
                    String configSummary = topicConfigsToApply.isEmpty() ? "" : String.format(", configs: %s", topicConfigsToApply.keySet());
                    createdTopics.add(String.format("%s (партиций: %d, RF: %d%s)", topic, srcPartitions, replicationFactor, configSummary));
                } else {
                    // Топик уже существует на целевом кластере — проверяем количество партиций
                    TopicDescription tgtDesc = targetAdmin.describeTopics(List.of(topic))
                            .allTopicNames().get(8, TimeUnit.SECONDS).get(topic);
                    if (tgtDesc != null) {
                        int tgtPartitions = tgtDesc.partitions().size();
                        if (tgtPartitions < srcPartitions) {
                            // Расширяем партиции до исходного значения
                            toExpand.put(topic, NewPartitions.increaseTo(srcPartitions));
                            expandedTopics.add(String.format("%s (партиций увеличено: %d -> %d)", topic, tgtPartitions, srcPartitions));
                        } else {
                            matchedTopics.add(String.format("%s (партиций: %d)", topic, tgtPartitions));
                        }
                    }
                }
            }

            // 3. Создаем отсутствующие топики на целевом брокере
            if (!toCreate.isEmpty()) {
                targetAdmin.createTopics(toCreate).all().get(10, TimeUnit.SECONDS);
                log.info("Успешно созданы {} топиков на целевом брокере [{}]: {}",
                        toCreate.size(), targetBootstrapServers, createdTopics);
            }

            // 4. Расширяем партиции при необходимости
            if (!toExpand.isEmpty()) {
                targetAdmin.createPartitions(toExpand).all().get(10, TimeUnit.SECONDS);
                log.info("Успешно расширены партиции для {} топиков на целевом брокере [{}]: {}",
                        toExpand.size(), targetBootstrapServers, expandedTopics);
            }

            return new TopicProvisioningResult(
                    createdTopics, expandedTopics, matchedTopics, true, "Топология топиков успешно проверена и синхронизирована");

        } catch (Exception e) {
            // У TimeoutException и ExecutionException сообщение пустое: причина в цепочке
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            String reason = root.getMessage() != null ? root.getMessage() : root.getClass().getSimpleName();
            String errorMsg = "Автопроверка/создание топиков Kafka завершились с предупреждением: " + reason;
            log.warn(errorMsg);
            if (failOnError) {
                throw new RuntimeException(errorMsg, e);
            }
            return new TopicProvisioningResult(createdTopics, expandedTopics, matchedTopics, false, errorMsg);
        } finally {
            closeQuietly(sourceAdmin);
            closeQuietly(targetAdmin);
        }
    }

    private void closeQuietly(AdminClient client) {
        if (client != null) {
            try {
                client.close(java.time.Duration.ofSeconds(2));
            } catch (Exception ignored) {
            }
        }
    }
}
