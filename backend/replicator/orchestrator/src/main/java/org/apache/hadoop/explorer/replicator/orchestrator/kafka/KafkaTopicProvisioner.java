package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.kafka.clients.admin.*;
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
                    // Топик отсутствует на целевом кластере — создаем с идентичными параметрами
                    toCreate.add(new NewTopic(topic, srcPartitions, replicationFactor));
                    createdTopics.add(String.format("%s (партиций: %d, RF: %d)", topic, srcPartitions, replicationFactor));
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
            String errorMsg = "Автопроверка/создание топиков Kafka завершились с предупреждением: " + e.getMessage();
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
