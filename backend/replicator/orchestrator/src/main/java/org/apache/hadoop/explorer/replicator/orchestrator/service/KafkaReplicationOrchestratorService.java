package org.apache.hadoop.explorer.replicator.orchestrator.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.kafka.KafkaTopicProvisioner;
import org.apache.hadoop.explorer.replicator.orchestrator.kafka.TopicProvisioningResult;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaReplicationOrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(KafkaReplicationOrchestratorService.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final KafkaReplicationJobRepository repository;
    private final KafkaTopicProvisioner topicProvisioner;
    private final org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry agentRegistry;
    private final org.apache.hadoop.explorer.replicator.orchestrator.kafka.KafkaAdminClientFactory adminClientFactory;

    @org.springframework.beans.factory.annotation.Autowired
    public KafkaReplicationOrchestratorService(KafkaReplicationJobRepository repository,
                                               KafkaTopicProvisioner topicProvisioner,
                                               @org.springframework.beans.factory.annotation.Autowired(required = false)
                                               org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry agentRegistry,
                                               @org.springframework.beans.factory.annotation.Autowired(required = false)
                                               org.apache.hadoop.explorer.replicator.orchestrator.kafka.KafkaAdminClientFactory adminClientFactory) {
        this.repository = repository;
        this.topicProvisioner = topicProvisioner != null ? topicProvisioner : new KafkaTopicProvisioner();
        this.agentRegistry = agentRegistry;
        this.adminClientFactory = adminClientFactory != null ? adminClientFactory : new org.apache.hadoop.explorer.replicator.orchestrator.kafka.DefaultKafkaAdminClientFactory();
    }

    public KafkaReplicationOrchestratorService(KafkaReplicationJobRepository repository,
                                               KafkaTopicProvisioner topicProvisioner,
                                               org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry agentRegistry) {
        this(repository, topicProvisioner, agentRegistry, new org.apache.hadoop.explorer.replicator.orchestrator.kafka.DefaultKafkaAdminClientFactory());
    }

    public KafkaReplicationOrchestratorService(KafkaReplicationJobRepository repository,
                                               KafkaTopicProvisioner topicProvisioner) {
        this(repository, topicProvisioner, null, new org.apache.hadoop.explorer.replicator.orchestrator.kafka.DefaultKafkaAdminClientFactory());
    }

    public KafkaReplicationOrchestratorService(KafkaReplicationJobRepository repository) {
        this(repository, new KafkaTopicProvisioner(), null, new org.apache.hadoop.explorer.replicator.orchestrator.kafka.DefaultKafkaAdminClientFactory());
    }

    @Transactional(readOnly = true)
    public List<KafkaReplicationJobDto> listAllJobs() {
        return repository.findAll().stream()
                .map(this::toDto)
                .toList();
    }

    @Transactional(readOnly = true)
    public Optional<KafkaReplicationJobDto> getJobById(String id) {
        return repository.findById(id).map(this::toDto);
    }

    /**
     * Возвращает список активных задач репликации Kafka для указанного ДЦ.
     */
    @Transactional(readOnly = true)
    public List<KafkaReplicationJobDto> getActiveJobsForDc(String dcId) {
        if (dcId == null || dcId.isBlank()) {
            return Collections.emptyList();
        }
        String dc = dcId.trim();
        return repository.findAll().stream()
                .filter(job -> "ACTIVE".equalsIgnoreCase(job.getStatus()))
                .filter(job -> dc.equalsIgnoreCase(job.getSourceDc()) ||
                        ("BIDIRECTIONAL".equalsIgnoreCase(job.getDirection()) && dc.equalsIgnoreCase(job.getTargetDc())))
                .map(this::toDto)
                .toList();
    }

    @Transactional
    public KafkaReplicationJobDto createJob(CreateKafkaJobRequest request) {
        String id = "kafka-job-" + UUID.randomUUID().toString().substring(0, 8);
        KafkaReplicationJobEntity entity = new KafkaReplicationJobEntity();
        entity.setId(id);
        entity.setName(request.getName() != null && !request.getName().isBlank() ? request.getName() : id);
        entity.setSourceDc(request.getSourceDc());
        entity.setTargetDc(request.getTargetDc());
        entity.setDirection(request.getDirection() != null ? request.getDirection() : "BIDIRECTIONAL");
        entity.setBandwidthLimitMb(request.getBandwidthLimitMb());
        entity.setOffsetSyncIntervalSec(request.getOffsetSyncIntervalSec());
        entity.setConsumerThreads(request.getConsumerThreads() != null ? request.getConsumerThreads() : 8);
        entity.setBatchSize(request.getBatchSize() != null ? request.getBatchSize() : 2000);
        entity.setSchemaTranscodingEnabled(request.isSchemaTranscodingEnabled());
        entity.setSourceSrType(request.getSourceSrType());
        entity.setTargetSrType(request.getTargetSrType());
        entity.setSourceSrUrl(request.getSourceSrUrl());
        entity.setTargetSrUrl(request.getTargetSrUrl());
        entity.setSourceSrKerberosEnabled(request.isSourceSrKerberosEnabled());
        entity.setTargetSrKerberosEnabled(request.isTargetSrKerberosEnabled());
        entity.setStatus("ACTIVE");

        // Kerberos
        entity.setSourceKerberosEnabled(request.isSourceKerberosEnabled());
        entity.setSourcePrincipal(request.getSourcePrincipal());
        entity.setSourceKeytabPath(request.getSourceKeytabPath());
        entity.setSourceServiceName(request.getSourceServiceName() != null ? request.getSourceServiceName() : "kafka");

        entity.setTargetKerberosEnabled(request.isTargetKerberosEnabled());
        entity.setTargetPrincipal(request.getTargetPrincipal());
        entity.setTargetKeytabPath(request.getTargetKeytabPath());
        entity.setTargetServiceName(request.getTargetServiceName() != null ? request.getTargetServiceName() : "kafka");

        // Bootstrap servers & Auto-create topics
        String sourceBootstrap = resolveBootstrapServers(request.getSourceDc(), request.getSourceBootstrapServers());
        String targetBootstrap = resolveBootstrapServers(request.getTargetDc(), request.getTargetBootstrapServers());
        entity.setSourceBootstrapServers(sourceBootstrap);
        entity.setTargetBootstrapServers(targetBootstrap);
        entity.setAutoCreateTopics(request.isAutoCreateTopics());

        try {
            entity.setTopicsJson(mapper.writeValueAsString(request.getTopics() != null ? request.getTopics() : Collections.emptyList()));
            entity.setConsumerGroupsJson(mapper.writeValueAsString(request.getConsumerGroups() != null ? request.getConsumerGroups() : Collections.emptyList()));
            if (request.getTargetEndpoints() != null && !request.getTargetEndpoints().isEmpty()) {
                entity.setTargetEndpointsJson(mapper.writeValueAsString(request.getTargetEndpoints()));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize topics or consumer groups to JSON", e);
        }

        // Автоматическая предварительная проверка и создание топиков на целевом брокере
        if (request.isAutoCreateTopics() && request.getTopics() != null && !request.getTopics().isEmpty()) {
            List<String> topicNames = request.getTopics().stream()
                    .map(CreateKafkaJobRequest.TopicSetting::getTopic)
                    .filter(Objects::nonNull)
                    .toList();

            TopicProvisioningResult forwardResult = topicProvisioner.provisionTopics(
                    sourceBootstrap,
                    request.isSourceKerberosEnabled(),
                    request.getSourcePrincipal(),
                    request.getSourceKeytabPath(),
                    request.getSourceServiceName(),
                    targetBootstrap,
                    request.isTargetKerberosEnabled(),
                    request.getTargetPrincipal(),
                    request.getTargetKeytabPath(),
                    request.getTargetServiceName(),
                    topicNames,
                    false
            );

            String summary = forwardResult.toSummaryString();

            // При двунаправленной репликации проверяем топики и в обратную сторону
            if ("BIDIRECTIONAL".equalsIgnoreCase(entity.getDirection())) {
                TopicProvisioningResult reverseResult = topicProvisioner.provisionTopics(
                        targetBootstrap,
                        request.isTargetKerberosEnabled(),
                        request.getTargetPrincipal(),
                        request.getTargetKeytabPath(),
                        request.getTargetServiceName(),
                        sourceBootstrap,
                        request.isSourceKerberosEnabled(),
                        request.getSourcePrincipal(),
                        request.getSourceKeytabPath(),
                        request.getSourceServiceName(),
                        topicNames,
                        false
                );
                if (!reverseResult.createdTopics().isEmpty() || !reverseResult.expandedTopics().isEmpty()) {
                    summary = summary + " [Обратная репликация: " + reverseResult.toSummaryString() + "]";
                }
            }
            entity.setTopicProvisioningSummary(summary);
        }

        KafkaReplicationJobEntity saved = repository.save(entity);
        log.info("Создана новая задача репликации Kafka '{}' ({}) [{} <-> {}], провижининг топиков: {}",
                saved.getName(), saved.getId(), saved.getSourceDc(), saved.getTargetDc(), saved.getTopicProvisioningSummary());
        return toDto(saved);
    }

    @Transactional
    public boolean pauseJob(String id) {
        return repository.findById(id).map(job -> {
            job.setStatus("PAUSED");
            repository.save(job);
            log.info("Задача репликации Kafka '{}' приостановлена", id);
            return true;
        }).orElse(false);
    }

    @Transactional
    public boolean resumeJob(String id) {
        return repository.findById(id).map(job -> {
            job.setStatus("ACTIVE");
            repository.save(job);
            log.info("Задача репликации Kafka '{}' возобновлена", id);
            return true;
        }).orElse(false);
    }

    @Transactional
    public boolean deleteJob(String id) {
        if (repository.existsById(id)) {
            repository.deleteById(id);
            log.info("Задача репликации Kafka '{}' удалена", id);
            return true;
        }
        return false;
    }

    @Transactional
    public boolean triggerFailover(String id) {
        return repository.findById(id).map(job -> {
            log.warn("Инициирован принудительный Failover консьюмеров для задачи Kafka '{}'", id);
            job.setLastErrorMessage("Failover triggered at " + java.time.Instant.now());
            repository.save(job);
            return true;
        }).orElse(false);
    }

    private KafkaReplicationJobDto toDto(KafkaReplicationJobEntity entity) {
        KafkaReplicationJobDto dto = new KafkaReplicationJobDto();
        dto.setId(entity.getId());
        dto.setName(entity.getName());
        dto.setSourceDc(entity.getSourceDc());
        dto.setTargetDc(entity.getTargetDc());
        dto.setDirection(entity.getDirection());
        dto.setBandwidthLimitMb(entity.getBandwidthLimitMb());
        dto.setOffsetSyncIntervalSec(entity.getOffsetSyncIntervalSec());
        dto.setConsumerThreads(entity.getConsumerThreads());
        dto.setBatchSize(entity.getBatchSize());
        dto.setSchemaTranscodingEnabled(entity.isSchemaTranscodingEnabled());
        dto.setSourceSrType(entity.getSourceSrType());
        dto.setTargetSrType(entity.getTargetSrType());
        dto.setSourceSrUrl(entity.getSourceSrUrl());
        dto.setTargetSrUrl(entity.getTargetSrUrl());
        dto.setSourceSrKerberosEnabled(entity.isSourceSrKerberosEnabled());
        dto.setTargetSrKerberosEnabled(entity.isTargetSrKerberosEnabled());
        dto.setStatus(entity.getStatus());

        // Kerberos
        dto.setSourceKerberosEnabled(entity.isSourceKerberosEnabled());
        dto.setSourcePrincipal(entity.getSourcePrincipal());
        dto.setSourceKeytabPath(entity.getSourceKeytabPath());
        dto.setSourceServiceName(entity.getSourceServiceName());

        dto.setTargetKerberosEnabled(entity.isTargetKerberosEnabled());
        dto.setTargetPrincipal(entity.getTargetPrincipal());
        dto.setTargetKeytabPath(entity.getTargetKeytabPath());
        dto.setTargetServiceName(entity.getTargetServiceName());

        dto.setMessagesReplicated(entity.getMessagesReplicated());
        dto.setBytesReplicated(entity.getBytesReplicated());
        dto.setCurrentLagMs(entity.getCurrentLagMs());
        dto.setLastErrorMessage(entity.getLastErrorMessage());
        dto.setCreatedAt(entity.getCreatedAt());
        dto.setUpdatedAt(entity.getUpdatedAt());

        dto.setSourceBootstrapServers(entity.getSourceBootstrapServers());
        dto.setTargetBootstrapServers(entity.getTargetBootstrapServers());
        dto.setAutoCreateTopics(entity.isAutoCreateTopics());
        dto.setTopicProvisioningSummary(entity.getTopicProvisioningSummary());

        try {
            if (entity.getTopicsJson() != null) {
                dto.setTopics(mapper.readValue(entity.getTopicsJson(), new TypeReference<>() {}));
            }
            if (entity.getConsumerGroupsJson() != null) {
                dto.setConsumerGroups(mapper.readValue(entity.getConsumerGroupsJson(), new TypeReference<>() {}));
            }
            if (entity.getTargetEndpointsJson() != null) {
                dto.setTargetEndpoints(mapper.readValue(entity.getTargetEndpointsJson(), new TypeReference<>() {}));
            }
        } catch (Exception e) {
            log.warn("Не удалось распарсить JSON топиков/групп для задачи {}: {}", entity.getId(), e.getMessage());
        }

        return dto;
    }

    /**
     * Возвращает список адресов активных агентов-приемников для целевого кластера/ДЦ.
     */
    public List<String> getLiveReceiverEndpoints(String clusterId) {
        if (agentRegistry == null) {
            return List.of();
        }
        var receivers = agentRegistry.getLiveKafkaReceiversForCluster(clusterId);
        return receivers.stream()
                .map(org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry.AgentEntry::getGrpcAddress)
                .toList();
    }

    /**
     * Разрешает адрес брокеров Kafka по идентификатору ДЦ или явно переданному адресу.
     */
    public String resolveBootstrapServers(String dc, String explicitServers) {
        if (explicitServers != null && !explicitServers.isBlank()) {
            return explicitServers.trim();
        }
        if (dc == null || dc.isBlank()) {
            return "localhost:9092";
        }
        String dId = dc.trim().toLowerCase();
        if ("dc1".equals(dId)) {
            return "kafka-dc1:9092";
        } else if ("dc2".equals(dId)) {
            return "kafka-dc2:9092";
        }
        return dId + "-kafka:9092";
    }

    /**
     * Повторный или ручной запуск предварительной проверки и автосоздания топиков для существующей задачи.
     */
    @Transactional
    public TopicProvisioningResult provisionTopicsForJob(String jobId) {
        return repository.findById(jobId).map(entity -> {
            List<String> topics = Collections.emptyList();
            try {
                if (entity.getTopicsJson() != null) {
                    List<CreateKafkaJobRequest.TopicSetting> settings =
                            mapper.readValue(entity.getTopicsJson(), new TypeReference<>() {});
                    topics = settings.stream()
                            .map(CreateKafkaJobRequest.TopicSetting::getTopic)
                            .filter(Objects::nonNull)
                            .toList();
                }
            } catch (Exception e) {
                log.warn("Не удалось прочитать JSON топиков для задачи {}: {}", jobId, e.getMessage());
            }

            TopicProvisioningResult res = topicProvisioner.provisionTopics(
                    entity.getSourceBootstrapServers(),
                    entity.isSourceKerberosEnabled(),
                    entity.getSourcePrincipal(),
                    entity.getSourceKeytabPath(),
                    entity.getSourceServiceName(),
                    entity.getTargetBootstrapServers(),
                    entity.isTargetKerberosEnabled(),
                    entity.getTargetPrincipal(),
                    entity.getTargetKeytabPath(),
                    entity.getTargetServiceName(),
                    topics,
                    false
            );
            entity.setTopicProvisioningSummary(res.toSummaryString());
            repository.save(entity);
            return res;
        }).orElse(TopicProvisioningResult.warning("Задача не найдена: " + jobId));
    }

    /**
     * Динамическое обнаружение топиков из кластера Kafka.
     * При недоступности брокера или пустом bootstrapServers возвращает пустой список.
     */
    public List<Map<String, Object>> discoverTopics(String bootstrapServers,
                                                    boolean kerberosEnabled,
                                                    String principal,
                                                    String keytabPath,
                                                    String serviceName) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            return Collections.emptyList();
        }
        try (AdminClient adminClient = adminClientFactory.createAdminClient(bootstrapServers, kerberosEnabled, principal, keytabPath, serviceName)) {
            ListTopicsResult listResult = adminClient.listTopics(new ListTopicsOptions().timeoutMs(5000));
            Set<String> topicNames = listResult.names().get(5, TimeUnit.SECONDS);
            if (topicNames == null || topicNames.isEmpty()) {
                return Collections.emptyList();
            }

            DescribeTopicsResult describeResult = adminClient.describeTopics(topicNames, new DescribeTopicsOptions().timeoutMs(5000));
            Map<String, TopicDescription> descriptions = describeResult.allTopicNames().get(5, TimeUnit.SECONDS);

            List<Map<String, Object>> result = new ArrayList<>();
            for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                String name = entry.getKey();
                TopicDescription desc = entry.getValue();
                if (desc.isInternal()) {
                    continue;
                }
                int partitions = desc.partitions() != null ? desc.partitions().size() : 0;
                boolean hasSchema = name.contains("avro") || name.contains("schema");
                String format = hasSchema ? "SCHEMA_REGISTRY" : "AUTO_DETECT";
                result.add(Map.of(
                        "name", name,
                        "partitions", partitions,
                        "detectedFormat", format,
                        "hasSchema", hasSchema
                ));
            }
            result.sort(Comparator.comparing(m -> (String) m.get("name")));
            return result;
        } catch (Exception e) {
            log.warn("Не удалось получить список топиков из Kafka кластера ({}): {}",
                    bootstrapServers, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Динамическое обнаружение консьюмер-групп из кластера Kafka.
     * При недоступности брокера или пустом bootstrapServers возвращает пустой список.
     */
    public List<Map<String, Object>> discoverConsumerGroups(String bootstrapServers,
                                                           boolean kerberosEnabled,
                                                           String principal,
                                                           String keytabPath,
                                                           String serviceName) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            return Collections.emptyList();
        }
        try (AdminClient adminClient = adminClientFactory.createAdminClient(bootstrapServers, kerberosEnabled, principal, keytabPath, serviceName)) {
            ListConsumerGroupsResult listResult = adminClient.listConsumerGroups(new ListConsumerGroupsOptions().timeoutMs(5000));
            Collection<ConsumerGroupListing> listings = listResult.all().get(5, TimeUnit.SECONDS);
            if (listings == null || listings.isEmpty()) {
                return Collections.emptyList();
            }
            List<String> groupIds = listings.stream().map(ConsumerGroupListing::groupId).toList();

            DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions().timeoutMs(5000));
            Map<String, ConsumerGroupDescription> descriptions = describeResult.allTopicNames() != null
                    ? describeResult.all().get(5, TimeUnit.SECONDS)
                    : Collections.emptyMap();

            List<Map<String, Object>> result = new ArrayList<>();
            for (Map.Entry<String, ConsumerGroupDescription> entry : descriptions.entrySet()) {
                String groupId = entry.getKey();
                ConsumerGroupDescription desc = entry.getValue();
                String state = desc.state() != null ? desc.state().name() : "UNKNOWN";
                int members = desc.members() != null ? desc.members().size() : 0;
                result.add(Map.of(
                        "groupId", groupId,
                        "state", state,
                        "members", members,
                        "lag", 0L
                ));
            }
            result.sort(Comparator.comparing(m -> (String) m.get("groupId")));
            return result;
        } catch (Exception e) {
            log.warn("Не удалось получить список консьюмер-групп из Kafka кластера ({}): {}",
                    bootstrapServers, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Обновление агрегированных счетчиков телеметрии Kafka (сообщения, байты, лаг)
     * из периодического keepalive heartbeat агента.
     */
    @Transactional
    public void updateMetricsFromHeartbeat(String dcId, String agentId, long messagesReplicated, long bytesReplicated, long currentLagMs) {
        if (dcId == null || dcId.isBlank()) {
            return;
        }
        String dc = dcId.trim();
        List<KafkaReplicationJobEntity> activeJobs = repository.findAll().stream()
                .filter(job -> "ACTIVE".equalsIgnoreCase(job.getStatus()))
                .filter(job -> dc.equalsIgnoreCase(job.getSourceDc()) ||
                        ("BIDIRECTIONAL".equalsIgnoreCase(job.getDirection()) && dc.equalsIgnoreCase(job.getTargetDc())))
                .toList();

        for (KafkaReplicationJobEntity job : activeJobs) {
            if (messagesReplicated > 0) {
                job.setMessagesReplicated(Math.max(job.getMessagesReplicated() != null ? job.getMessagesReplicated() : 0L, messagesReplicated));
            }
            if (bytesReplicated > 0) {
                job.setBytesReplicated(Math.max(job.getBytesReplicated() != null ? job.getBytesReplicated() : 0L, bytesReplicated));
            }
            if (currentLagMs >= 0) {
                job.setCurrentLagMs(currentLagMs);
            }
            job.setUpdatedAt(Instant.now());
            repository.save(job);
        }
    }
}
