package org.apache.hadoop.explorer.replicator.orchestrator.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.replicator.kafka.filter.ConsumerGroupBlacklistFilter;
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
    private final org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry topologyRegistry;

    @org.springframework.beans.factory.annotation.Autowired
    public KafkaReplicationOrchestratorService(KafkaReplicationJobRepository repository,
                                               KafkaTopicProvisioner topicProvisioner,
                                               @org.springframework.beans.factory.annotation.Autowired(required = false)
                                               org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry agentRegistry,
                                               @org.springframework.beans.factory.annotation.Autowired(required = false)
                                               org.apache.hadoop.explorer.replicator.orchestrator.kafka.KafkaAdminClientFactory adminClientFactory,
                                               @org.springframework.beans.factory.annotation.Autowired(required = false)
                                               org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry topologyRegistry) {
        this.repository = repository;
        this.topicProvisioner = topicProvisioner != null ? topicProvisioner : new KafkaTopicProvisioner();
        this.agentRegistry = agentRegistry;
        this.adminClientFactory = adminClientFactory != null ? adminClientFactory : new org.apache.hadoop.explorer.replicator.orchestrator.kafka.DefaultKafkaAdminClientFactory();
        this.topologyRegistry = topologyRegistry;
    }

    public KafkaReplicationOrchestratorService(KafkaReplicationJobRepository repository,
                                               KafkaTopicProvisioner topicProvisioner,
                                               org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry agentRegistry,
                                               org.apache.hadoop.explorer.replicator.orchestrator.kafka.KafkaAdminClientFactory adminClientFactory) {
        this(repository, topicProvisioner, agentRegistry, adminClientFactory, null);
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

    public KafkaReplicationJobDto createJob(CreateKafkaJobRequest request) {
        String id = "kafka-job-" + UUID.randomUUID().toString().substring(0, 8);
        KafkaReplicationJobEntity entity = new KafkaReplicationJobEntity();
        entity.setId(id);
        entity.setName(request.getName() != null && !request.getName().isBlank() ? request.getName() : id);
        entity.setSourceDc(request.getSourceDc());
        entity.setTargetDc(request.getTargetDc());
        entity.setDirection(request.getDirection() != null ? request.getDirection() : "BIDIRECTIONAL");

        // Автоматический расчет полосы WAN из TopologyRegistry HTB и заданных при старте лимитов агентов (если не задано вручную)
        Double resolvedBandwidth = request.getBandwidthLimitMb();
        if ((resolvedBandwidth == null || resolvedBandwidth <= 0) && topologyRegistry != null
                && request.getSourceDc() != null && request.getTargetDc() != null) {
            var dcLimit = topologyRegistry.getDcLimit(request.getSourceDc(), request.getTargetDc());
            Double wanQuotaMb = null;
            if (dcLimit.isPresent()) {
                // 40% гарантированная полоса HTB для Kafka (в МБ/с)
                wanQuotaMb = Math.round((dcLimit.get() * 0.40) / (1024.0 * 1024.0) * 10.0) / 10.0;
            }

            // Суммарный лимит пула агентов-отправителей источника (задается при старте демонов DevOps)
            Double sendersCapacityMb = null;
            List<org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry.AgentEntry> liveSenders =
                    agentRegistry != null ? agentRegistry.getLiveKafkaSendersForCluster(request.getSourceDc()) : List.of();
            if (!liveSenders.isEmpty()) {
                sendersCapacityMb = liveSenders.stream()
                        .mapToDouble(a -> (a.getMaxBandwidthMbS() != null && a.getMaxBandwidthMbS() > 0) ? a.getMaxBandwidthMbS() : 100.0)
                        .sum();
            }

            if (wanQuotaMb != null && sendersCapacityMb != null) {
                // Двухконтурное ограничение: не превышаем WAN HTB квоту и суммарную емкость клиентских интерфейсов агентов
                resolvedBandwidth = Math.min(wanQuotaMb, sendersCapacityMb);
            } else if (wanQuotaMb != null) {
                resolvedBandwidth = wanQuotaMb;
            } else if (sendersCapacityMb != null) {
                resolvedBandwidth = sendersCapacityMb;
            }
        }
        if (resolvedBandwidth == null || resolvedBandwidth <= 0) {
            resolvedBandwidth = 50.0; // безопасный дефолт
        }
        entity.setBandwidthLimitMb(resolvedBandwidth);

        // Автоматический расчет интервала синхронизации оффсетов Consumer Groups
        Integer resolvedOffsetSync = request.getOffsetSyncIntervalSec();
        if (resolvedOffsetSync == null || resolvedOffsetSync <= 0) {
            if (resolvedBandwidth >= 1500.0) {
                resolvedOffsetSync = 1;
            } else if (resolvedBandwidth >= 500.0) {
                resolvedOffsetSync = 2;
            } else if (resolvedBandwidth >= 100.0) {
                resolvedOffsetSync = 3;
            } else if (resolvedBandwidth <= 20.0) {
                resolvedOffsetSync = 10;
            } else {
                resolvedOffsetSync = 5;
            }
        }
        entity.setOffsetSyncIntervalSec(resolvedOffsetSync);

        // Автоматический расчет размера микробатча
        Integer resolvedBatchSize = request.getBatchSize();
        if (resolvedBatchSize == null || resolvedBatchSize <= 0) {
            if (resolvedBandwidth <= 20.0) {
                resolvedBatchSize = 1000;
            } else if (resolvedBandwidth <= 80.0) {
                resolvedBatchSize = 2000;
            } else if (resolvedBandwidth <= 200.0) {
                resolvedBatchSize = 5000;
            } else if (resolvedBandwidth <= 800.0) {
                resolvedBatchSize = 10000;
            } else if (resolvedBandwidth <= 1500.0) {
                resolvedBatchSize = 15000;
            } else if (resolvedBandwidth <= 3000.0) {
                resolvedBatchSize = 20000;
            } else if (resolvedBandwidth <= 6000.0) {
                resolvedBatchSize = 35000;
            } else {
                resolvedBatchSize = 50000;
            }
        }
        entity.setBatchSize(resolvedBatchSize);

        // Автоматический расчет потоков вычитки на каждый агент-отправитель (шардинг пула без искусственного потолка в 40G: масштабирование на 100G, 200G, 400G+)
        Integer resolvedConsumerThreads = request.getConsumerThreads();
        if (resolvedConsumerThreads == null || resolvedConsumerThreads <= 0) {
            int targetTotalThreads;
            if (resolvedBandwidth <= 50.0) {
                targetTotalThreads = 8;
            } else if (resolvedBandwidth <= 100.0) {
                targetTotalThreads = 16;
            } else if (resolvedBandwidth <= 300.0) {
                targetTotalThreads = 24;
            } else if (resolvedBandwidth <= 800.0) {
                targetTotalThreads = 32;
            } else if (resolvedBandwidth <= 2000.0) {
                targetTotalThreads = 64;
            } else {
                // Динамическое линейное масштабирование для 100G/200G/400G+ сетей (~30 МБ/с на тред)
                targetTotalThreads = Math.max(64, (int) Math.round(resolvedBandwidth / 30.0));
            }
            int sendersCount = 1;
            if (request.getSourceDc() != null) {
                int liveSenders = getLiveSenderEndpoints(request.getSourceDc()).size();
                if (liveSenders > 0) {
                    sendersCount = liveSenders;
                }
            }
            resolvedConsumerThreads = Math.max(1, (int) Math.ceil((double) targetTotalThreads / sendersCount));
            resolvedConsumerThreads = Math.min(32, resolvedConsumerThreads);
        }
        entity.setConsumerThreads(resolvedConsumerThreads);
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

            ConsumerGroupBlacklistFilter groupFilter = ConsumerGroupBlacklistFilter.defaultFilter();
            List<String> rawGroups = request.getConsumerGroups() != null ? request.getConsumerGroups() : Collections.emptyList();
            List<String> filteredGroups = groupFilter.filterAllowed(rawGroups);
            Set<String> excludedGroups = groupFilter.filterExcluded(rawGroups);
            if (!excludedGroups.isEmpty()) {
                log.warn("При создании задачи репликации служебные/инфраструктурные консьюмер-группы исключены из синхронизации: {}", excludedGroups);
            }
            entity.setConsumerGroupsJson(mapper.writeValueAsString(filteredGroups));

            List<String> effectiveEndpoints = request.getTargetEndpoints();
            if ((effectiveEndpoints == null || effectiveEndpoints.isEmpty()) && request.getTargetDc() != null) {
                effectiveEndpoints = getLiveReceiverEndpoints(request.getTargetDc());
            }
            if (effectiveEndpoints != null && !effectiveEndpoints.isEmpty()) {
                entity.setTargetEndpointsJson(mapper.writeValueAsString(effectiveEndpoints));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize topics or consumer groups to JSON", e);
        }

        // Задача записывается до обращения к брокерам: провижининг топиков — блокирующий
        // вызов AdminClient с Kerberos-логином, и внутри транзакции он удерживал соединение
        // Hikari на весь таймаут, а недоступный брокер откатывал создание задачи целиком.
        KafkaReplicationJobEntity saved = repository.save(entity);

        String provisioningSummary = provisionTopicsOutsideTransaction(request, saved);
        if (provisioningSummary != null) {
            saved.setTopicProvisioningSummary(provisioningSummary);
            saved = repository.save(saved);
        }

        log.info("Создана новая задача репликации Kafka '{}' ({}) [{} <-> {}], провижининг топиков: {}",
                saved.getName(), saved.getId(), saved.getSourceDc(), saved.getTargetDc(), saved.getTopicProvisioningSummary());
        return toDto(saved);
    }


    /**
     * Провижининг топиков вне транзакции создания задачи.
     *
     * <p>Вызов блокирующий (AdminClient к обоим брокерам, при BIDIRECTIONAL дважды, с
     * Kerberos-логином), поэтому выполняется после фиксации задачи. Сбой брокера не отменяет
     * созданную задачу: топики можно досоздать позже, а причина сохраняется в сводке.
     */
    private String provisionTopicsOutsideTransaction(CreateKafkaJobRequest request, KafkaReplicationJobEntity entity) {
        String sourceBootstrap = entity.getSourceBootstrapServers();
        String targetBootstrap = entity.getTargetBootstrapServers();
        try {
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
                return summary;
            }


        } catch (Exception e) {
            log.error("Провижининг топиков для задачи '{}' не выполнен: {}", entity.getId(), e.getMessage());
            return "Ошибка провижининга топиков: " + e.getMessage();
        }
        return null;
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
                List<CreateKafkaJobRequest.TopicSetting> topics = mapper.readValue(entity.getTopicsJson(), new TypeReference<>() {});
                if (topics != null) {
                    for (CreateKafkaJobRequest.TopicSetting t : topics) {
                        if (t.getLag() == null) {
                            t.setLag(0L);
                        }
                        if (t.getLagMs() == null) {
                            t.setLagMs(entity.getCurrentLagMs() != null ? entity.getCurrentLagMs() : 0L);
                        }
                    }
                }
                dto.setTopics(topics);
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
     * Возвращает список адресов активных агентов-отправителей для кластера/ДЦ источника.
     */
    public List<String> getLiveSenderEndpoints(String clusterId) {
        if (agentRegistry == null) {
            return List.of();
        }
        var senders = agentRegistry.getLiveKafkaSendersForCluster(clusterId);
        return senders.stream()
                .map(org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry.AgentEntry::getGrpcAddress)
                .toList();
    }

    /**
     * Возвращает детальный список активных агентов-отправителей для кластера/ДЦ источника
     * с их локальными лимитами полосы (maxBandwidthMbS), заданными инженером при старте демонов.
     */
    public List<org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaAgentEndpointDto> getLiveSenderAgentDetails(String clusterId) {
        if (agentRegistry == null) {
            return List.of();
        }
        var senders = agentRegistry.getLiveKafkaSendersForCluster(clusterId);
        return senders.stream()
                .map(a -> new org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaAgentEndpointDto(
                        a.getAgentId(),
                        a.getGrpcAddress(),
                        a.getClusterId(),
                        a.getMode(),
                        a.getStatus() != null ? a.getStatus().name() : "ONLINE",
                        a.getMaxBandwidthMbS()
                ))
                .toList();
    }

    /**
     * Разрешает адрес брокеров Kafka по идентификатору ДЦ/кластера из топологии или явно переданному адресу.
     */
    public String resolveBootstrapServers(String dc, String explicitServers) {
        if (explicitServers != null && !explicitServers.isBlank()) {
            return explicitServers.trim();
        }
        if (dc == null || dc.isBlank()) {
            return null;
        }
        String trimmed = dc.trim();
        if (topologyRegistry != null) {
            for (var c : topologyRegistry.getClusters()) {
                var cfg = topologyRegistry.getCluster(c.getId()).orElse(null);
                if (cfg != null && (trimmed.equalsIgnoreCase(cfg.getDcId()) || trimmed.equalsIgnoreCase(cfg.getId()))) {
                    if (cfg.getKafkaBootstrapServers() != null && !cfg.getKafkaBootstrapServers().isBlank()) {
                        return cfg.getKafkaBootstrapServers().trim();
                    }
                }
            }
        }
        if (trimmed.contains(":")) {
            return trimmed;
        }
        return null;
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
            List<String> rawGroupIds = listings.stream().map(ConsumerGroupListing::groupId).toList();
            ConsumerGroupBlacklistFilter groupFilter = ConsumerGroupBlacklistFilter.defaultFilter();
            List<String> groupIds = groupFilter.filterAllowed(rawGroupIds);
            if (groupIds.isEmpty()) {
                return Collections.emptyList();
            }

            DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions().timeoutMs(5000));
            Map<String, ConsumerGroupDescription> descriptions = describeResult.all() != null
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
        // Точечная выборка вместо чтения всей таблицы задач на каждый heartbeat агента
        List<KafkaReplicationJobEntity> activeJobs = repository.findActiveJobsReportedByDc(dc);

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
