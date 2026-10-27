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
        // Выборка запросом: метод вызывается на каждом обращении агента ЦОД
        return repository.findActiveJobsReportedByDc(dc).stream()
                .map(this::toDto)
                .toList();
    }

    public KafkaReplicationJobDto createJob(CreateKafkaJobRequest request) {
        String direction = request.getDirection() != null && !request.getDirection().isBlank()
                ? request.getDirection() : "UNIDIRECTIONAL";
        List<String> requestedTopics = request.getTopics() == null ? List.of() : request.getTopics().stream()
                .map(CreateKafkaJobRequest.TopicSetting::getTopic)
                .filter(Objects::nonNull)
                .toList();
        findOverlap(null, request.getSourceDc(), request.getTargetDc(), direction, requestedTopics, false)
                .ifPresent(conflict -> {
                    throw new JobOverlapException(conflict);
                });
        String id = "kafka-job-" + UUID.randomUUID().toString().substring(0, 8);
        KafkaReplicationJobEntity entity = new KafkaReplicationJobEntity();
        entity.setId(id);
        entity.setName(request.getName() != null && !request.getName().isBlank() ? request.getName() : id);
        entity.setSourceDc(request.getSourceDc());
        entity.setTargetDc(request.getTargetDc());
        entity.setDirection(request.getDirection() != null && !request.getDirection().isBlank()
                ? request.getDirection().trim().toUpperCase(Locale.ROOT) : "UNIDIRECTIONAL");
        entity.setStartPosition(normalizeStartPosition(request.getStartPosition()));

        // Автоматический расчет полосы WAN из TopologyRegistry HTB и заданных при старте лимитов агентов (если не задано вручную)
        Double resolvedBandwidth = request.getBandwidthLimitMb();
        if ((resolvedBandwidth == null || resolvedBandwidth <= 0) && topologyRegistry != null
                && request.getSourceDc() != null && request.getTargetDc() != null) {
            var dcLimit = topologyRegistry.getDcLimit(request.getSourceDc(), request.getTargetDc());
            Double wanQuotaMb = null;
            // Нулевой лимит канала — ограждение, а не квота: из него не выводится лимит задачи
            // (иначе 0 стал бы «без лимита» — ровно обратный смысл)
            if (dcLimit.isPresent() && dcLimit.get() > 0) {
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
        // Лимит не задан и вывести его не из чего — задача идет без лимита (0). Прежний
        // «безопасный дефолт» 50 МБ/с молча душил поток: для 2 млн событий/с это в десятки раз
        // меньше нужного, а после деления лимита между агентами — единицы МБ/с на агента.
        if (resolvedBandwidth == null || resolvedBandwidth < 0) {
            resolvedBandwidth = 0.0;
        }
        entity.setBandwidthLimitMb(resolvedBandwidth);
        // Ориентир для подбора батча, потоков и интервала синхронизации: без лимита
        // рассчитываем на широкий канал, а не на минимальный
        double sizingBandwidth = resolvedBandwidth > 0 ? resolvedBandwidth : UNLIMITED_SIZING_MB;

        // Автоматический расчет интервала синхронизации оффсетов Consumer Groups
        Integer resolvedOffsetSync = request.getOffsetSyncIntervalSec();
        if (resolvedOffsetSync == null || resolvedOffsetSync <= 0) {
            if (sizingBandwidth >= 1500.0) {
                resolvedOffsetSync = 1;
            } else if (sizingBandwidth >= 500.0) {
                resolvedOffsetSync = 2;
            } else if (sizingBandwidth >= 100.0) {
                resolvedOffsetSync = 3;
            } else if (sizingBandwidth <= 20.0) {
                resolvedOffsetSync = 10;
            } else {
                resolvedOffsetSync = 5;
            }
        }
        entity.setOffsetSyncIntervalSec(resolvedOffsetSync);

        // Автоматический расчет размера микробатча
        Integer resolvedBatchSize = request.getBatchSize();
        if (resolvedBatchSize == null || resolvedBatchSize <= 0) {
            if (sizingBandwidth <= 20.0) {
                resolvedBatchSize = 1000;
            } else if (sizingBandwidth <= 80.0) {
                resolvedBatchSize = 2000;
            } else if (sizingBandwidth <= 200.0) {
                resolvedBatchSize = 5000;
            } else if (sizingBandwidth <= 800.0) {
                resolvedBatchSize = 10000;
            } else if (sizingBandwidth <= 1500.0) {
                resolvedBatchSize = 15000;
            } else if (sizingBandwidth <= 3000.0) {
                resolvedBatchSize = 20000;
            } else if (sizingBandwidth <= 6000.0) {
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
            if (sizingBandwidth <= 50.0) {
                targetTotalThreads = 8;
            } else if (sizingBandwidth <= 100.0) {
                targetTotalThreads = 16;
            } else if (sizingBandwidth <= 300.0) {
                targetTotalThreads = 24;
            } else if (sizingBandwidth <= 800.0) {
                targetTotalThreads = 32;
            } else if (sizingBandwidth <= 2000.0) {
                targetTotalThreads = 64;
            } else {
                // Динамическое линейное масштабирование для 100G/200G/400G+ сетей (~30 МБ/с на тред)
                targetTotalThreads = Math.max(64, (int) Math.round(sizingBandwidth / 30.0));
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

            // Сохраняются только приемники, заданные явно. Снимок живых приемников на момент
            // создания навсегда привязывал задачу к тем, кто был жив тогда: если работал один
            // из 12, все отправители так и слали в него. Без явного списка агенты получают
            // текущих живых приемников при каждом опросе.
            List<String> explicitEndpoints = request.getTargetEndpoints();
            if (explicitEndpoints != null && !explicitEndpoints.isEmpty()) {
                entity.setTargetEndpointsJson(mapper.writeValueAsString(explicitEndpoints));
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
            repository.updateProvisioningSummary(saved.getId(), provisioningSummary);
            saved.setTopicProvisioningSummary(provisioningSummary);
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
                    if (!reverseResult.createdTopics().isEmpty() || !reverseResult.conflicts().isEmpty()) {
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
        boolean updated = repository.updateStatus(id, "PAUSED", Instant.now()) > 0;
        if (updated) {
            log.info("Задача репликации Kafka '{}' приостановлена", id);
        }
        return updated;
    }

    @Transactional
    public boolean resumeJob(String id) {
        repository.findById(id).ifPresent(job -> findOverlap(id, job.getSourceDc(), job.getTargetDc(),
                job.getDirection(), topicsOf(job), true).ifPresent(conflict -> {
                    throw new JobOverlapException(conflict);
                }));
        boolean updated = repository.updateStatus(id, "ACTIVE", Instant.now()) > 0;
        if (updated) {
            log.info("Задача репликации Kafka '{}' возобновлена", id);
        }
        return updated;
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
        dto.setRunningAgents(getRunningAgents(entity.getId()));
        dto.setAgentErrors(getAgentErrors(entity.getId()));
        dto.setLastErrorMessage(entity.getLastErrorMessage());
        dto.setCreatedAt(entity.getCreatedAt());
        dto.setUpdatedAt(entity.getUpdatedAt());

        dto.setSourceBootstrapServers(entity.getSourceBootstrapServers());
        dto.setTargetBootstrapServers(entity.getTargetBootstrapServers());
        dto.setAutoCreateTopics(entity.isAutoCreateTopics());
        dto.setStartPosition(entity.getStartPosition() != null ? entity.getStartPosition() : "EARLIEST");
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
            } else if (entity.getTargetDc() != null) {
                // Явного списка нет — задача работает на текущих живых приемниках целевого ЦОД
                dto.setTargetEndpoints(getLiveReceiverEndpoints(entity.getTargetDc()));
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
        // Порядок фиксированный: реестр хранит агентов в хеш-таблице, и перестановка между
        // опросами выглядела бы для агента сменой приемников
        return receivers.stream()
                .map(org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry.AgentEntry::getGrpcAddress)
                .filter(Objects::nonNull)
                .sorted()
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
                .filter(Objects::nonNull)
                .sorted()
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
    /*
     * Без транзакции: вызовы AdminClient к двум брокерам с таймаутами держали бы соединение
     * с БД все это время. Сводка пишется точечным UPDATE.
     */
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
            String summary = res.toSummaryString();
            if ("BIDIRECTIONAL".equalsIgnoreCase(entity.getDirection())) {
                TopicProvisioningResult reverse = topicProvisioner.provisionTopics(
                        entity.getTargetBootstrapServers(),
                        entity.isTargetKerberosEnabled(),
                        entity.getTargetPrincipal(),
                        entity.getTargetKeytabPath(),
                        entity.getTargetServiceName(),
                        entity.getSourceBootstrapServers(),
                        entity.isSourceKerberosEnabled(),
                        entity.getSourcePrincipal(),
                        entity.getSourceKeytabPath(),
                        entity.getSourceServiceName(),
                        topics,
                        false
                );
                if (!reverse.createdTopics().isEmpty() || !reverse.conflicts().isEmpty() || !reverse.success()) {
                    summary = summary + " [Обратная репликация: " + reverse.toSummaryString() + "]";
                }
                if (!reverse.success()) {
                    res = new TopicProvisioningResult(res.createdTopics(), res.conflicts(), res.matchedTopics(),
                            false, summary);
                }
            }
            repository.updateProvisioningSummary(jobId, summary);
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
     * Стартовая позиция задачи. Незаданная — LATEST, неизвестное значение — ошибка: опечатка
     * не должна молча превращаться в перекачку всего топика.
     */
    /** Задача пересекается с другой: тот же топик в том же направлении между теми же ЦОД. */
    public static class JobOverlapException extends RuntimeException {
        public JobOverlapException(String message) {
            super(message);
        }
    }

    /**
     * Поиск задачи, которая уже переносит один из топиков в том же направлении.
     *
     * <p>У каждой задачи своя консьюмер-группа, поэтому две такие задачи читают топик
     * независимо и пишут в цель каждую запись дважды. Задача на паузе тоже мешает созданию:
     * после возобновления она начнет дублировать. При возобновлении учитываются только
     * активные задачи.
     *
     * @return описание конфликта или пусто
     */
    public Optional<String> findOverlap(String excludeId, String sourceDc, String targetDc, String direction,
                                        Collection<String> topics, boolean activeOnly) {
        if (topics == null || topics.isEmpty() || sourceDc == null || targetDc == null) {
            return Optional.empty();
        }
        Set<String> wanted = new HashSet<>(topics);
        Set<String> legs = legs(sourceDc, targetDc, direction);
        for (KafkaReplicationJobEntity other : repository.findAll()) {
            if (other.getId().equals(excludeId)) {
                continue;
            }
            if (activeOnly && !"ACTIVE".equalsIgnoreCase(other.getStatus())) {
                continue;
            }
            Set<String> sharedLegs = new HashSet<>(legs(other.getSourceDc(), other.getTargetDc(), other.getDirection()));
            sharedLegs.retainAll(legs);
            if (sharedLegs.isEmpty()) {
                continue;
            }
            List<String> sharedTopics = topicsOf(other).stream().filter(wanted::contains).sorted().toList();
            if (!sharedTopics.isEmpty()) {
                return Optional.of(String.format(
                        "Топики %s уже реплицирует задача '%s' (%s, %s): вторая задача писала бы каждую запись дважды",
                        sharedTopics, other.getName(), other.getId(), other.getStatus()));
            }
        }
        return Optional.empty();
    }

    /** Направления задачи как пары ЦОД; BIDIRECTIONAL дает оба. */
    private static Set<String> legs(String sourceDc, String targetDc, String direction) {
        if (sourceDc == null || targetDc == null) {
            return Set.of();
        }
        String forward = sourceDc.trim().toLowerCase(Locale.ROOT) + "->" + targetDc.trim().toLowerCase(Locale.ROOT);
        if (!"BIDIRECTIONAL".equalsIgnoreCase(direction)) {
            return Set.of(forward);
        }
        return Set.of(forward, targetDc.trim().toLowerCase(Locale.ROOT) + "->" + sourceDc.trim().toLowerCase(Locale.ROOT));
    }

    private List<String> topicsOf(KafkaReplicationJobEntity job) {
        try {
            if (job.getTopicsJson() == null) {
                return List.of();
            }
            List<CreateKafkaJobRequest.TopicSetting> settings =
                    mapper.readValue(job.getTopicsJson(), new TypeReference<>() {});
            return settings == null ? List.of() : settings.stream()
                    .map(CreateKafkaJobRequest.TopicSetting::getTopic)
                    .filter(Objects::nonNull)
                    .toList();
        } catch (Exception e) {
            log.warn("Не удалось прочитать топики задачи {}: {}", job.getId(), e.getMessage());
            return List.of();
        }
    }

    public static String normalizeStartPosition(String value) {
        if (value == null || value.isBlank()) {
            return "LATEST";
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        if (!normalized.equals("LATEST") && !normalized.equals("EARLIEST")) {
            throw new IllegalArgumentException("start_position должен быть LATEST или EARLIEST, получено: " + value);
        }
        return normalized;
    }

    /** Ориентир полосы (МБ/с) для подбора параметров задачи без лимита. */
    private static final double UNLIMITED_SIZING_MB = 1000.0;

    /**
     * Доля лимита задачи на одного агента-отправителя.
     *
     * <p>Лимит задачи — суммарный на задачу, а применяет его каждый агент у себя. Без деления
     * 12 агентов пропускали в 12 раз больше заданного. 0 — без лимита, и делить нечего.
     */
    public double perAgentBandwidthMb(Double jobLimitMb, String sendingDc) {
        if (jobLimitMb == null || jobLimitMb <= 0) {
            return 0.0;
        }
        int senders = sendingDc != null ? getLiveSenderEndpoints(sendingDc).size() : 0;
        return jobLimitMb / Math.max(1, senders);
    }

    /** Агент считается живым для лага, пока его heartbeat не старше этого порога. */
    private static final long AGENT_LAG_FRESHNESS_MS = 30_000L;

    /**
     * Последняя телеметрия агента. {@code base} накапливает счетчики прошлых жизней агента:
     * после рестарта он начинает счет с нуля, и без этого сумма по ЦОД проседала бы.
     */
    private static final class AgentKafkaStats {
        long base;
        long lastMessages;
        long baseBytes;
        long lastBytes;
        long lagMs;
        long seenAtMs;
        /** Пул задачи на агенте работает: задача есть в его последнем heartbeat. */
        volatile boolean running;

        long messages() {
            return base + lastMessages;
        }

        long bytes() {
            return baseBytes + lastBytes;
        }
    }

    /** Телеметрия агентов по ЦОД: dc -> agentId -> статистика. */
    private final Map<String, Map<String, AgentKafkaStats>> agentKafkaStats = new java.util.concurrent.ConcurrentHashMap<>();

    /** Телеметрия задач по агентам: jobId -> agentId -> статистика. */
    private final Map<String, Map<String, AgentKafkaStats>> jobAgentStats = new java.util.concurrent.ConcurrentHashMap<>();

    /** Ошибки запуска пулов задач: jobId -> agentId -> причина. */
    private final Map<String, Map<String, AgentJobError>> jobAgentErrors = new java.util.concurrent.ConcurrentHashMap<>();

    private record AgentJobError(String message, long seenAtMs) {}

    /**
     * Ошибки запуска пулов задач из heartbeat агента.
     *
     * <p>Отчет агента полный: задача, которой в нем нет, на этом агенте ошибок не имеет.
     * Хранятся в памяти — это состояние агентов, а не задачи, и запись в БД на каждый heartbeat
     * не нужна.
     */
    public void updateJobErrorsFromHeartbeat(String agentId,
                                             List<org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest.KafkaJobError> errors) {
        String agentKey = agentId != null && !agentId.isBlank() ? agentId : "unknown";
        long now = System.currentTimeMillis();
        jobAgentErrors.values().forEach(agents -> agents.remove(agentKey));
        if (errors != null) {
            for (var error : errors) {
                if (error.jobId() == null || error.jobId().isBlank()) {
                    continue;
                }
                jobAgentErrors.computeIfAbsent(error.jobId(), k -> new java.util.concurrent.ConcurrentHashMap<>())
                        .put(agentKey, new AgentJobError(error.message(), now));
            }
        }
        jobAgentErrors.values().removeIf(Map::isEmpty);
    }

    /** Ошибки задачи на агентах в виде «агент: причина»; агенты без свежего heartbeat не учитываются. */
    public List<String> getAgentErrors(String jobId) {
        Map<String, AgentJobError> agents = jobAgentErrors.get(jobId);
        if (agents == null) {
            return List.of();
        }
        long now = System.currentTimeMillis();
        List<String> result = new ArrayList<>();
        new java.util.TreeMap<>(agents).forEach((agent, error) -> {
            if (now - error.seenAtMs() <= AGENT_LAG_FRESHNESS_MS) {
                result.add(agent + ": " + error.message());
            }
        });
        return result;
    }

    /** Сброс накопленной телеметрии агентов: оркестратор начинает счет заново. */
    public void resetAgentTelemetry() {
        agentKafkaStats.clear();
        jobAgentStats.clear();
        jobAgentErrors.clear();
    }

    /**
     * Телеметрия задач из heartbeat агента: у каждой задачи свои счетчики.
     *
     * <p>Раньше агент сообщал одну сумму по всем своим задачам, и она приписывалась каждой
     * активной задаче ЦОД: новая задача на пустом топике показывала миллионы перенесенных
     * сообщений. Объем задачи — сумма по агентам, которые ее обслуживают, лаг — наибольший
     * среди живых агентов.
     */
    @Transactional
    public void updateJobMetricsFromHeartbeat(String agentId,
                                              List<org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest.KafkaJobStats> stats) {
        if (stats == null) {
            return;
        }
        String agentKey = agentId != null && !agentId.isBlank() ? agentId : "unknown";
        long now = System.currentTimeMillis();
        // Агент сообщает только задачи с работающим пулом: задача, которой нет в отчете,
        // на нем остановлена. Так подтверждается, что пауза и удаление дошли до агента
        java.util.Set<String> reported = new java.util.HashSet<>();
        for (var jobStats : stats) {
            reported.add(jobStats.jobId());
        }
        for (Map.Entry<String, Map<String, AgentKafkaStats>> job : jobAgentStats.entrySet()) {
            AgentKafkaStats agent = job.getValue().get(agentKey);
            if (agent != null && !reported.contains(job.getKey())) {
                agent.running = false;
            }
        }
        for (var jobStats : stats) {
            if (jobStats.jobId() == null || jobStats.jobId().isBlank()) {
                continue;
            }
            Map<String, AgentKafkaStats> agents = jobAgentStats.computeIfAbsent(jobStats.jobId(),
                    k -> new java.util.concurrent.ConcurrentHashMap<>());
            long totalMessages = 0L;
            long totalBytes = 0L;
            long maxLag = 0L;
            synchronized (agents) {
                AgentKafkaStats agentStats = agents.computeIfAbsent(agentKey, k -> new AgentKafkaStats());
                record(agentStats, jobStats.messagesReplicated(), jobStats.bytesReplicated(), jobStats.lagMs(), now);
                agentStats.running = true;
                for (AgentKafkaStats agent : agents.values()) {
                    totalMessages += agent.messages();
                    totalBytes += agent.bytes();
                    if (now - agent.seenAtMs <= AGENT_LAG_FRESHNESS_MS) {
                        maxLag = Math.max(maxLag, agent.lagMs);
                    }
                }
            }
            repository.applyJobTelemetry(jobStats.jobId(), totalMessages, totalBytes, maxLag, Instant.now());
        }
    }

    /**
     * Агенты, у которых пул задачи работает по последнему heartbeat.
     *
     * <p>После паузы или удаления список пустеет за один-два цикла опроса агентов: пока он
     * не пуст, задача на них еще читает. Агент, переставший слать heartbeat, выпадает из
     * списка по порогу свежести.
     */
    public List<String> getRunningAgents(String jobId) {
        Map<String, AgentKafkaStats> agents = jobAgentStats.get(jobId);
        if (agents == null) {
            return List.of();
        }
        long now = System.currentTimeMillis();
        List<String> running = new ArrayList<>();
        for (Map.Entry<String, AgentKafkaStats> agent : agents.entrySet()) {
            if (agent.getValue().running && now - agent.getValue().seenAtMs <= AGENT_LAG_FRESHNESS_MS) {
                running.add(agent.getKey());
            }
        }
        running.sort(null);
        return running;
    }

    /** Новые показания агента с учетом его рестарта: после него счетчики идут с нуля. */
    private static void record(AgentKafkaStats stats, long messagesReplicated, long bytesReplicated,
                               long lagMs, long nowMs) {
        long messages = Math.max(0L, messagesReplicated);
        long bytes = Math.max(0L, bytesReplicated);
        if (messages < stats.lastMessages) {
            stats.base += stats.lastMessages;
        }
        if (bytes < stats.lastBytes) {
            stats.baseBytes += stats.lastBytes;
        }
        stats.lastMessages = messages;
        stats.lastBytes = bytes;
        stats.lagMs = Math.max(0L, lagMs);
        stats.seenAtMs = nowMs;
    }

    /**
     * Обновление агрегированных счетчиков телеметрии Kafka (сообщения, байты, лаг)
     * из периодического keepalive heartbeat агента.
     *
     * <p>Партиции задачи делят между собой все агенты ЦОД, поэтому объем задачи — сумма их
     * счетчиков, а не максимум (иначе при 12 агентах интерфейс показывал бы ~1/12 объема), а
     * лаг — наибольший среди живых агентов, а не значение последнего приславшего heartbeat.
     */
    @Transactional
    public void updateMetricsFromHeartbeat(String dcId, String agentId, long messagesReplicated, long bytesReplicated, long currentLagMs) {
        if (dcId == null || dcId.isBlank()) {
            return;
        }
        String dc = dcId.trim().toLowerCase(Locale.ROOT);
        String agentKey = agentId != null && !agentId.isBlank() ? agentId : "unknown";
        long now = System.currentTimeMillis();

        long totalMessages = 0L;
        long totalBytes = 0L;
        long maxLag = 0L;
        Map<String, AgentKafkaStats> dcAgents = agentKafkaStats.computeIfAbsent(dc, k -> new java.util.concurrent.ConcurrentHashMap<>());
        synchronized (dcAgents) {
            record(dcAgents.computeIfAbsent(agentKey, k -> new AgentKafkaStats()),
                    messagesReplicated, bytesReplicated, currentLagMs, now);

            for (AgentKafkaStats agent : dcAgents.values()) {
                totalMessages += agent.messages();
                totalBytes += agent.bytes();
                if (now - agent.seenAtMs <= AGENT_LAG_FRESHNESS_MS) {
                    maxLag = Math.max(maxLag, agent.lagMs);
                }
            }
        }

        // Точечный UPDATE вместо чтения и сохранения сущностей: не затирает статус задачи
        repository.applyDcTelemetry(dc, totalMessages, totalBytes, maxLag, Instant.now());
    }
}
