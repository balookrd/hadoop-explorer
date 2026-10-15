package org.apache.hadoop.explorer.replicator.orchestrator.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
public class KafkaReplicationOrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(KafkaReplicationOrchestratorService.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final KafkaReplicationJobRepository repository;

    public KafkaReplicationOrchestratorService(KafkaReplicationJobRepository repository) {
        this.repository = repository;
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

        try {
            entity.setTopicsJson(mapper.writeValueAsString(request.getTopics() != null ? request.getTopics() : Collections.emptyList()));
            entity.setConsumerGroupsJson(mapper.writeValueAsString(request.getConsumerGroups() != null ? request.getConsumerGroups() : Collections.emptyList()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize topics or consumer groups to JSON", e);
        }

        KafkaReplicationJobEntity saved = repository.save(entity);
        log.info("Создана новая задача репликации Kafka '{}' ({}) [{} <-> {}]",
                saved.getName(), saved.getId(), saved.getSourceDc(), saved.getTargetDc());
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

        try {
            if (entity.getTopicsJson() != null) {
                dto.setTopics(mapper.readValue(entity.getTopicsJson(), new TypeReference<>() {}));
            }
            if (entity.getConsumerGroupsJson() != null) {
                dto.setConsumerGroups(mapper.readValue(entity.getConsumerGroupsJson(), new TypeReference<>() {}));
            }
        } catch (Exception e) {
            log.warn("Не удалось распарсить JSON топиков/групп для задачи {}: {}", entity.getId(), e.getMessage());
        }

        return dto;
    }
}
