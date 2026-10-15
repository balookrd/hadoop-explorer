package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.controller.KafkaReplicationController;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Комплексный Smoke-тест Control Plane оркестратора для репликации Kafka:
 * 1. Создание задания репликации с поддержкой Kerberos SASL и SPNEGO для Schema Registry;
 * 2. Проверка персистентности в JPA репозитории (Kerberos-флаги, типы реестров);
 * 3. Валидация жизненного цикла задания (PAUSE, RESUME, FAILOVER, DELETE);
 * 4. Проверка REST API контроллера KafkaReplicationController.
 */
@SpringBootTest(properties = {"replicator.hms.mock-cluster-api.enabled=true"})
@ActiveProfiles("test")
@Transactional
public class KafkaReplicationOrchestratorSmokeTest {

    @Autowired
    private KafkaReplicationController controller;

    @Autowired
    private KafkaReplicationOrchestratorService service;

    @Autowired
    private KafkaReplicationJobRepository repository;

    @BeforeEach
    void setUpSecurity() {
        UserSession adminSession = new UserSession(
                "kafka_admin", "Kafka Admin", "admin@kafka.local",
                List.of("kafka-admins"), "TEST", true, Role.ADMIN
        );
        SecurityContextHolder.getContext().setAuthentication(new CommonAuthenticationToken(adminSession));
    }

    @Test
    @DisplayName("Smoke Test: Создание и управление жизненным циклом задачи Kafka-репликации с Kerberos/SPNEGO")
    void shouldCreateAndManageKafkaReplicationJobWithKerberosSpnego() {
        // 1. Создание запроса на регистрацию репликации
        CreateKafkaJobRequest request = new CreateKafkaJobRequest();
        request.setName("e2e-smoke-kafka-replication");
        request.setSourceDc("DC1");
        request.setTargetDc("DC2");
        request.setDirection("UNIDIRECTIONAL");

        request.setTopics(List.of(
                new CreateKafkaJobRequest.TopicSetting("orders-avro", "SCHEMA_REGISTRY"),
                new CreateKafkaJobRequest.TopicSetting("metrics-raw", "PASSTHROUGH")
        ));
        request.setConsumerGroups(List.of("billing-group", "analytics-group"));
        request.setSchemaTranscodingEnabled(true);

        // Конфигурация Kerberos для брокеров
        request.setSourceKerberosEnabled(true);
        request.setSourcePrincipal("kafka-replicator@DC1.CORP");
        request.setSourceKeytabPath("/etc/security/keytabs/dc1-kafka.keytab");

        request.setTargetKerberosEnabled(true);
        request.setTargetPrincipal("kafka-replicator@DC2.CORP");
        request.setTargetKeytabPath("/etc/security/keytabs/dc2-kafka.keytab");

        // Конфигурация Schema Registry с кросс-вендорной трансляцией и SPNEGO
        request.setSourceSrUrl("http://dc1-hwx-sr.corp:7788/api/v1");
        request.setSourceSrType("HORTONWORKS");
        request.setSourceSrKerberosEnabled(true);

        request.setTargetSrUrl("http://dc2-confluent-sr.corp:8081");
        request.setTargetSrType("CONFLUENT");
        request.setTargetSrKerberosEnabled(true);

        // 2. Вызов REST API создания задачи
        ResponseEntity<KafkaReplicationJobDto> response = controller.createJob(request);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        KafkaReplicationJobDto createdJob = response.getBody();
        assertNotNull(createdJob);
        assertNotNull(createdJob.getId());
        assertEquals("e2e-smoke-kafka-replication", createdJob.getName());
        assertEquals("ACTIVE", createdJob.getStatus());

        // Проверка флагов Kerberos и SPNEGO в DTO
        assertTrue(createdJob.isSourceKerberosEnabled());
        assertTrue(createdJob.isSourceKerberosEnabled());
        assertTrue(createdJob.isTargetKerberosEnabled());
        assertTrue(createdJob.isSourceSrKerberosEnabled());
        assertTrue(createdJob.isTargetSrKerberosEnabled());
        assertEquals("HORTONWORKS", createdJob.getSourceSrType());
        assertEquals("CONFLUENT", createdJob.getTargetSrType());
        assertEquals(2, createdJob.getTopics().size());
        assertTrue(createdJob.isAutoCreateTopics(), "auto_create_topics по умолчанию true");
        assertNotNull(createdJob.getSourceBootstrapServers());
        assertNotNull(createdJob.getTargetBootstrapServers());
        assertNotNull(createdJob.getTopicProvisioningSummary());

        // 3. Проверка персистентности в JPA Entity репозитории
        Optional<KafkaReplicationJobEntity> entityOpt = repository.findById(createdJob.getId());
        assertTrue(entityOpt.isPresent(), "Сущность задачи должна быть сохранена в БД");
        KafkaReplicationJobEntity entity = entityOpt.get();
        assertEquals("DC1", entity.getSourceDc());
        assertEquals("DC2", entity.getTargetDc());
        assertTrue(entity.isSourceSrKerberosEnabled(), "JPA колонка source_sr_kerberos_enabled должна быть true");
        assertTrue(entity.isTargetSrKerberosEnabled(), "JPA колонка target_sr_kerberos_enabled должна быть true");
        assertTrue(entity.isAutoCreateTopics(), "JPA колонка auto_create_topics должна быть true");
        assertEquals("HORTONWORKS", entity.getSourceSrType());
        assertEquals("CONFLUENT", entity.getTargetSrType());
        assertTrue(entity.getTopicsJson().contains("orders-avro"));

        // 3.1. Проверка вызова REST эндпоинта предварительной проверки топиков
        var provisionResp = controller.provisionTopics(createdJob.getId());
        assertEquals(HttpStatus.OK, provisionResp.getStatusCode());
        assertNotNull(provisionResp.getBody());

        // 4. Проверка получения списка задач
        List<KafkaReplicationJobDto> jobs = controller.listJobs();
        assertFalse(jobs.isEmpty());
        assertTrue(jobs.stream().anyMatch(j -> j.getId().equals(createdJob.getId())));

        // 5. Проверка паузы (PAUSE)
        ResponseEntity<Map<String, Object>> pauseResp = controller.pauseJob(createdJob.getId());
        assertEquals(HttpStatus.OK, pauseResp.getStatusCode());
        assertEquals("PAUSED", pauseResp.getBody().get("status"));

        KafkaReplicationJobEntity pausedEntity = repository.findById(createdJob.getId()).orElseThrow();
        assertEquals("PAUSED", pausedEntity.getStatus());

        // 6. Проверка возобновления (RESUME)
        ResponseEntity<Map<String, Object>> resumeResp = controller.resumeJob(createdJob.getId());
        assertEquals(HttpStatus.OK, resumeResp.getStatusCode());
        assertEquals("ACTIVE", resumeResp.getBody().get("status"));

        KafkaReplicationJobEntity activeEntity = repository.findById(createdJob.getId()).orElseThrow();
        assertEquals("ACTIVE", activeEntity.getStatus());

        // 7. Проверка аварийного переключения (FAILOVER)
        ResponseEntity<Map<String, Object>> failoverResp = controller.triggerFailover(createdJob.getId());
        assertEquals(HttpStatus.OK, failoverResp.getStatusCode());
        assertEquals("SUCCESS", failoverResp.getBody().get("status"));

        // 8. Удаление задачи (DELETE)
        ResponseEntity<Void> deleteResp = controller.deleteJob(createdJob.getId());
        assertEquals(HttpStatus.NO_CONTENT, deleteResp.getStatusCode());

        Optional<KafkaReplicationJobEntity> deletedEntityOpt = repository.findById(createdJob.getId());
        assertFalse(deletedEntityOpt.isPresent(), "Сущность задачи должна быть удалена из БД");
    }
}
