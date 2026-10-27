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

    @Autowired(required = false)
    private org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry agentRegistry;

    @Autowired(required = false)
    private org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry topologyRegistry;

    @BeforeEach
    void setUpSecurity() {
        if (agentRegistry != null) {
            agentRegistry.clear();
        }
        // Лимиты каналов живут в синглтоне топологии: нулевое ограждение, оставленное другим
        // тестом контекста, меняло автоподбор полосы в зависимости от порядка запуска
        if (topologyRegistry != null) {
            topologyRegistry.resetLimitsToDefaults();
        }
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
        assertEquals(8, createdJob.getConsumerThreads());
        assertEquals(2000, createdJob.getBatchSize());
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
        assertEquals(8, entity.getConsumerThreads());
        assertEquals(2000, entity.getBatchSize());
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

        // 9. Проверка защищенности Discovery эндпоинтов (авторизованный пользователь)
        assertFalse(controller.discoverClusters().isEmpty());
        assertNotNull(controller.discoverTopics(null, null));
        assertNotNull(controller.discoverGroups(null, null));
        assertNotNull(controller.getReceiverEndpoints("DC2"));

        // 10. Проверка запрета неаутентифицированного доступа (401 UNAUTHORIZED)
        SecurityContextHolder.clearContext();
        org.springframework.web.server.ResponseStatusException ex1 =
                assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> controller.discoverClusters());
        assertEquals(HttpStatus.UNAUTHORIZED, ex1.getStatusCode());

        org.springframework.web.server.ResponseStatusException ex2 =
                assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> controller.discoverTopics(null, null));
        assertEquals(HttpStatus.UNAUTHORIZED, ex2.getStatusCode());

        org.springframework.web.server.ResponseStatusException ex3 =
                assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> controller.discoverGroups(null, null));
        assertEquals(HttpStatus.UNAUTHORIZED, ex3.getStatusCode());

        org.springframework.web.server.ResponseStatusException ex4 =
                assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> controller.getReceiverEndpoints("DC2"));
        assertEquals(HttpStatus.UNAUTHORIZED, ex4.getStatusCode());
    }

    @Test
    @DisplayName("Автоматический расчет параметров: полоса WAN из топологии HTB и эндпоинты агентов")
    void shouldAutoResolveTopologyBandwidthAndAgentEndpoints() {
        CreateKafkaJobRequest req = new CreateKafkaJobRequest();
        req.setName("Auto-Tuned-Job");
        req.setSourceDc("dc1");
        req.setTargetDc("dc2");
        req.setBandwidthLimitMb(null); // Автоматический расчет из топологии
        req.setTargetEndpoints(null); // Автоматический сбор из AgentRegistry

        ResponseEntity<KafkaReplicationJobDto> resp = controller.createJob(req);
        assertEquals(HttpStatus.CREATED, resp.getStatusCode());
        KafkaReplicationJobDto dto = resp.getBody();
        assertNotNull(dto);
        assertNotNull(dto.getBandwidthLimitMb());
        assertTrue(dto.getBandwidthLimitMb() > 0, "Полоса должна быть автоматически рассчитана из TopologyRegistry HTB");
        assertTrue(dto.getConsumerThreads() > 0, "Потоки консьюмеров должны быть автоматически рассчитаны");
        assertTrue(dto.getBatchSize() >= 1000, "Размер батча должен быть автоматически рассчитан");
        assertTrue(dto.getOffsetSyncIntervalSec() >= 3, "Интервал оффсетов должен быть автоматически рассчитан");

        // Проверка эндпоинтов отправки и приема
        List<String> senders = controller.getSenderEndpoints("dc1");
        assertNotNull(senders);
        List<String> receivers = controller.getReceiverEndpoints("dc2");
        assertNotNull(receivers);

        // Проверка расчета двухконтурного QoS
        Map<String, Object> qos = controller.calculateQos("dc1", "dc2");
        assertNotNull(qos);
        assertNotNull(qos.get("effectiveBandwidthMb"));
        assertNotNull(qos.get("bottleneck"));
        assertTrue((int) qos.get("threadsPerAgent") > 0);
    }

    @Test
    void testUltraHighBandwidthQosScaling() {
        // Тестирование масштабирования под ультра-высокие полосы 100G (5000 МБ/с) и 400G (20000 МБ/с)
        CreateKafkaJobRequest req100G = new CreateKafkaJobRequest();
        req100G.setName("job-100g-scale");
        req100G.setSourceDc("dc1");
        req100G.setTargetDc("dc2");
        req100G.setBandwidthLimitMb(5000.0); // 100 GBit/s (5000 МБ/с)

        KafkaReplicationJobDto created100G = service.createJob(req100G);
        assertEquals(35000, created100G.getBatchSize(), "Размер батча для 5000 МБ/с должен быть 35 000");
        // При 1 агенте sendersCount=1, Math.min(32, 167) -> 32 (защитный потолок на узел)
        assertEquals(32, created100G.getConsumerThreads(), "Потоки на 1 агент должны быть ограничены защитным потолком 32");

        CreateKafkaJobRequest req400G = new CreateKafkaJobRequest();
        req400G.setName("job-400g-scale");
        req400G.setSourceDc("dc1");
        req400G.setTargetDc("dc2");
        req400G.setBandwidthLimitMb(20000.0); // 400 GBit/s (20 000 МБ/с)

        KafkaReplicationJobDto created400G = service.createJob(req400G);
        assertEquals(50000, created400G.getBatchSize(), "Размер батча для 20 000 МБ/с должен быть 50 000");
        assertEquals(32, created400G.getConsumerThreads(), "Потоки на 1 агент должны быть ограничены 32 тредами во избежание трэшинга JVM");
    }
}
