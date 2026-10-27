package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Границы транзакции при создании Kafka-задачи.
 *
 * <p>Провижининг топиков — блокирующий вызов Kafka AdminClient к обоим брокерам (при
 * BIDIRECTIONAL в два прохода), да еще и с Kerberos-логином. Внутри транзакции он удерживал
 * соединение Hikari на весь таймаут AdminClient: несколько одновременных созданий задач
 * выбирали пул и роняли control plane. Кроме того, недоступный брокер откатывал транзакцию,
 * и задача не создавалась вовсе, хотя топики можно создать позже.
 */
@DisplayName("Создание Kafka-задачи: провижининг вне транзакции")
class TopicProvisioningBoundaryTest {

    private KafkaReplicationJobRepository repository;
    private KafkaTopicProvisioner provisioner;
    private KafkaReplicationOrchestratorService service;

    @BeforeEach
    void setUp() {
        repository = mock(KafkaReplicationJobRepository.class);
        provisioner = mock(KafkaTopicProvisioner.class);
        when(repository.save(any(KafkaReplicationJobEntity.class)))
                .thenAnswer(inv -> inv.getArgument(0));
        service = new KafkaReplicationOrchestratorService(repository, provisioner);
    }

    private static CreateKafkaJobRequest request() {
        CreateKafkaJobRequest request = new CreateKafkaJobRequest();
        request.setName("orders-replication");
        request.setSourceDc("dc1");
        request.setTargetDc("dc2");
        request.setSourceBootstrapServers("kafka-dc1:9092");
        request.setTargetBootstrapServers("kafka-dc2:9092");
        request.setAutoCreateTopics(true);
        CreateKafkaJobRequest.TopicSetting topic = new CreateKafkaJobRequest.TopicSetting();
        topic.setTopic("orders");
        request.setTopics(List.of(topic));
        return request;
    }

    @Test
    @DisplayName("Недоступный брокер не отменяет создание задачи")
    void testJobIsCreatedWhenProvisioningFails() {
        when(provisioner.provisionTopics(anyString(), anyBoolean(), any(), any(), any(),
                anyString(), anyBoolean(), any(), any(), any(), anyList(), anyBoolean()))
                .thenThrow(new RuntimeException("broker unreachable"));

        KafkaReplicationJobDto created = assertDoesNotThrow(() -> service.createJob(request()),
                "Сбой провижининга не должен отменять создание задачи: топики можно создать позже");

        assertNotNull(created.getId());
        assertNotNull(created.getTopicProvisioningSummary(), "Результат провижининга должен быть зафиксирован");
        assertTrue(created.getTopicProvisioningSummary().toLowerCase().contains("broker unreachable")
                        || created.getTopicProvisioningSummary().toLowerCase().contains("ошибка"),
                "В сводке должна быть причина сбоя, получено: " + created.getTopicProvisioningSummary());
    }

    @Test
    @DisplayName("Задача сохраняется до обращения к брокеру")
    void testJobIsPersistedBeforeProvisioning() {
        when(provisioner.provisionTopics(anyString(), anyBoolean(), any(), any(), any(),
                anyString(), anyBoolean(), any(), any(), any(), anyList(), anyBoolean()))
                .thenAnswer(inv -> {
                    // К моменту обращения к брокеру задача уже должна быть записана
                    verify(repository, atLeastOnce()).save(any(KafkaReplicationJobEntity.class));
                    return new TopicProvisioningResult(List.of("orders"), List.of(), List.of(), true, "Создан топик orders");
                });

        assertDoesNotThrow(() -> service.createJob(request()));

        // Сводка дописывается точечным UPDATE, а не повторным сохранением всей строки:
        // за время провижининга агенты и оператор могли поменять статус и метрики задачи
        verify(repository, times(1)).save(any(KafkaReplicationJobEntity.class));
        verify(repository).updateProvisioningSummary(anyString(), contains("orders"));
    }

    @Test
    @DisplayName("Повторный провижининг не перезаписывает строку задачи: пауза за это время сохраняется")
    void testReprovisioningDoesNotOverwriteJob() {
        KafkaReplicationJobEntity entity = new KafkaReplicationJobEntity();
        entity.setId("kafka-job-1");
        entity.setStatus("ACTIVE");
        entity.setDirection("BIDIRECTIONAL");
        entity.setSourceBootstrapServers("kafka-dc1:9092");
        entity.setTargetBootstrapServers("kafka-dc2:9092");
        entity.setTopicsJson("[{\"topic\":\"orders\"}]");
        when(repository.findById("kafka-job-1")).thenReturn(java.util.Optional.of(entity));
        when(provisioner.provisionTopics(anyString(), anyBoolean(), any(), any(), any(),
                anyString(), anyBoolean(), any(), any(), any(), anyList(), anyBoolean()))
                .thenReturn(new TopicProvisioningResult(List.of(), List.of(), List.of("orders"), true, "ok"));

        service.provisionTopicsForJob("kafka-job-1");

        verify(repository, never()).save(any(KafkaReplicationJobEntity.class));
        verify(repository).updateProvisioningSummary(eq("kafka-job-1"), anyString());
        verify(provisioner, times(2)).provisionTopics(anyString(), anyBoolean(), any(), any(), any(),
                anyString(), anyBoolean(), any(), any(), any(), anyList(), anyBoolean());
    }

    @Test
    @DisplayName("Успешный провижининг попадает в сводку задачи")
    void testSuccessfulProvisioningIsRecorded() {
        when(provisioner.provisionTopics(anyString(), anyBoolean(), any(), any(), any(),
                anyString(), anyBoolean(), any(), any(), any(), anyList(), anyBoolean()))
                .thenReturn(new TopicProvisioningResult(List.of("orders"), List.of(), List.of(), true, "Создан топик orders"));

        KafkaReplicationJobDto created = service.createJob(request());

        assertNotNull(created.getTopicProvisioningSummary());
        assertFalse(created.getTopicProvisioningSummary().isBlank());
    }
}
