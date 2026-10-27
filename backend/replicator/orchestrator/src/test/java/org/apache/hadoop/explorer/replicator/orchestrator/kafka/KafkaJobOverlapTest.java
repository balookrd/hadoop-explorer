package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService.JobOverlapException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Пересечение Kafka-задач.
 *
 * <p>У каждой задачи своя консьюмер-группа: две задачи с одним топиком в одном направлении
 * читали его независимо и писали в цель каждую запись дважды.
 */
@DisplayName("Kafka: пересекающиеся задачи отклоняются")
class KafkaJobOverlapTest {

    private final List<KafkaReplicationJobEntity> jobs = new ArrayList<>();
    private KafkaReplicationJobRepository repository;
    private KafkaReplicationOrchestratorService service;

    @BeforeEach
    void setUp() {
        jobs.clear();
        repository = mock(KafkaReplicationJobRepository.class);
        when(repository.findAll()).thenReturn(jobs);
        when(repository.save(any(KafkaReplicationJobEntity.class))).thenAnswer(inv -> inv.getArgument(0));
        when(repository.findById(anyString())).thenAnswer(inv ->
                jobs.stream().filter(j -> j.getId().equals(inv.getArgument(0))).findFirst());
        when(repository.updateStatus(anyString(), anyString(), any())).thenReturn(1);
        service = new KafkaReplicationOrchestratorService(repository, mock(KafkaTopicProvisioner.class));
    }

    private void existing(String id, String src, String dst, String direction, String status, String... topics) {
        KafkaReplicationJobEntity job = new KafkaReplicationJobEntity();
        job.setId(id);
        job.setName(id);
        job.setSourceDc(src);
        job.setTargetDc(dst);
        job.setDirection(direction);
        job.setStatus(status);
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < topics.length; i++) {
            json.append(i > 0 ? "," : "").append("{\"topic\":\"").append(topics[i]).append("\"}");
        }
        job.setTopicsJson(json.append("]").toString());
        jobs.add(job);
    }

    private static CreateKafkaJobRequest request(String src, String dst, String direction, String... topics) {
        CreateKafkaJobRequest request = new CreateKafkaJobRequest();
        request.setName("new");
        request.setSourceDc(src);
        request.setTargetDc(dst);
        request.setDirection(direction);
        List<CreateKafkaJobRequest.TopicSetting> settings = new ArrayList<>();
        for (String t : topics) {
            CreateKafkaJobRequest.TopicSetting s = new CreateKafkaJobRequest.TopicSetting();
            s.setTopic(t);
            settings.add(s);
        }
        request.setTopics(settings);
        request.setAutoCreateTopics(false);
        return request;
    }

    @Test
    @DisplayName("Тот же топик в том же направлении отклоняется, в том числе при другом регистре ЦОД")
    void testSameTopicSameDirectionIsRejected() {
        existing("job-a", "dc1", "dc2", "UNIDIRECTIONAL", "ACTIVE", "orders", "payments");
        JobOverlapException e = assertThrows(JobOverlapException.class,
                () -> service.createJob(request("DC1", "dc2", "UNIDIRECTIONAL", "payments", "audit")));
        assertTrue(e.getMessage().contains("[payments]") && e.getMessage().contains("job-a"), e.getMessage());
    }

    @Test
    @DisplayName("BIDIRECTIONAL пересекается с однонаправленной задачей в обратную сторону")
    void testBidirectionalCoversReverseLeg() {
        existing("job-bi", "dc1", "dc2", "BIDIRECTIONAL", "ACTIVE", "orders");
        assertThrows(JobOverlapException.class,
                () -> service.createJob(request("dc2", "dc1", "UNIDIRECTIONAL", "orders")));
    }

    @Test
    @DisplayName("Другой топик, другое направление или другой ЦОД — не пересечение")
    void testDisjointJobsAreAllowed() {
        existing("job-a", "dc1", "dc2", "UNIDIRECTIONAL", "ACTIVE", "orders");
        assertDoesNotThrow(() -> service.createJob(request("dc1", "dc2", "UNIDIRECTIONAL", "audit")));
        assertDoesNotThrow(() -> service.createJob(request("dc2", "dc1", "UNIDIRECTIONAL", "orders")));
        assertDoesNotThrow(() -> service.createJob(request("dc1", "dc3", "UNIDIRECTIONAL", "orders")));
    }

    @Test
    @DisplayName("Задача на паузе мешает созданию, а ее возобновление проверяется по активным")
    void testPausedJobBlocksCreationAndResumeChecksActive() {
        existing("job-paused", "dc1", "dc2", "UNIDIRECTIONAL", "PAUSED", "orders");
        assertThrows(JobOverlapException.class,
                () -> service.createJob(request("dc1", "dc2", "UNIDIRECTIONAL", "orders")));

        existing("job-active", "dc1", "dc2", "UNIDIRECTIONAL", "ACTIVE", "orders");
        assertThrows(JobOverlapException.class, () -> service.resumeJob("job-paused"),
                "Возобновление при активной задаче с тем же топиком дублировало бы записи");

        jobs.removeIf(j -> j.getId().equals("job-active"));
        assertTrue(service.resumeJob("job-paused"));
        verify(repository).updateStatus(eq("job-paused"), eq("ACTIVE"), any());
        assertEquals(Optional.empty(), service.findOverlap("job-paused", "dc1", "dc2", "UNIDIRECTIONAL",
                List.of("orders"), true));
    }
}
