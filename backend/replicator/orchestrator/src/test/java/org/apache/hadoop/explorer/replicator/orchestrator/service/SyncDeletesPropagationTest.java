package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Флаг репликации удалений должен доходить от интерфейса до агента.
 *
 * <p>Поле {@code syncDeletes} существовало только в DTO, которое оркестратор отдает агентам, но
 * ни интерфейс, ни REST, ни сущность задания его не знали — значит включить его было нечем, и
 * условие {@code if (job.isSyncDeletes())} в обработчике inotify никогда не выполнялось.
 *
 * <p>Удаление данных на приемнике необратимо, поэтому по умолчанию флаг выключен, и существующие
 * задания после обновления удалять ничего не начинают.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Репликация удалений: флаг задания")
class SyncDeletesPropagationTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private AgentRegistry agentRegistry;

    @BeforeEach
    void setUp() {
        jobRepository.deleteAll();
        // Потоковое задание требует живого inotify-стримера на кластере источника
        agentRegistry.register(new AgentRegisterRequest(
                "streamer-sync-deletes", "dc1", "streamer", "streamer-dc1:50051", "streamer-dc1", 100.0),
                "test-orchestrator-agent-secret-0123456789");
    }

    @AfterEach
    void tearDown() {
        jobRepository.deleteAll();
        agentRegistry.clear();
    }

    private CreateJobRequest streamingJob(Boolean syncDeletes) {
        return new CreateJobRequest(
                "/warehouse/sales", "/backup/warehouse/sales", "dc1", "dc2",
                0L, "hdfs@REALM.LOCAL", true, false,
                null, 20, "STANDARD", null, "STREAMING_INOTIFY", syncDeletes);
    }

    @Test
    @DisplayName("Включенный флаг сохраняется и отдается в API")
    void testEnabledFlagIsStoredAndReturned() {
        JobResponse created = jobService.createJob(streamingJob(true), "operator");

        assertTrue(created.syncDeletes(),
                "Флаг обязан сохраняться: иначе включить репликацию удалений нечем");
        assertTrue(jobRepository.findById(created.id()).orElseThrow().isSyncDeletes(),
                "Флаг обязан попадать в базу, а не только в ответ");
    }

    @Test
    @DisplayName("По умолчанию репликация удалений выключена")
    void testFlagIsDisabledByDefault() {
        JobResponse withoutFlag = jobService.createJob(streamingJob(null), "operator");

        assertFalse(withoutFlag.syncDeletes(),
                "Удаление данных на приемнике необратимо: по умолчанию оно должно быть выключено");
    }
}
