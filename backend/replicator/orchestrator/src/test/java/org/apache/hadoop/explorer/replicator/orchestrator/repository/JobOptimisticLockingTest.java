package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Оптимистичная блокировка задач репликации.
 *
 * <p>Статус, счетчики и сообщение задачи обновляются read-modify-write одновременно из
 * gRPC-вызовов агентов, планировщиков и REST, причем с нескольких узлов active-active.
 * Без {@code @Version} последняя запись молча затирает предыдущую: два параллельных
 * {@code failTask} увеличивают счетчик ошибок на единицу вместо двух, а
 * {@code finalizeJobCompletion} и {@code failoverTask} перетирают статус друг друга
 * (COMPLETED поверх FAILED).
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Оптимистичная блокировка задач репликации")
class JobOptimisticLockingTest {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @BeforeEach
    void setup() {
        jobRepository.deleteAll();
        hmsJobRepository.deleteAll();
    }

    private JobEntity createJob() {
        JobEntity job = new JobEntity();
        job.setId("job-locking-1");
        job.setSourceClusterId("prod-a");
        job.setTargetClusterId("prod-b");
        job.setSourcePath("/warehouse/sales");
        job.setTargetPath("/backup/warehouse/sales");
        job.setStatus("RUNNING");
        job.setFailedObjects(0);
        job.setCreatedBy("tester");
        return jobRepository.saveAndFlush(job);
    }

    private HmsReplicationJobEntity createHmsJob() {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId("hms-job-locking-1");
        job.setSourceClusterId("prod-a");
        job.setTargetClusterId("prod-b");
        job.setSourceDbName("analytics");
        job.setTargetDbName("analytics");
        job.setStatus("ACTIVE");
        job.setCreatedBy("tester");
        return hmsJobRepository.saveAndFlush(job);
    }

    @Test
    @DisplayName("Конкурентная запись по устаревшей версии задачи отклоняется")
    void testStaleJobWriteIsRejected() {
        String id = createJob().getId();

        JobEntity readerOne = jobRepository.findById(id).orElseThrow();
        JobEntity readerTwo = jobRepository.findById(id).orElseThrow();
        jobRepository.saveAndFlush(detach(readerOne, "FAILED", 1));

        assertThrows(ObjectOptimisticLockingFailureException.class,
                () -> jobRepository.saveAndFlush(detach(readerTwo, "COMPLETED", 0)),
                "Запись по устаревшей версии обязана быть отклонена, иначе COMPLETED затирает FAILED");

        assertEquals("FAILED", jobRepository.findById(id).orElseThrow().getStatus());
    }

    @Test
    @DisplayName("Конкурентная запись по устаревшей версии HMS-задачи отклоняется")
    void testStaleHmsJobWriteIsRejected() {
        String id = createHmsJob().getId();

        HmsReplicationJobEntity readerOne = hmsJobRepository.findById(id).orElseThrow();
        HmsReplicationJobEntity readerTwo = hmsJobRepository.findById(id).orElseThrow();

        readerOne.setStatus("STOPPED");
        hmsJobRepository.saveAndFlush(detachHms(readerOne));

        readerTwo.setStatus("ACTIVE");
        assertThrows(ObjectOptimisticLockingFailureException.class,
                () -> hmsJobRepository.saveAndFlush(detachHms(readerTwo)),
                "Устаревшая запись не должна возвращать остановленную задачу в работу");
    }

    @Test
    @DisplayName("Последовательные обновления задачи проходят штатно")
    void testSequentialUpdatesSucceed() {
        String id = createJob().getId();

        JobEntity job = jobRepository.findById(id).orElseThrow();
        job.setStatus("COMPLETED");
        jobRepository.saveAndFlush(job);

        JobEntity reloaded = jobRepository.findById(id).orElseThrow();
        reloaded.setFailedObjects(3);
        jobRepository.saveAndFlush(reloaded);

        JobEntity finalState = jobRepository.findById(id).orElseThrow();
        assertEquals("COMPLETED", finalState.getStatus());
        assertEquals(3, finalState.getFailedObjects());
    }

    /** Копия сущности с той же версией: имитирует параллельный узел, прочитавший задачу раньше. */
    private static JobEntity detach(JobEntity source, String status, int failedObjects) {
        JobEntity copy = new JobEntity();
        copy.setId(source.getId());
        copy.setSourceClusterId(source.getSourceClusterId());
        copy.setTargetClusterId(source.getTargetClusterId());
        copy.setSourcePath(source.getSourcePath());
        copy.setTargetPath(source.getTargetPath());
        copy.setCreatedBy(source.getCreatedBy());
        copy.setVersion(source.getVersion());
        copy.setStatus(status);
        copy.setFailedObjects(failedObjects);
        return copy;
    }

    private static HmsReplicationJobEntity detachHms(HmsReplicationJobEntity source) {
        HmsReplicationJobEntity copy = new HmsReplicationJobEntity();
        copy.setId(source.getId());
        copy.setSourceClusterId(source.getSourceClusterId());
        copy.setTargetClusterId(source.getTargetClusterId());
        copy.setSourceDbName(source.getSourceDbName());
        copy.setTargetDbName(source.getTargetDbName());
        copy.setCreatedBy(source.getCreatedBy());
        copy.setVersion(source.getVersion());
        copy.setStatus(source.getStatus());
        return copy;
    }
}
