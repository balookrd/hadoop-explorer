package io.github.balookrd.pristan.iskra.repository;

import io.github.balookrd.pristan.iskra.model.SparkExecutionHistory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@DisplayName("Условная отмена выполнения в spark_execution_history")
class SparkExecutionHistoryRepositoryTest {

    @Autowired
    private SparkExecutionHistoryRepository repository;

    private String save(String status) {
        SparkExecutionHistory h = new SparkExecutionHistory();
        h.setId(UUID.randomUUID().toString());
        h.setSessionId("s1");
        h.setUsername("analyst");
        h.setClusterId("c1");
        h.setLanguage("pyspark");
        h.setCode("spark.range(1)");
        h.setStatus(status);
        h.setCreatedAt(Instant.now());
        repository.save(h);
        return h.getId();
    }

    @Test
    @DisplayName("RUNNING переводится в CANCELLED")
    void runningExecutionIsCancelled() {
        String id = save("RUNNING");

        assertEquals(1, repository.markCancelledIfActive(id, Instant.now()));
        SparkExecutionHistory h = repository.findById(id).orElseThrow();
        assertEquals("CANCELLED", h.getStatus());
        assertNotNull(h.getFinishedAt());
    }

    @Test
    @DisplayName("Терминальный статус FINISHED не перезаписывается")
    void finishedExecutionIsNotTouched() {
        String id = save("FINISHED");

        assertEquals(0, repository.markCancelledIfActive(id, Instant.now()));
        assertEquals("FINISHED", repository.findById(id).orElseThrow().getStatus());
    }

    @Test
    @DisplayName("Итог записывается в RUNNING-выполнение")
    void outcomeIsWrittenToRunningExecution() {
        String id = save("RUNNING");

        assertEquals(1, repository.completeIfActive(id, "FINISHED", 8, 12.5, "[]", "log", null, true, Instant.now()));
        SparkExecutionHistory h = repository.findById(id).orElseThrow();
        assertEquals("FINISHED", h.getStatus());
        assertEquals(8, h.getRowsCount());
        assertEquals(12.5, h.getExecutionTimeMs());
        assertEquals("[]", h.getColumnsJson());
        assertEquals("log", h.getLogs());
        assertTrue(h.isHasCachedResult());
        assertNotNull(h.getFinishedAt());
    }

    @Test
    @DisplayName("Итог не перезаписывает принятую отмену")
    void outcomeDoesNotOverwriteCancel() {
        String id = save("RUNNING");
        repository.markCancelledIfActive(id, Instant.now());

        assertEquals(0, repository.completeIfActive(id, "FINISHED", 8, 12.5, "[]", "log", null, true, Instant.now()));
        SparkExecutionHistory h = repository.findById(id).orElseThrow();
        assertEquals("CANCELLED", h.getStatus());
        assertEquals(0, h.getRowsCount());
    }

    @Test
    @DisplayName("RUNNING ставится только из QUEUED")
    void runningIsSetOnlyFromQueued() {
        String queued = save("QUEUED");
        String cancelled = save("CANCELLED");

        assertEquals(1, repository.markRunningIfQueued(queued, Instant.now()));
        assertEquals("RUNNING", repository.findById(queued).orElseThrow().getStatus());
        assertEquals(0, repository.markRunningIfQueued(cancelled, Instant.now()));
        assertEquals("CANCELLED", repository.findById(cancelled).orElseThrow().getStatus());
    }
}
