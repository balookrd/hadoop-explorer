package io.github.balookrd.pristan.sql.repository;

import io.github.balookrd.pristan.sql.model.QueryHistory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@SpringBootTest
@DisplayName("Условная отмена запроса в query_history")
class QueryHistoryRepositoryTest {

    @Autowired
    private QueryHistoryRepository repository;

    private String save(String status) {
        QueryHistory h = new QueryHistory();
        h.setId(UUID.randomUUID().toString());
        h.setUsername("analyst");
        h.setClusterId("c1");
        h.setClusterName("Cluster 1");
        h.setEngineType("trino");
        h.setQueryText("SELECT 1");
        h.setStatus(status);
        h.setInQueue(true);
        h.setCreatedAt(Instant.now());
        repository.save(h);
        return h.getId();
    }

    @Test
    @DisplayName("RUNNING переводится в CANCELLED и убирается из очереди")
    void runningQueryIsCancelled() {
        String id = save("RUNNING");

        assertEquals(1, repository.markCancelledIfActive(id, Instant.now()));
        QueryHistory h = repository.findById(id).orElseThrow();
        assertEquals("CANCELLED", h.getStatus());
        assertFalse(h.isInQueue());
    }

    @Test
    @DisplayName("Терминальный статус FINISHED не перезаписывается")
    void finishedQueryIsNotTouched() {
        String id = save("FINISHED");

        assertEquals(0, repository.markCancelledIfActive(id, Instant.now()));
        assertEquals("FINISHED", repository.findById(id).orElseThrow().getStatus());
    }
}
