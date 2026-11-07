package org.apache.hadoop.explorer.sql.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.model.QueryHistory;
import org.apache.hadoop.explorer.sql.repository.QueryHistoryRepository;
import org.apache.hadoop.explorer.sql.service.engine.SqlEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryManagerServiceMetricsTest {

    private SimpleMeterRegistry registry;
    private QueryHistory history;
    private SqlEngine engine;
    private QueryManagerService service;
    private SqlProperties.ClusterConfig cluster;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        history = new QueryHistory();
        QueryHistoryRepository repo = mock(QueryHistoryRepository.class);
        when(repo.findById(anyString())).thenAnswer(i -> Optional.of(history));
        when(repo.save(any(QueryHistory.class))).thenAnswer(i -> {
            history = i.getArgument(0);
            return history;
        });
        engine = mock(SqlEngine.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getEngine(any())).thenReturn(engine);
        service = new QueryManagerService(repo, clusterService, new SqlProperties(),
                new ObjectMapper(), new SqlQueryMetrics(registry));
        cluster = new SqlProperties.ClusterConfig();
        cluster.setId("trino-prod");
        cluster.setType("trino");
    }

    private long count(String status) {
        Timer t = registry.find(SqlQueryMetrics.QUERY_DURATION_METRIC).tag("status", status).timer();
        return t == null ? 0 : t.count();
    }

    private void awaitStatus(String status) throws Exception {
        for (int i = 0; i < 100 && !status.equals(history.getStatus()); i++) {
            Thread.sleep(50);
        }
        Thread.sleep(300); // let the task finish notifications and any erroneous extra recording
        assertEquals(status, history.getStatus());
    }

    private SqlEngine.ExecutionResult result(String status) {
        return new SqlEngine.ExecutionResult(List.of(), List.of(), 0, 10.0, status, null);
    }

    @Test
    void brokenUserEmitterDoesNotTurnFinishedIntoFailed() throws Exception {
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any()))
                .thenReturn(result("FINISHED"));
        SseEmitter emitter = service.registerUserStream("analyst");
        emitter.complete(); // later send() throws IllegalStateException

        service.submitQuery(cluster, "analyst", "SELECT 1");
        awaitStatus("FINISHED");

        assertEquals(1, count("FINISHED"));
        assertEquals(0, count("FAILED"));
    }

    @Test
    void cancelledQueryIsRecordedOnce() throws Exception {
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any()))
                .thenReturn(result("CANCELLED"));
        SseEmitter emitter = service.registerUserStream("analyst");
        emitter.complete();

        service.submitQuery(cluster, "analyst", "SELECT 1");
        awaitStatus("CANCELLED");

        assertEquals(1, count("CANCELLED"));
        assertEquals(0, count("FAILED"));
    }

    @Test
    void engineExceptionIsRecordedAsFailedOnce() throws Exception {
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any()))
                .thenThrow(new IllegalArgumentException("boom"));

        service.submitQuery(cluster, "analyst", "SELECT 1");
        awaitStatus("FAILED");

        assertEquals(1, count("FAILED"));
        assertEquals(0, count("FINISHED"));
    }
}
