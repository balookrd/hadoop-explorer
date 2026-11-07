package org.apache.hadoop.explorer.spark.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.hadoop.explorer.spark.config.SparkProperties;
import org.apache.hadoop.explorer.spark.model.SparkExecutionHistory;
import org.apache.hadoop.explorer.spark.model.SparkSessionRecord;
import org.apache.hadoop.explorer.spark.repository.SparkExecutionHistoryRepository;
import org.apache.hadoop.explorer.spark.repository.SparkSessionRepository;
import org.apache.hadoop.explorer.spark.service.engine.LivyClient;
import org.apache.hadoop.explorer.spark.service.engine.MockSparkEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SparkSessionManagerMetricsTest {

    private SimpleMeterRegistry registry;
    private SparkExecutionHistory history;
    private SparkSessionRecord session;
    private MockSparkEngine engine;
    private SparkSessionManager manager;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        session = new SparkSessionRecord();
        session.setId("s1");
        session.setUsername("analyst");
        session.setClusterId("prod-analytics");
        session.setKind("pyspark");
        session.setStatus("idle");

        SparkSessionRepository sessionRepo = mock(SparkSessionRepository.class);
        when(sessionRepo.findById("s1")).thenAnswer(i -> Optional.of(session));
        when(sessionRepo.save(any(SparkSessionRecord.class))).thenAnswer(i -> i.getArgument(0));

        SparkExecutionHistoryRepository historyRepo = mock(SparkExecutionHistoryRepository.class);
        when(historyRepo.findById(anyString())).thenAnswer(i -> Optional.ofNullable(history));
        when(historyRepo.save(any(SparkExecutionHistory.class))).thenAnswer(i -> {
            history = i.getArgument(0);
            return history;
        });

        engine = mock(MockSparkEngine.class);
        LivyClient livy = mock(LivyClient.class);
        when(livy.getMockEngine()).thenReturn(engine);

        manager = new SparkSessionManager(sessionRepo, historyRepo, livy, new SparkProperties(),
                new ObjectMapper(), new SparkMetrics(registry, sessionRepo));
    }

    private long count(String status) {
        Timer t = registry.find(SparkMetrics.STATEMENT_DURATION_METRIC).tag("status", status).timer();
        return t == null ? 0 : t.count();
    }

    private void awaitHistoryStatus(String status) throws Exception {
        for (int i = 0; i < 100 && (history == null || !status.equals(history.getStatus())); i++) {
            Thread.sleep(50);
        }
        Thread.sleep(300); // let the task finish and expose any erroneous extra recording
        assertEquals(status, history.getStatus());
    }

    private static MockSparkEngine.ExecutionResult result(String status) {
        return new MockSparkEngine.ExecutionResult(List.of(), List.of(), 0, "", 10.0, status, null);
    }

    @Test
    void completedEmitterDoesNotTurnFinishedIntoFailed() throws Exception {
        when(engine.executeCode(anyString(), anyString(), any(), anyString(), any(), any()))
                .thenAnswer(i -> {
                    // A subscriber that has already completed: send() throws IllegalStateException
                    manager.registerStream(i.getArgument(0)).complete();
                    Consumer<Map<String, Object>> events = i.getArgument(4);
                    events.accept(Map.of("type", "status", "status", "RUNNING"));
                    return result("FINISHED");
                });

        manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        awaitHistoryStatus("FINISHED");

        assertEquals(1, count("FINISHED"));
        assertEquals(0, count("FAILED"));
        assertEquals("idle", session.getStatus());
    }

    @Test
    void cancelledResultIsRecordedOnce() throws Exception {
        when(engine.executeCode(anyString(), anyString(), any(), anyString(), any(), any()))
                .thenReturn(result("CANCELLED"));

        manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        awaitHistoryStatus("CANCELLED");

        assertEquals(1, count("CANCELLED"));
        assertEquals(0, count("FAILED"));
    }

    @Test
    void engineExceptionIsRecordedAsFailedOnce() throws Exception {
        when(engine.executeCode(anyString(), anyString(), any(), anyString(), any(), any()))
                .thenThrow(new IllegalArgumentException("boom"));

        manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        awaitHistoryStatus("FAILED");

        assertEquals(1, count("FAILED"));
        assertEquals(0, count("FINISHED"));
    }
}
