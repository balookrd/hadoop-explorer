package io.github.balookrd.pristan.sql.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.github.balookrd.pristan.sql.config.SqlProperties;
import io.github.balookrd.pristan.sql.model.QueryHistory;
import io.github.balookrd.pristan.sql.repository.QueryHistoryRepository;
import io.github.balookrd.pristan.sql.service.engine.SqlEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
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
    private QueryEventHub hub;
    private QueryHistoryRepository repo;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        history = new QueryHistory();
        repo = mock(QueryHistoryRepository.class);
        when(repo.findById(anyString())).thenAnswer(i -> Optional.of(history));
        when(repo.save(any(QueryHistory.class))).thenAnswer(i -> {
            history = i.getArgument(0);
            return history;
        });
        engine = mock(SqlEngine.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getEngine(any())).thenReturn(engine);
        hub = new QueryEventHub();
        service = new QueryManagerService(repo, clusterService, new SqlProperties(),
                new ObjectMapper(), new SqlQueryMetrics(registry), hub);
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
    @SuppressWarnings("unchecked")
    private static Consumer<Map<String, Object>> events(org.mockito.invocation.InvocationOnMock invocation) {
        return (Consumer<Map<String, Object>>) invocation.getArgument(4);
    }

    @Test
    void brokenQueryStreamSubscribersDoNotTurnFinishedIntoFailed() throws Exception {
        List<Map<String, Object>> received = new CopyOnWriteArrayList<>();
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any())).thenAnswer(invocation -> {
            String queryId = invocation.getArgument(0);
            SseEmitter dead = service.registerQueryStream(queryId, "analyst", false).orElseThrow();
            dead.complete(); // send() throws IllegalStateException
            hub.subscribe(queryId, e -> {
                throw new IllegalStateException("subscriber gone");
            });
            hub.subscribe(queryId, received::add);
            events(invocation).accept(Map.of("type", "rows", "rows", List.of(), "total_rows", 0));
            events(invocation).accept(Map.of("type", "finished", "status", "FINISHED"));
            return result("FINISHED");
        });

        service.submitQuery(cluster, "analyst", "SELECT 1");
        awaitStatus("FINISHED");

        assertEquals(1, count("FINISHED"));
        assertEquals(0, count("FAILED"));
        assertEquals(List.of("rows", "finished"), received.stream().map(e -> e.get("type")).toList());
    }

    @Test
    void cancelledQueryEndsWithFinishedEventInsteadOfStatus() throws Exception {
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any()))
                .thenReturn(result("CANCELLED"));

        String queryId = service.submitQuery(cluster, "analyst", "SELECT 1");
        awaitStatus("CANCELLED");
        List<Map<String, Object>> replay = new CopyOnWriteArrayList<>();
        assertTrue(hub.subscribe(queryId, replay::add));

        assertEquals(1, replay.size());
        assertEquals("finished", replay.get(0).get("type"));
        assertEquals("CANCELLED", replay.get(0).get("status"));
        assertEquals(1, count("CANCELLED"));
    }

    @Test
    void failedQueryEndsWithErrorEventCarryingErrorAndMessage() throws Exception {
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any()))
                .thenThrow(new IllegalArgumentException("boom"));

        String queryId = service.submitQuery(cluster, "analyst", "SELECT 1");
        awaitStatus("FAILED");
        List<Map<String, Object>> replay = new CopyOnWriteArrayList<>();
        assertTrue(hub.subscribe(queryId, replay::add));

        assertEquals(1, replay.size());
        assertEquals("error", replay.get(0).get("type"));
        assertEquals("FAILED", replay.get(0).get("status"));
        assertEquals("boom", replay.get(0).get("error"));
        assertEquals("boom", replay.get(0).get("message"));
        assertEquals(1, count("FAILED"));
    }

    @Test
    void engineErrorIsRecordedAsFailedWithTerminalEvent() throws Exception {
        // An Error (not an Exception) from the engine must not leave the query RUNNING without a terminal event
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any()))
                .thenThrow(new NoClassDefFoundError("org/apache/hive/jdbc/HiveDriver"));

        String queryId = service.submitQuery(cluster, "analyst", "SELECT 1");
        awaitStatus("FAILED");
        List<Map<String, Object>> replay = awaitReplayEndingWith(queryId, "error");

        assertEquals("NoClassDefFoundError: org/apache/hive/jdbc/HiveDriver", replay.get(replay.size() - 1).get("error"));
        assertEquals("NoClassDefFoundError: org/apache/hive/jdbc/HiveDriver", history.getErrorMessage());
        assertEquals(1, count("FAILED"));
    }

    @Test
    void virtualMachineErrorIsRecordedBeforeItIsRethrown() throws Exception {
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any()))
                .thenThrow(new StackOverflowError());

        String queryId = service.submitQuery(cluster, "analyst", "SELECT 1");
        awaitStatus("FAILED");
        List<Map<String, Object>> replay = awaitReplayEndingWith(queryId, "error");

        assertEquals("StackOverflowError", replay.get(replay.size() - 1).get("error"));
        assertEquals(1, count("FAILED"));
    }

    private List<Map<String, Object>> awaitReplayEndingWith(String queryId, String type) throws Exception {
        for (int i = 0; i < 100; i++) {
            List<Map<String, Object>> replay = new CopyOnWriteArrayList<>();
            hub.subscribe(queryId, replay::add);
            if (!replay.isEmpty() && type.equals(replay.get(replay.size() - 1).get("type"))) {
                return replay;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no terminal event " + type + " for " + queryId);
    }

    @Test
    void historyWriteFailureStillPublishesErrorAndClosesChannel() throws Exception {
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any()))
                .thenThrow(new IllegalArgumentException("boom"));
        when(repo.save(argThat(h -> h != null && "FAILED".equals(h.getStatus()))))
                .thenThrow(new IllegalStateException("database is down"));

        String queryId = service.submitQuery(cluster, "analyst", "SELECT 1");
        List<Map<String, Object>> replay = awaitReplayEndingWith(queryId, "error");

        assertEquals("boom", replay.get(replay.size() - 1).get("error"));
        // Closed channel: later events are ignored and new subscribers are not kept
        hub.publish(queryId, Map.of("type", "status", "status", "RUNNING"));
        List<Map<String, Object>> after = new CopyOnWriteArrayList<>();
        hub.subscribe(queryId, after::add);
        assertEquals(replay.size(), after.size());
        assertEquals(1, count("FAILED"));
    }

    @Test
    void cancellingFinishedQueryKeepsFinished() throws Exception {
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any()))
                .thenReturn(result("FINISHED"));
        history.setUsername("analyst");

        String queryId = service.submitQuery(cluster, "analyst", "SELECT 1");
        awaitStatus("FINISHED");

        assertTrue(service.cancelQuery(queryId, "analyst", false));
        assertEquals("FINISHED", history.getStatus());
        verify(repo, never()).markCancelledIfActive(anyString(), any());
    }

    @Test
    void cancellingRunningQueryUsesConditionalUpdate() throws Exception {
        java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
        when(engine.execute(anyString(), anyString(), anyString(), anyInt(), any(), any())).thenAnswer(invocation -> {
            java.util.concurrent.atomic.AtomicBoolean cancel = invocation.getArgument(5);
            started.countDown();
            while (!cancel.get()) {
                Thread.sleep(10);
            }
            return result("CANCELLED");
        });

        String queryId = service.submitQuery(cluster, "analyst", "SELECT 1");
        assertTrue(started.await(5, java.util.concurrent.TimeUnit.SECONDS));
        history.setUsername("analyst");

        assertTrue(service.cancelQuery(queryId, "analyst", false));
        verify(repo, timeout(2000)).markCancelledIfActive(eq(queryId), any());
        awaitStatus("CANCELLED");
        assertEquals(1, count("CANCELLED"));
    }

    @Test
    void cancellingOrphanedRunningQueryMarksItCancelled() {
        // RUNNING record left by a previous process (or running on another instance): not in activeQueries
        history.setId("orphan");
        history.setUsername("analyst");
        history.setStatus("RUNNING");
        when(repo.markCancelledIfActive(eq("orphan"), any())).thenAnswer(i -> {
            history.setStatus("CANCELLED");
            return 1;
        });

        assertTrue(service.cancelQuery("orphan", "analyst", false));

        verify(repo).markCancelledIfActive(eq("orphan"), any());
        assertEquals("CANCELLED", history.getStatus());
    }
}
