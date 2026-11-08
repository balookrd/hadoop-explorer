package org.apache.hadoop.explorer.spark.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.hadoop.explorer.spark.config.SparkProperties;
import org.apache.hadoop.explorer.spark.model.SparkExecutionHistory;
import org.apache.hadoop.explorer.spark.model.SparkSessionRecord;
import org.apache.hadoop.explorer.spark.repository.SparkExecutionHistoryRepository;
import org.apache.hadoop.explorer.spark.repository.SparkSessionRepository;
import org.apache.hadoop.explorer.spark.service.engine.SparkEngine;
import org.apache.hadoop.explorer.spark.service.engine.SparkEngineRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SparkSessionManagerMetricsTest {

    private SimpleMeterRegistry registry;
    private SparkExecutionHistory history;
    private SparkSessionRecord session;
    private SparkEngine engine;
    private SparkExecutionHistoryRepository historyRepo;
    private SparkSessionManager manager;
    private SparkSessionRepository sessionRepo;
    /** Simulates a database outage for the terminal write. */
    private volatile boolean historyDown;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        session = new SparkSessionRecord();
        session.setId("s1");
        session.setUsername("analyst");
        session.setClusterId("prod-analytics");
        session.setKind("pyspark");
        session.setStatus("idle");

        sessionRepo = mock(SparkSessionRepository.class);
        when(sessionRepo.findById("s1")).thenAnswer(i -> Optional.of(session));
        when(sessionRepo.save(any(SparkSessionRecord.class))).thenAnswer(i -> i.getArgument(0));
        // Same transition as the JPQL update in production: a stopped or dead session is not revived
        when(sessionRepo.markIdleIfActive(eq("s1"), any())).thenAnswer(i -> {
            if ("dead".equals(session.getStatus()) || "killed".equals(session.getStatus())) {
                return 0;
            }
            session.setStatus("idle");
            return 1;
        });

        historyRepo = mock(SparkExecutionHistoryRepository.class);
        when(historyRepo.findById(anyString())).thenAnswer(i -> Optional.ofNullable(history));
        when(historyRepo.save(any(SparkExecutionHistory.class))).thenAnswer(i -> {
            history = i.getArgument(0);
            return history;
        });

        // Same transition as the JPQL update in production: only QUEUED/RUNNING become CANCELLED
        when(historyRepo.markCancelledIfActive(anyString(), any())).thenAnswer(i -> {
            if (history != null && ("QUEUED".equals(history.getStatus()) || "RUNNING".equals(history.getStatus()))) {
                history.setStatus("CANCELLED");
                return 1;
            }
            return 0;
        });
        when(historyRepo.markRunningIfQueued(anyString(), any())).thenAnswer(i -> {
            if (history != null && "QUEUED".equals(history.getStatus())) {
                history.setStatus("RUNNING");
                return 1;
            }
            return 0;
        });
        // Same condition as the JPQL terminal write: only a QUEUED/RUNNING execution takes the outcome
        when(historyRepo.completeIfActive(anyString(), anyString(), anyLong(), anyDouble(), any(), any(), any(),
                anyBoolean(), any())).thenAnswer(i -> {
            if (historyDown) {
                throw new IllegalStateException("database is down");
            }
            if (history != null && ("QUEUED".equals(history.getStatus()) || "RUNNING".equals(history.getStatus()))) {
                history.setRowsCount(i.getArgument(2));
                history.setErrorMessage(i.getArgument(6));
                history.setStatus(i.getArgument(1));
                return 1;
            }
            return 0;
        });

        engine = mock(SparkEngine.class);
        SparkEngineRegistry engines = mock(SparkEngineRegistry.class);
        when(engines.forCluster(any())).thenReturn(engine);

        SparkProperties.SparkClusterConfig cluster = new SparkProperties.SparkClusterConfig();
        cluster.setId("prod-analytics");
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.findCluster("prod-analytics")).thenReturn(Optional.of(cluster));

        manager = new SparkSessionManager(sessionRepo, historyRepo, engines, clusterService, new SparkProperties(),
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

    private static SparkEngine.ExecutionResult result(String status) {
        return new SparkEngine.ExecutionResult(List.of(), List.of(), 0, "", 10.0, status, null);
    }

    @Test
    void completedEmitterDoesNotTurnFinishedIntoFailed() throws Exception {
        when(engine.execute(anyInt(), anyString(), anyString(), any(), anyString(), any(), any()))
                .thenAnswer(i -> {
                    // A subscriber that has already completed: send() throws IllegalStateException
                    manager.registerStream(i.getArgument(1), "analyst", false).orElseThrow().complete();
                    Consumer<Map<String, Object>> events = i.getArgument(5);
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
        when(engine.execute(anyInt(), anyString(), anyString(), any(), anyString(), any(), any()))
                .thenReturn(result("CANCELLED"));

        manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        awaitHistoryStatus("CANCELLED");

        assertEquals(1, count("CANCELLED"));
        assertEquals(0, count("FAILED"));
    }

    @Test
    void engineExceptionIsRecordedAsFailedOnce() throws Exception {
        when(engine.execute(anyInt(), anyString(), anyString(), any(), anyString(), any(), any()))
                .thenThrow(new IllegalArgumentException("boom"));

        manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        awaitHistoryStatus("FAILED");

        assertEquals(1, count("FAILED"));
        assertEquals(0, count("FINISHED"));
    }

    @Test
    void engineErrorIsRecordedAsFailedAndFreesSession() throws Exception {
        CountDownLatch subscribed = new CountDownLatch(1);
        when(engine.execute(anyInt(), anyString(), anyString(), any(), anyString(), any(), any()))
                .thenAnswer(i -> {
                    assertTrue(subscribed.await(5, TimeUnit.SECONDS));
                    // A broken classpath in the HTTP client stack surfaces as an Error, not an Exception
                    throw new NoClassDefFoundError("org/springframework/web/client/RestClient");
                });

        String executionId = manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        List<String> received = new java.util.concurrent.CopyOnWriteArrayList<>();
        registerEmitter(executionId, recordingEmitter(received));
        subscribed.countDown();
        awaitHistoryStatus("FAILED");
        await(() -> !finishedEvents(received).isEmpty(), "no terminal event: " + received);

        assertEquals(1, count("FAILED"));
        assertEquals("idle", session.getStatus());
        assertTrue(history.getErrorMessage().contains("NoClassDefFoundError"), history.getErrorMessage());
        assertTrue(received.stream().anyMatch(d -> d.contains("\"status\":\"FAILED\"")), () -> "events: " + received);
    }

    @Test
    void virtualMachineErrorIsRecordedAsFailedOnce() throws Exception {
        when(engine.execute(anyInt(), anyString(), anyString(), any(), anyString(), any(), any()))
                .thenThrow(new StackOverflowError());

        manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        awaitHistoryStatus("FAILED");

        assertEquals(1, count("FAILED"));
        assertEquals("idle", session.getStatus());
    }

    /** Waits for the condition instead of a fixed sleep; fails with the message if it never holds. */
    private static void await(java.util.function.BooleanSupplier condition, String message) throws Exception {
        for (int i = 0; i < 200 && !condition.getAsBoolean(); i++) {
            Thread.sleep(25);
        }
        assertTrue(condition.getAsBoolean(), message);
    }

    /** Engine that streams its own "finished FINISHED" event (as MockSparkEngine and Livy do) and returns FINISHED. */
    private final CountDownLatch subscriberReady = new CountDownLatch(1);

    private void engineFinishesAfter(CountDownLatch started, CountDownLatch release) {
        when(engine.execute(anyInt(), anyString(), anyString(), any(), anyString(), any(), any()))
                .thenAnswer(i -> {
                    // Tests that do not subscribe never count down; the engine then streams to nobody
                    subscriberReady.await(1, TimeUnit.SECONDS);
                    Consumer<Map<String, Object>> events = i.getArgument(5);
                    events.accept(Map.of("type", "status", "status", "RUNNING"));
                    events.accept(Map.of("type", "finished", "status", "FINISHED", "total_rows", 1));
                    started.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                    return new SparkEngine.ExecutionResult(List.of(), List.of(List.of(1)), 1, "", 10.0, "FINISHED", null);
                });
    }

    private List<String> finishedEvents(List<String> events) {
        return events.stream().filter(e -> e.contains("\"type\":\"finished\"")).toList();
    }

    @Test
    void cancelAcceptedBeforeFinalWriteMakesEveryChannelCancelled() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        engineFinishesAfter(started, release);
        List<String> events = new java.util.concurrent.CopyOnWriteArrayList<>();

        String executionId = manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        registerEmitter(executionId, recordingEmitter(events));
        subscriberReady.countDown();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        assertTrue(manager.cancelExecution(executionId, "analyst", false));
        release.countDown();
        await(() -> finishedEvents(events).size() == 1, "no terminal event: " + events);

        assertEquals("CANCELLED", history.getStatus());
        var result = manager.getResult(executionId, "analyst", false, 0, 10).orElseThrow();
        assertEquals("CANCELLED", result.status());
        assertEquals(List.of(), result.rows());
        assertEquals(1, count("CANCELLED"));
        assertEquals(0, count("FINISHED"));
        assertTrue(finishedEvents(events).get(0).contains("\"status\":\"CANCELLED\""), () -> "events: " + events);
    }

    @Test
    void cancelAfterFinalWriteKeepsEveryChannelFinished() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        engineFinishesAfter(started, release);
        List<String> events = new java.util.concurrent.CopyOnWriteArrayList<>();

        String executionId = manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        registerEmitter(executionId, recordingEmitter(events));
        subscriberReady.countDown();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        release.countDown();
        await(() -> finishedEvents(events).size() == 1, "no terminal event: " + events);
        assertTrue(manager.cancelExecution(executionId, "analyst", false));

        assertEquals("FINISHED", history.getStatus());
        var result = manager.getResult(executionId, "analyst", false, 0, 10).orElseThrow();
        assertEquals("FINISHED", result.status());
        assertEquals(1, result.rows().size());
        assertEquals(1, count("FINISHED"));
        assertEquals(0, count("CANCELLED"));
        assertTrue(finishedEvents(events).get(0).contains("\"status\":\"FINISHED\""), () -> "events: " + events);
    }

    @Test
    void sessionSaveFailureDoesNotTurnFinishedIntoFailed() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        engineFinishesAfter(started, release);

        String executionId = manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        assertTrue(started.await(5, TimeUnit.SECONDS));
        doAnswer(i -> {
            throw new IllegalStateException("database is down");
        }).when(sessionRepo).save(any(SparkSessionRecord.class));
        release.countDown();
        await(() -> count("FINISHED") + count("FAILED") > 0, "no outcome recorded");

        assertEquals(1, count("FINISHED"));
        assertEquals(0, count("FAILED"));
        assertEquals("FINISHED", history.getStatus());
        assertEquals("FINISHED", manager.getResult(executionId, "analyst", false, 0, 10).orElseThrow().status());
    }

    @Test
    void failureAfterCancelElsewhereIsReportedAsCancelled() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(engine.execute(anyInt(), anyString(), anyString(), any(), anyString(), any(), any()))
                .thenAnswer(i -> {
                    started.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                    throw new IllegalStateException("statement killed");
                });

        String executionId = manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        assertTrue(started.await(5, TimeUnit.SECONDS));
        // Cancelled by another instance: the record says CANCELLED, the local cancel signal is not set
        history.setStatus("CANCELLED");
        release.countDown();
        await(() -> count("CANCELLED") + count("FAILED") > 0, "no outcome recorded");

        assertEquals(1, count("CANCELLED"));
        assertEquals(0, count("FAILED"));
        assertEquals("CANCELLED", manager.getResult(executionId, "analyst", false, 0, 10).orElseThrow().status());
    }

    @Test
    void failureIsReportedWhenHistoryCannotBeSaved() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch subscribed = new CountDownLatch(1);
        when(engine.execute(anyInt(), anyString(), anyString(), any(), anyString(), any(), any()))
                .thenAnswer(i -> {
                    started.countDown();
                    assertTrue(subscribed.await(5, TimeUnit.SECONDS));
                    throw new IllegalStateException("Livy недоступен");
                });

        String executionId = manager.submitCode("s1", "df.show()", "pyspark", "analyst");
        assertTrue(started.await(5, TimeUnit.SECONDS));
        // The database fails from now on: the terminal outcome must still reach subscribers and GET /result
        historyDown = true;
        doAnswer(i -> {
            throw new IllegalStateException("database is down");
        }).when(historyRepo).save(any(SparkExecutionHistory.class));
        List<String> received = new java.util.concurrent.CopyOnWriteArrayList<>();
        registerEmitter(executionId, recordingEmitter(received));
        subscribed.countDown();

        await(() -> !finishedEvents(received).isEmpty(), "no terminal event: " + received);

        assertEquals(1, count("FAILED"));
        assertTrue(received.stream().anyMatch(d -> d.contains("\"status\":\"FAILED\"") && d.contains("Livy недоступен")),
                () -> "events: " + received);
        var result = manager.getResult(executionId, "analyst", false, 0, 10).orElseThrow();
        assertEquals("FAILED", result.status());
        assertEquals("Livy недоступен", result.errorMessage());
    }

    private static SseEmitter recordingEmitter(List<String> received) {
        return new SseEmitter(5_000L) {
            @Override
            public void send(SseEventBuilder builder) {
                builder.build().forEach(part -> received.add(String.valueOf(part.getData())));
            }
        };
    }

    @SuppressWarnings("unchecked")
    private void registerEmitter(String executionId, SseEmitter emitter) throws Exception {
        var field = SparkSessionManager.class.getDeclaredField("executionEmitters");
        field.setAccessible(true);
        ((Map<String, List<SseEmitter>>) field.get(manager))
                .computeIfAbsent(executionId, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(emitter);
    }
}
