package io.github.balookrd.pristan.tolmach.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balookrd.pristan.tolmach.config.SqlProperties;
import io.github.balookrd.pristan.tolmach.dto.CachedResultResponse;
import io.github.balookrd.pristan.tolmach.dto.ColumnMetadata;
import io.github.balookrd.pristan.tolmach.dto.QueryHistoryItem;
import io.github.balookrd.pristan.tolmach.model.QueryHistory;
import io.github.balookrd.pristan.tolmach.repository.QueryHistoryRepository;
import io.github.balookrd.pristan.tolmach.service.engine.SqlEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class QueryManagerService {

    private static final Logger log = LoggerFactory.getLogger(QueryManagerService.class);

    private static final Set<String> TERMINAL_STATUSES = Set.of("FINISHED", "FAILED", "CANCELLED");

    private final QueryHistoryRepository historyRepository;
    private final ClusterService clusterService;
    private final SqlProperties sqlProperties;
    private final ObjectMapper objectMapper;
    private final SqlQueryMetrics queryMetrics;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    // queryId -> CancelSignal
    private final Map<String, AtomicBoolean> activeQueries = new ConcurrentHashMap<>();

    // queryId -> query progress events with replay for late subscribers
    private final QueryEventHub eventHub;

    // username -> List<SseEmitter> (глобальные уведомления пользователя)
    private final Map<String, List<SseEmitter>> userEmitters = new ConcurrentHashMap<>();

    // queryId -> CachedResultData (строки и колонки в памяти для быстрого пейджинга)
    private final Map<String, CachedResultData> resultCache = new ConcurrentHashMap<>();

    public record CachedResultData(
            List<ColumnMetadata> columns,
            List<List<Object>> rows,
            long totalRows,
            Instant createdAt
    ) {}

    @Autowired
    public QueryManagerService(
            QueryHistoryRepository historyRepository,
            ClusterService clusterService,
            SqlProperties sqlProperties,
            ObjectMapper objectMapper,
            SqlQueryMetrics queryMetrics
    ) {
        this(historyRepository, clusterService, sqlProperties, objectMapper, queryMetrics, new QueryEventHub());
    }

    /** Test seam: lets tests observe the per-query event stream. */
    QueryManagerService(
            QueryHistoryRepository historyRepository,
            ClusterService clusterService,
            SqlProperties sqlProperties,
            ObjectMapper objectMapper,
            SqlQueryMetrics queryMetrics,
            QueryEventHub eventHub
    ) {
        this.historyRepository = historyRepository;
        this.clusterService = clusterService;
        this.sqlProperties = sqlProperties;
        this.objectMapper = objectMapper;
        this.queryMetrics = queryMetrics;
        this.eventHub = eventHub;
    }

    public String submitQuery(SqlProperties.ClusterConfig cluster, String username, String queryText) {
        String queryId = UUID.randomUUID().toString();
        AtomicBoolean cancelSignal = new AtomicBoolean(false);
        activeQueries.put(queryId, cancelSignal);
        eventHub.open(queryId);

        QueryHistory history = new QueryHistory();
        history.setId(queryId);
        history.setUsername(username);
        history.setClusterId(cluster.getId());
        history.setClusterName(cluster.getName());
        history.setEngineType(cluster.getType());
        history.setQueryText(queryText);
        history.setStatus("QUEUED");
        history.setInQueue(true);
        history.setCreatedAt(Instant.now());
        historyRepository.save(history);

        executor.submit(() -> runQueryTask(queryId, cluster, username, queryText, cancelSignal));
        return queryId;
    }

    private void runQueryTask(
            String queryId,
            SqlProperties.ClusterConfig cluster,
            String username,
            String queryText,
            AtomicBoolean cancelSignal
    ) {
        long startNanos = System.nanoTime();
        // Guards "exactly one recordQuery per terminal outcome" if something fails after a terminal state
        boolean recorded = false;
        try {
            updateStatus(queryId, "RUNNING", Instant.now(), null, 0.0, 0, 0.0);
            SqlEngine engine = clusterService.getEngine(cluster);

            SqlEngine.ExecutionResult result = engine.execute(
                    queryId,
                    queryText,
                    username,
                    sqlProperties.getMaxRowsPerQuery(),
                    event -> eventHub.publish(queryId, event),
                    cancelSignal
            );

            activeQueries.remove(queryId);

            if ("CANCELLED".equals(result.status())) {
                updateStatus(queryId, "CANCELLED", null, Instant.now(), result.executionTimeMs(), result.totalRows(), 0.0);
                queryMetrics.recordQuery(cluster, "CANCELLED", System.nanoTime() - startNanos);
                recorded = true;
                // The UI resets the running tab only on "finished"/"error", never on a status event
                eventHub.publish(queryId, Map.of(
                        "type", "finished",
                        "status", "CANCELLED",
                        "total_rows", result.totalRows(),
                        "execution_time_ms", result.executionTimeMs(),
                        "message", "Запрос отменен пользователем"
                ));
                eventHub.close(queryId);
                notifyUser(username, Map.of("type", "QUERY_CANCELLED", "query_id", queryId));
                return;
            }

            // Сохраняем в кэш
            resultCache.put(queryId, new CachedResultData(
                    result.columns(),
                    result.rows(),
                    result.totalRows(),
                    Instant.now()
            ));

            String columnsJson = objectMapper.writeValueAsString(result.columns());
            Optional<QueryHistory> opt = historyRepository.findById(queryId);
            if (opt.isPresent()) {
                QueryHistory qh = opt.get();
                qh.setStatus("FINISHED");
                qh.setRowsCount(result.totalRows());
                qh.setExecutionTimeMs(result.executionTimeMs());
                qh.setProgressPercent(100.0);
                qh.setFinishedAt(Instant.now());
                qh.setHasCachedResult(true);
                qh.setInQueue(false);
                qh.setColumnsJson(columnsJson);
                historyRepository.save(qh);
            }

            queryMetrics.recordQuery(cluster, "FINISHED", System.nanoTime() - startNanos);
            recorded = true;
            eventHub.close(queryId);
            notifyUser(username, Map.of(
                    "type", "QUERY_FINISHED",
                    "query_id", queryId,
                    "rows_count", result.totalRows(),
                    "duration_ms", result.executionTimeMs()
            ));
        } catch (Throwable e) {
            // Throwable, not Exception: an Error from the engine (NoClassDefFoundError of a JDBC driver,
            // LinkageError, StackOverflowError) would otherwise leave the history RUNNING without a terminal event
            String message = failureMessage(e);
            log.error("Ошибка при выполнении запроса {}: {}", queryId, message, e);
            if (recorded) {
                // The terminal outcome is already stored and recorded; do not turn it into FAILED
                eventHub.close(queryId);
                rethrowIfFatal(e);
                return;
            }
            queryMetrics.recordQuery(cluster, "FAILED", System.nanoTime() - startNanos);
            activeQueries.remove(queryId);
            try {
                Optional<QueryHistory> opt = historyRepository.findById(queryId);
                if (opt.isPresent()) {
                    QueryHistory qh = opt.get();
                    qh.setStatus("FAILED");
                    qh.setErrorMessage(message);
                    qh.setFinishedAt(Instant.now());
                    qh.setInQueue(false);
                    historyRepository.save(qh);
                }
            } catch (RuntimeException historyError) {
                // The subscribers must still get the terminal event below
                log.error("Не удалось сохранить статус FAILED запроса {}: {}", queryId, historyError.getMessage(), historyError);
            }
            // "error" is read by the UI (App.svelte), "message" is kept for older clients
            eventHub.publish(queryId, Map.of("type", "error", "status", "FAILED", "message", message, "error", message));
            eventHub.close(queryId);
            notifyUser(username, Map.of("type", "QUERY_FAILED", "query_id", queryId, "error", message));
            rethrowIfFatal(e);
        } finally {
            // Safety net: a channel that is never closed is never evicted (close is idempotent)
            activeQueries.remove(queryId);
            eventHub.close(queryId);
        }
    }

    private static String failureMessage(Throwable e) {
        String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        // An Error is a defect of the service, not of the query: keep its type visible to the user
        return e instanceof Error && e.getMessage() != null ? e.getClass().getSimpleName() + ": " + message : message;
    }

    /** The JVM itself is in trouble: after the outcome is recorded, let the executor see the error. */
    private static void rethrowIfFatal(Throwable e) {
        if (e instanceof VirtualMachineError fatal) {
            throw fatal;
        }
    }

    private void updateStatus(String queryId, String status, Instant startedAt, Instant finishedAt, double durationMs, long rows, double progress) {
        historyRepository.findById(queryId).ifPresent(qh -> {
            qh.setStatus(status);
            if (startedAt != null) qh.setStartedAt(startedAt);
            if (finishedAt != null) qh.setFinishedAt(finishedAt);
            if (durationMs > 0) qh.setExecutionTimeMs(durationMs);
            if (rows > 0) qh.setRowsCount(rows);
            if (progress > 0) qh.setProgressPercent(progress);
            historyRepository.save(qh);
        });
    }

    public boolean cancelQuery(String queryId, String username, boolean isAdmin) {
        Optional<QueryHistory> opt = historyRepository.findById(queryId);
        if (opt.isEmpty()) return false;
        QueryHistory qh = opt.get();
        if (!isAdmin && !qh.getUsername().equals(username)) {
            return false;
        }

        // A terminal status is final: cancelling a finished, failed or cancelled query changes nothing
        if (TERMINAL_STATUSES.contains(qh.getStatus())) {
            return true;
        }
        AtomicBoolean cancel = activeQueries.get(queryId);
        if (cancel != null) {
            cancel.set(true);
        }
        // Conditional write: a FINISHED/FAILED saved by the query thread in the meantime is not overwritten.
        // Without a local cancel signal (left by a previous process or running on another instance) this only
        // clears the QUEUED/RUNNING record; a query on another instance keeps running.
        historyRepository.markCancelledIfActive(queryId, Instant.now());
        return true;
    }

    /**
     * Result of a query owned by {@code username} (any query for an admin). A query of another user is reported
     * as absent, so the caller cannot tell a foreign id from an unknown one.
     */
    public Optional<CachedResultResponse> getCachedResult(String queryId, String username, boolean isAdmin, int offset, int limit) {
        Optional<QueryHistory> opt = findVisible(queryId, username, isAdmin);
        if (opt.isEmpty()) {
            return Optional.empty();
        }
        CachedResultData data = resultCache.get(queryId);
        if (data == null) {
            // Only the column metadata survives in the DB
            if (!opt.get().isHasCachedResult()) {
                return Optional.empty();
            }
            List<ColumnMetadata> cols = parseColumns(opt.get().getColumnsJson());
            return Optional.of(new CachedResultResponse(queryId, cols, List.of(), (int) opt.get().getRowsCount(), offset, limit));
        }

        int total = data.rows().size();
        int fromIndex = Math.min(offset, total);
        int toIndex = Math.min(fromIndex + limit, total);
        List<List<Object>> subRows = data.rows().subList(fromIndex, toIndex);

        return Optional.of(new CachedResultResponse(queryId, data.columns(), subRows, total, offset, limit));
    }

    /** The history record when the query belongs to {@code username} or the caller is an admin. */
    private Optional<QueryHistory> findVisible(String queryId, String username, boolean isAdmin) {
        return historyRepository.findById(queryId)
                .filter(h -> isAdmin || (h.getUsername() != null && h.getUsername().equals(username)));
    }

    private List<ColumnMetadata> parseColumns(String json) {
        if (json == null || json.isBlank()) return List.of();
        try {
            return objectMapper.readValue(json, new TypeReference<List<ColumnMetadata>>() {});
        } catch (Exception e) {
            return List.of();
        }
    }

    public List<QueryHistoryItem> getUserHistory(String username, int offset, int limit) {
        int page = offset / Math.max(limit, 1);
        List<QueryHistory> entities = historyRepository.findByUsernameOrderByCreatedAtDesc(username, PageRequest.of(page, limit));
        return entities.stream().map(this::toItem).toList();
    }

    public List<QueryHistoryItem> getUserQueue(String username) {
        List<QueryHistory> entities = historyRepository.findByUsernameAndInQueueTrueOrderByCreatedAtDesc(username, PageRequest.of(0, 100));
        return entities.stream().map(this::toItem).toList();
    }

    private QueryHistoryItem toItem(QueryHistory e) {
        return new QueryHistoryItem(
                e.getId(),
                e.getClusterId(),
                e.getClusterName(),
                e.getEngineType(),
                e.getQueryText(),
                e.getStatus(),
                e.getRowsCount(),
                e.getExecutionTimeMs(),
                e.isHasCachedResult(),
                e.isInQueue(),
                e.getErrorMessage(),
                e.getCreatedAt(),
                e.getFinishedAt()
        );
    }

    /**
     * Progress stream of a query owned by {@code username} (any query for an admin); empty for a query of another
     * user or an unknown id. The history record is written before submitQuery returns the id, so the owner's
     * subscription never races with it.
     */
    public Optional<SseEmitter> registerQueryStream(String queryId, String username, boolean isAdmin) {
        Optional<QueryHistory> visible = findVisible(queryId, username, isAdmin);
        if (visible.isEmpty()) {
            return Optional.empty();
        }
        SseEmitter emitter = new SseEmitter(180_000L);
        QueryEventHub.Sink sink = event -> trySend(emitter, event);

        emitter.onCompletion(() -> eventHub.unsubscribe(queryId, sink));
        emitter.onTimeout(() -> eventHub.unsubscribe(queryId, sink));
        emitter.onError(e -> eventHub.unsubscribe(queryId, sink));

        if (!eventHub.subscribe(queryId, sink)) {
            // Evicted by TTL or started before a restart: only the terminal state is known
            historyRepository.findById(queryId)
                    .flatMap(this::terminalEventFromHistory)
                    .ifPresent(sink::send);
        }
        return Optional.of(emitter);
    }

    private Optional<Map<String, Object>> terminalEventFromHistory(QueryHistory h) {
        if (h.getStatus() == null) {
            return Optional.empty();
        }
        return switch (h.getStatus()) {
            case "FINISHED" -> Optional.of(Map.of(
                    "type", "finished",
                    "status", "FINISHED",
                    "total_rows", h.getRowsCount(),
                    "execution_time_ms", h.getExecutionTimeMs(),
                    "message", "Запрос уже завершен, результат доступен в истории"));
            case "CANCELLED" -> Optional.of(Map.of("type", "finished", "status", "CANCELLED", "message", "Запрос отменен"));
            case "FAILED" -> {
                String message = h.getErrorMessage() != null ? h.getErrorMessage() : "Запрос завершился ошибкой";
                yield Optional.of(Map.of("type", "error", "status", "FAILED", "message", message, "error", message));
            }
            default -> Optional.empty();
        };
    }

    /** Returns false (the hub drops the subscriber) when the emitter is gone; never throws into the query thread. */
    private boolean trySend(SseEmitter emitter, Map<String, Object> event) {
        try {
            emitter.send(SseEmitter.event().data(objectMapper.writeValueAsString(event)));
            return true;
        } catch (IOException | IllegalStateException e) {
            // IllegalStateException: the emitter has already completed; a delivery failure must not fail the query
            log.debug("SSE delivery failed, dropping emitter: {}", e.getMessage());
            completeQuietly(emitter);
            return false;
        }
    }

    public SseEmitter registerUserStream(String username) {
        SseEmitter emitter = new SseEmitter(300_000L);
        userEmitters.computeIfAbsent(username, k -> new CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> removeUserEmitter(username, emitter));
        emitter.onTimeout(() -> removeUserEmitter(username, emitter));
        emitter.onError(e -> removeUserEmitter(username, emitter));

        try {
            emitter.send(SseEmitter.event().data(Map.of("type", "CONNECTED")));
        } catch (IOException ignored) {}

        return emitter;
    }

    private void removeUserEmitter(String username, SseEmitter emitter) {
        List<SseEmitter> list = userEmitters.get(username);
        if (list != null) {
            list.remove(emitter);
            if (list.isEmpty()) userEmitters.remove(username);
        }
    }

    private void dropEmitter(List<SseEmitter> list, SseEmitter emitter) {
        list.remove(emitter);
        completeQuietly(emitter);
    }

    private static void completeQuietly(SseEmitter emitter) {
        try {
            emitter.complete();
        } catch (RuntimeException ignored) {
            // already completed
        }
    }

    private void notifyUser(String username, Map<String, Object> event) {
        List<SseEmitter> list = userEmitters.get(username);
        if (list == null || list.isEmpty()) return;

        for (SseEmitter emitter : list) {
            try {
                emitter.send(SseEmitter.event().data(objectMapper.writeValueAsString(event)));
            } catch (IOException | IllegalStateException e) {
                // IllegalStateException: the emitter has already completed; a delivery failure must not fail the query
                log.debug("SSE delivery failed, dropping emitter: {}", e.getMessage());
                dropEmitter(list, emitter);
            }
        }
    }

    @Scheduled(fixedRate = 3600000)
    public void cleanupExpiredResults() {
        Instant threshold = Instant.now().minusSeconds(sqlProperties.getResultsTtlHours() * 3600L);
        resultCache.entrySet().removeIf(entry -> entry.getValue().createdAt().isBefore(threshold));
        eventHub.evictClosedBefore(threshold);
    }
}
