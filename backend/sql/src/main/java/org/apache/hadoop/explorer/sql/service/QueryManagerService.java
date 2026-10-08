package org.apache.hadoop.explorer.sql.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.dto.CachedResultResponse;
import org.apache.hadoop.explorer.sql.dto.ColumnMetadata;
import org.apache.hadoop.explorer.sql.dto.QueryHistoryItem;
import org.apache.hadoop.explorer.sql.model.QueryHistory;
import org.apache.hadoop.explorer.sql.repository.QueryHistoryRepository;
import org.apache.hadoop.explorer.sql.service.engine.SqlEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private final QueryHistoryRepository historyRepository;
    private final ClusterService clusterService;
    private final SqlProperties sqlProperties;
    private final ObjectMapper objectMapper;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    // queryId -> CancelSignal
    private final Map<String, AtomicBoolean> activeQueries = new ConcurrentHashMap<>();

    // queryId -> List<SseEmitter> (стриминг процесса запроса)
    private final Map<String, List<SseEmitter>> queryEmitters = new ConcurrentHashMap<>();

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

    public QueryManagerService(
            QueryHistoryRepository historyRepository,
            ClusterService clusterService,
            SqlProperties sqlProperties,
            ObjectMapper objectMapper
    ) {
        this.historyRepository = historyRepository;
        this.clusterService = clusterService;
        this.sqlProperties = sqlProperties;
        this.objectMapper = objectMapper;
    }

    public String submitQuery(SqlProperties.ClusterConfig cluster, String username, String queryText) {
        String queryId = UUID.randomUUID().toString();
        AtomicBoolean cancelSignal = new AtomicBoolean(false);
        activeQueries.put(queryId, cancelSignal);

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
        try {
            updateStatus(queryId, "RUNNING", Instant.now(), null, 0.0, 0, 0.0);
            SqlEngine engine = clusterService.getEngine(cluster);

            SqlEngine.ExecutionResult result = engine.execute(
                    queryId,
                    queryText,
                    username,
                    sqlProperties.getMaxRowsPerQuery(),
                    event -> broadcastQueryEvent(queryId, event),
                    cancelSignal
            );

            activeQueries.remove(queryId);

            if ("CANCELLED".equals(result.status())) {
                updateStatus(queryId, "CANCELLED", null, Instant.now(), result.executionTimeMs(), result.totalRows(), 0.0);
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

            notifyUser(username, Map.of(
                    "type", "QUERY_FINISHED",
                    "query_id", queryId,
                    "rows_count", result.totalRows(),
                    "duration_ms", result.executionTimeMs()
            ));
        } catch (Exception e) {
            log.error("Ошибка при выполнении запроса {}: {}", queryId, e.getMessage(), e);
            activeQueries.remove(queryId);
            Optional<QueryHistory> opt = historyRepository.findById(queryId);
            if (opt.isPresent()) {
                QueryHistory qh = opt.get();
                qh.setStatus("FAILED");
                qh.setErrorMessage(e.getMessage());
                qh.setFinishedAt(Instant.now());
                qh.setInQueue(false);
                historyRepository.save(qh);
            }
            broadcastQueryEvent(queryId, Map.of("type", "error", "message", e.getMessage()));
            notifyUser(username, Map.of("type", "QUERY_FAILED", "query_id", queryId, "error", e.getMessage()));
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

        AtomicBoolean cancel = activeQueries.get(queryId);
        if (cancel != null) {
            cancel.set(true);
        }
        qh.setStatus("CANCELLED");
        qh.setInQueue(false);
        qh.setFinishedAt(Instant.now());
        historyRepository.save(qh);
        return true;
    }

    public Optional<CachedResultResponse> getCachedResult(String queryId, int offset, int limit) {
        CachedResultData data = resultCache.get(queryId);
        if (data == null) {
            // Попробуем прочитать метаданные колонок из БД
            Optional<QueryHistory> opt = historyRepository.findById(queryId);
            if (opt.isEmpty() || !opt.get().isHasCachedResult()) {
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

    public SseEmitter registerQueryStream(String queryId) {
        SseEmitter emitter = new SseEmitter(180_000L);
        queryEmitters.computeIfAbsent(queryId, k -> new CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> removeQueryEmitter(queryId, emitter));
        emitter.onTimeout(() -> removeQueryEmitter(queryId, emitter));
        emitter.onError(e -> removeQueryEmitter(queryId, emitter));

        return emitter;
    }

    private void removeQueryEmitter(String queryId, SseEmitter emitter) {
        List<SseEmitter> list = queryEmitters.get(queryId);
        if (list != null) {
            list.remove(emitter);
            if (list.isEmpty()) queryEmitters.remove(queryId);
        }
    }

    private void broadcastQueryEvent(String queryId, Map<String, Object> event) {
        List<SseEmitter> list = queryEmitters.get(queryId);
        if (list == null || list.isEmpty()) return;

        for (SseEmitter emitter : list) {
            try {
                emitter.send(SseEmitter.event().data(objectMapper.writeValueAsString(event)));
            } catch (IOException e) {
                emitter.complete();
            }
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

    private void notifyUser(String username, Map<String, Object> event) {
        List<SseEmitter> list = userEmitters.get(username);
        if (list == null || list.isEmpty()) return;

        for (SseEmitter emitter : list) {
            try {
                emitter.send(SseEmitter.event().data(objectMapper.writeValueAsString(event)));
            } catch (IOException e) {
                emitter.complete();
            }
        }
    }

    @Scheduled(fixedRate = 3600000)
    public void cleanupExpiredResults() {
        Instant threshold = Instant.now().minusSeconds(sqlProperties.getResultsTtlHours() * 3600L);
        resultCache.entrySet().removeIf(entry -> entry.getValue().createdAt().isBefore(threshold));
    }
}
