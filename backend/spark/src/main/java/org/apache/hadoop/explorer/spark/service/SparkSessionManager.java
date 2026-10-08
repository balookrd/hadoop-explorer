package org.apache.hadoop.explorer.spark.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.spark.config.SparkProperties;
import org.apache.hadoop.explorer.spark.dto.*;
import org.apache.hadoop.explorer.spark.model.SparkExecutionHistory;
import org.apache.hadoop.explorer.spark.model.SparkSessionRecord;
import org.apache.hadoop.explorer.spark.repository.SparkExecutionHistoryRepository;
import org.apache.hadoop.explorer.spark.repository.SparkSessionRepository;
import org.apache.hadoop.explorer.spark.service.engine.LivyClient;
import org.apache.hadoop.explorer.spark.service.engine.MockSparkEngine;
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
public class SparkSessionManager {

    private static final Logger log = LoggerFactory.getLogger(SparkSessionManager.class);

    private final SparkSessionRepository sessionRepository;
    private final SparkExecutionHistoryRepository historyRepository;
    private final LivyClient livyClient;
    private final SparkProperties sparkProperties;
    private final ObjectMapper objectMapper;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    // executionId -> cancelSignal
    private final Map<String, AtomicBoolean> activeExecutions = new ConcurrentHashMap<>();

    // executionId -> List<SseEmitter>
    private final Map<String, List<SseEmitter>> executionEmitters = new ConcurrentHashMap<>();

    // executionId -> CachedExecutionResult
    private final Map<String, CachedExecutionResult> resultCache = new ConcurrentHashMap<>();

    public record CachedExecutionResult(
            List<ColumnMetadata> columns,
            List<List<Object>> rows,
            long totalRows,
            String logs,
            String errorMessage,
            double executionTimeMs,
            Instant createdAt
    ) {}

    public SparkSessionManager(
            SparkSessionRepository sessionRepository,
            SparkExecutionHistoryRepository historyRepository,
            LivyClient livyClient,
            SparkProperties sparkProperties,
            ObjectMapper objectMapper
    ) {
        this.sessionRepository = sessionRepository;
        this.historyRepository = historyRepository;
        this.livyClient = livyClient;
        this.sparkProperties = sparkProperties;
        this.objectMapper = objectMapper;
    }

    public List<SessionResponse> getActiveSessions(String username) {
        return sessionRepository.findByUsernameAndStatusNotInOrderByCreatedAtDesc(username, List.of("dead", "killed"))
                .stream()
                .map(this::toResponse)
                .toList();
    }

    public SessionResponse createSession(SparkProperties.SparkClusterConfig cluster, String username, CreateSessionRequest req) {
        String sessionId = UUID.randomUUID().toString();
        var livyInfo = livyClient.createSession(cluster, req.kind() != null ? req.kind() : "pyspark");

        SparkSessionRecord record = new SparkSessionRecord();
        record.setId(sessionId);
        record.setUsername(username);
        record.setClusterId(cluster.getId());
        record.setSparkVersionId(req.sparkVersionId());
        record.setPythonEnvId(req.pythonEnvId());
        record.setCustomPythonArchive(req.customPythonArchive());
        record.setCustomPythonPath(req.customPythonPath());
        record.setMetastoreId(req.metastoreId());
        record.setYarnQueue(req.yarnQueue());
        record.setResourceProfile(req.resourceProfile());
        record.setKind(req.kind() != null ? req.kind() : "pyspark");
        record.setLivySessionId(livyInfo.livyId());
        record.setYarnApplicationId(livyInfo.appId());
        record.setStatus("idle");
        record.setCreatedAt(Instant.now());
        record.setLastActivityAt(Instant.now());

        try {
            if (req.packages() != null) record.setPackagesJson(objectMapper.writeValueAsString(req.packages()));
            if (req.jars() != null) record.setJarsJson(objectMapper.writeValueAsString(req.jars()));
            if (req.pyFiles() != null) record.setPyFilesJson(objectMapper.writeValueAsString(req.pyFiles()));
            if (req.sparkConf() != null) record.setSparkConfJson(objectMapper.writeValueAsString(req.sparkConf()));
        } catch (Exception ignored) {}

        SparkSessionRecord saved = sessionRepository.save(record);
        return toResponse(saved);
    }

    public Optional<SessionResponse> getSession(String sessionId, String username) {
        return sessionRepository.findByIdAndUsername(sessionId, username).map(this::toResponse);
    }

    public boolean stopSession(String sessionId, String username, boolean isAdmin) {
        Optional<SparkSessionRecord> opt = sessionRepository.findById(sessionId);
        if (opt.isEmpty()) return false;
        SparkSessionRecord rec = opt.get();
        if (!isAdmin && !rec.getUsername().equals(username)) return false;

        rec.setStatus("killed");
        rec.setStoppedAt(Instant.now());
        sessionRepository.save(rec);
        return true;
    }

    public String submitCode(String sessionId, String code, String language, String username) {
        SparkSessionRecord session = sessionRepository.findById(sessionId)
                .orElseThrow(() -> new IllegalArgumentException("Сессия не найдена: " + sessionId));

        if (!session.getUsername().equals(username)) {
            throw new SecurityException("Доступ к чужой сессии Spark запрещен");
        }

        String executionId = UUID.randomUUID().toString();
        AtomicBoolean cancelSignal = new AtomicBoolean(false);
        activeExecutions.put(executionId, cancelSignal);

        SparkExecutionHistory history = new SparkExecutionHistory();
        history.setId(executionId);
        history.setSessionId(sessionId);
        history.setUsername(username);
        history.setClusterId(session.getClusterId());
        history.setLanguage(language != null ? language : session.getKind());
        history.setCode(code);
        history.setStatus("QUEUED");
        history.setCreatedAt(Instant.now());
        historyRepository.save(history);

        session.setLastActivityAt(Instant.now());
        session.setStatus("busy");
        sessionRepository.save(session);

        executor.submit(() -> runExecutionTask(executionId, sessionId, code, language, username, cancelSignal));
        return executionId;
    }

    private void runExecutionTask(
            String executionId,
            String sessionId,
            String code,
            String language,
            String username,
            AtomicBoolean cancelSignal
    ) {
        try {
            historyRepository.findById(executionId).ifPresent(h -> {
                h.setStatus("RUNNING");
                h.setStartedAt(Instant.now());
                historyRepository.save(h);
            });

            MockSparkEngine.ExecutionResult result = livyClient.getMockEngine().executeCode(
                    executionId,
                    code,
                    language,
                    username,
                    event -> broadcastExecutionEvent(executionId, event),
                    cancelSignal
            );

            activeExecutions.remove(executionId);

            resultCache.put(executionId, new CachedExecutionResult(
                    result.columns(),
                    result.rows(),
                    result.totalRows(),
                    result.logs(),
                    result.errorMessage(),
                    result.executionTimeMs(),
                    Instant.now()
            ));

            String columnsJson = objectMapper.writeValueAsString(result.columns());
            historyRepository.findById(executionId).ifPresent(h -> {
                h.setStatus(result.status());
                h.setRowsCount(result.totalRows());
                h.setExecutionTimeMs(result.executionTimeMs());
                h.setColumnsJson(columnsJson);
                h.setLogs(result.logs());
                h.setErrorMessage(result.errorMessage());
                h.setHasCachedResult(true);
                h.setFinishedAt(Instant.now());
                historyRepository.save(h);
            });

            sessionRepository.findById(sessionId).ifPresent(s -> {
                s.setStatus("idle");
                s.setLastActivityAt(Instant.now());
                sessionRepository.save(s);
            });
        } catch (Exception e) {
            log.error("Ошибка исполнения кода {}: {}", executionId, e.getMessage(), e);
            activeExecutions.remove(executionId);
            historyRepository.findById(executionId).ifPresent(h -> {
                h.setStatus("FAILED");
                h.setErrorMessage(e.getMessage());
                h.setFinishedAt(Instant.now());
                historyRepository.save(h);
            });
            sessionRepository.findById(sessionId).ifPresent(s -> {
                s.setStatus("idle");
                sessionRepository.save(s);
            });
        }
    }

    public boolean cancelExecution(String executionId, String username, boolean isAdmin) {
        Optional<SparkExecutionHistory> opt = historyRepository.findById(executionId);
        if (opt.isEmpty()) return false;
        SparkExecutionHistory h = opt.get();
        if (!isAdmin && !h.getUsername().equals(username)) return false;

        AtomicBoolean cancel = activeExecutions.get(executionId);
        if (cancel != null) cancel.set(true);

        h.setStatus("CANCELLED");
        h.setFinishedAt(Instant.now());
        historyRepository.save(h);
        return true;
    }

    public Optional<StatementResultResponse> getResult(String executionId, int offset, int limit) {
        CachedExecutionResult data = resultCache.get(executionId);
        if (data != null) {
            int total = data.rows().size();
            int fromIdx = Math.min(offset, total);
            int toIdx = Math.min(fromIdx + limit, total);
            return Optional.of(new StatementResultResponse(
                    executionId,
                    "FINISHED",
                    data.columns(),
                    data.rows().subList(fromIdx, toIdx),
                    total,
                    offset,
                    limit,
                    data.logs(),
                    data.errorMessage(),
                    data.executionTimeMs()
            ));
        }

        return historyRepository.findById(executionId).map(h -> {
            List<ColumnMetadata> cols = parseColumns(h.getColumnsJson());
            return new StatementResultResponse(
                    executionId,
                    h.getStatus(),
                    cols,
                    List.of(),
                    (int) h.getRowsCount(),
                    offset,
                    limit,
                    h.getLogs(),
                    h.getErrorMessage(),
                    h.getExecutionTimeMs()
            );
        });
    }

    private List<ColumnMetadata> parseColumns(String json) {
        if (json == null || json.isBlank()) return List.of();
        try {
            return objectMapper.readValue(json, new TypeReference<List<ColumnMetadata>>() {});
        } catch (Exception e) {
            return List.of();
        }
    }

    public List<HistoryItemResponse> getUserHistory(String username, int limit) {
        return historyRepository.findByUsernameOrderByCreatedAtDesc(username, PageRequest.of(0, limit))
                .stream()
                .map(h -> new HistoryItemResponse(
                        h.getId(),
                        h.getSessionId(),
                        h.getClusterId(),
                        h.getLanguage(),
                        h.getCode(),
                        h.getStatus(),
                        h.getRowsCount(),
                        h.getExecutionTimeMs(),
                        h.getErrorMessage(),
                        h.isHasCachedResult(),
                        h.getCreatedAt(),
                        h.getFinishedAt()
                )).toList();
    }

    public SseEmitter registerStream(String executionId) {
        SseEmitter emitter = new SseEmitter(180_000L);
        executionEmitters.computeIfAbsent(executionId, k -> new CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> removeEmitter(executionId, emitter));
        emitter.onTimeout(() -> removeEmitter(executionId, emitter));
        emitter.onError(e -> removeEmitter(executionId, emitter));

        // Если задача уже завершилась к моменту подписки:
        historyRepository.findById(executionId).ifPresent(h -> {
            if ("FINISHED".equals(h.getStatus()) || "FAILED".equals(h.getStatus()) || "CANCELLED".equals(h.getStatus())) {
                try {
                    emitter.send(SseEmitter.event().data(objectMapper.writeValueAsString(Map.of(
                            "type", "finished",
                            "status", h.getStatus(),
                            "total_rows", h.getRowsCount(),
                            "execution_time_ms", h.getExecutionTimeMs(),
                            "logs", h.getLogs() != null ? h.getLogs() : ""
                    ))));
                } catch (IOException ignored) {}
            }
        });

        return emitter;
    }

    private void removeEmitter(String executionId, SseEmitter emitter) {
        List<SseEmitter> list = executionEmitters.get(executionId);
        if (list != null) {
            list.remove(emitter);
            if (list.isEmpty()) executionEmitters.remove(executionId);
        }
    }

    private void broadcastExecutionEvent(String executionId, Map<String, Object> event) {
        List<SseEmitter> list = executionEmitters.get(executionId);
        if (list == null || list.isEmpty()) return;

        for (SseEmitter emitter : list) {
            try {
                emitter.send(SseEmitter.event().data(objectMapper.writeValueAsString(event)));
            } catch (IOException e) {
                emitter.complete();
            }
        }
    }

    private SessionResponse toResponse(SparkSessionRecord r) {
        return new SessionResponse(
                r.getId(),
                r.getClusterId(),
                r.getSparkVersionId(),
                r.getPythonEnvId(),
                r.getCustomPythonArchive(),
                r.getCustomPythonPath(),
                r.getMetastoreId(),
                r.getYarnQueue(),
                r.getResourceProfile(),
                r.getKind(),
                r.getStatus(),
                r.getYarnApplicationId(),
                r.getCreatedAt() != null ? r.getCreatedAt().toString() : null,
                r.getLastActivityAt() != null ? r.getLastActivityAt().toString() : null
        );
    }

    @Scheduled(fixedRate = 60000)
    public void cleanupIdleSessions() {
        Instant threshold = Instant.now().minusSeconds(sparkProperties.getSessionTtlMinutes() * 60L);
        for (SparkSessionRecord s : sessionRepository.findAll()) {
            if ("idle".equalsIgnoreCase(s.getStatus()) && s.getLastActivityAt().isBefore(threshold)) {
                log.info("Завершение неактивной сессии Spark: {}", s.getId());
                s.setStatus("killed");
                s.setStoppedAt(Instant.now());
                sessionRepository.save(s);
            }
        }
    }
}
