package io.github.balookrd.pristan.spark.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balookrd.pristan.common.auth.UserLoggedOutEvent;
import io.github.balookrd.pristan.spark.config.SparkProperties;
import io.github.balookrd.pristan.spark.dto.*;
import io.github.balookrd.pristan.spark.model.SparkExecutionHistory;
import io.github.balookrd.pristan.spark.model.SparkSessionRecord;
import io.github.balookrd.pristan.spark.repository.SparkExecutionHistoryRepository;
import io.github.balookrd.pristan.spark.repository.SparkSessionRepository;
import io.github.balookrd.pristan.spark.service.engine.SparkEngine;
import io.github.balookrd.pristan.spark.service.engine.SparkEngineRegistry;
import io.github.balookrd.pristan.spark.service.engine.livy.LivyException;
import io.github.balookrd.pristan.spark.service.engine.livy.LivySparkEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PreDestroy;
import org.springframework.context.event.EventListener;
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
    private static final Set<String> TERMINAL_SESSION_STATES = Set.of("dead", "killed");
    private static final Set<String> TERMINAL_EXECUTION_STATUSES = Set.of("FINISHED", "FAILED", "CANCELLED");
    private static final String CANCELLED_MESSAGE = "Выполнение отменено пользователем";

    private final SparkSessionRepository sessionRepository;
    private final SparkExecutionHistoryRepository historyRepository;
    private final SparkEngineRegistry engines;
    private final ClusterService clusterService;
    private final SparkProperties sparkProperties;
    private final ObjectMapper objectMapper;
    private final SparkMetrics sparkMetrics;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    // clusterId -> end of the sync pause after 503/504 from Livy
    private final Map<String, Instant> syncPausedUntil = new ConcurrentHashMap<>();

    // clusters whose sync task is still running
    private final Set<String> syncInFlight = ConcurrentHashMap.newKeySet();

    // executionId -> cancelSignal
    private final Map<String, AtomicBoolean> activeExecutions = new ConcurrentHashMap<>();

    // executionId -> List<SseEmitter>
    private final Map<String, List<SseEmitter>> executionEmitters = new ConcurrentHashMap<>();

    // executionId -> CachedExecutionResult
    private final Map<String, CachedExecutionResult> resultCache = new ConcurrentHashMap<>();

    /** username is the owner of the statement: GET /result is checked against it even when the database is down. */
    public record CachedExecutionResult(
            String username,
            String status,
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
            SparkEngineRegistry engines,
            ClusterService clusterService,
            SparkProperties sparkProperties,
            ObjectMapper objectMapper,
            SparkMetrics sparkMetrics
    ) {
        this.sessionRepository = sessionRepository;
        this.historyRepository = historyRepository;
        this.engines = engines;
        this.clusterService = clusterService;
        this.sparkProperties = sparkProperties;
        this.objectMapper = objectMapper;
        this.sparkMetrics = sparkMetrics;
    }

    public List<SessionResponse> getActiveSessions(String username) {
        return sessionRepository.findByUsernameAndStatusNotInOrderByCreatedAtDesc(username, List.of("dead", "killed"))
                .stream()
                .map(this::toResponse)
                .toList();
    }

    public SessionResponse createSession(SparkProperties.SparkClusterConfig cluster, String username, CreateSessionRequest req) {
        String sessionId = UUID.randomUUID().toString();
        String kind = req.kind() != null ? req.kind() : "pyspark";
        SparkEngine.SessionHandle handle = engines.forCluster(cluster).createSession(username, req);

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
        record.setKind(kind);
        record.setLivySessionId(handle.livySessionId());
        record.setYarnApplicationId(handle.appId());
        // A real Livy session starts as "starting"; the UI polls GET /api/v1/sessions until it is idle
        record.setStatus(handle.state());
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
        // The database only: syncSessions keeps the state current, a slow Livy never holds a request thread
        return sessionRepository.findByIdAndUsername(sessionId, username)
                .map(this::toResponse);
    }

    public boolean stopSession(String sessionId, String username, boolean isAdmin) {
        Optional<SparkSessionRecord> opt = sessionRepository.findById(sessionId);
        if (opt.isEmpty()) return false;
        SparkSessionRecord rec = opt.get();
        if (!isAdmin && !rec.getUsername().equals(username)) return false;

        // A dead or killed record is not deleted in Livy: after a Livy restart its id may belong to a new session
        if (sessionRepository.markKilledIfActive(sessionId, Instant.now()) == 1) {
            deleteRemoteSession(rec);
        }
        return true;
    }

    public String submitCode(String sessionId, String code, String language, String username) {
        SparkSessionRecord session = sessionRepository.findById(sessionId)
                .orElseThrow(() -> new IllegalArgumentException("Сессия не найдена: " + sessionId));

        if (!session.getUsername().equals(username)) {
            throw new SecurityException("Доступ к чужой сессии Spark запрещен");
        }
        if (TERMINAL_SESSION_STATES.contains(session.getStatus())) {
            throw new IllegalArgumentException("Сессия " + sessionId + " завершена (" + session.getStatus() + "), запустите новую");
        }
        SparkProperties.SparkClusterConfig cluster = clusterService.findCluster(session.getClusterId())
                .orElseThrow(() -> new IllegalArgumentException("Кластер сессии не найден: " + session.getClusterId()));

        String executionId = UUID.randomUUID().toString();
        AtomicBoolean cancelSignal = new AtomicBoolean(false);
        activeExecutions.put(executionId, cancelSignal);
        // Passed to the engine and used as the metric label: the statement language or, if absent, the session kind
        String resolvedLanguage = language != null ? language : session.getKind();

        SparkExecutionHistory history = new SparkExecutionHistory();
        history.setId(executionId);
        history.setSessionId(sessionId);
        history.setUsername(username);
        history.setClusterId(session.getClusterId());
        history.setLanguage(resolvedLanguage);
        history.setCode(code);
        history.setStatus("QUEUED");
        history.setCreatedAt(Instant.now());
        historyRepository.save(history);

        session.setLastActivityAt(Instant.now());
        session.setStatus("busy");
        sessionRepository.save(session);

        int livySessionId = session.getLivySessionId() != null ? session.getLivySessionId() : 0;
        executor.submit(() -> runExecutionTask(executionId, cluster, sessionId, livySessionId, code,
                resolvedLanguage, username, cancelSignal));
        return executionId;
    }

    private void runExecutionTask(
            String executionId,
            SparkProperties.SparkClusterConfig cluster,
            String sessionId,
            int livySessionId,
            String code,
            String language,
            String username,
            AtomicBoolean cancelSignal
    ) {
        long startNanos = System.nanoTime();
        String clusterId = cluster.getId();
        // Guards "exactly one recordStatement per terminal outcome" if something fails after the outcome is recorded
        boolean recorded = false;
        try {
            // Conditional: a statement cancelled while still queued stays CANCELLED
            historyRepository.markRunningIfQueued(executionId, Instant.now());

            SparkEngine.ExecutionResult result;
            if (cancelSignal.get()) {
                // Cancelled before the engine was called: nothing is submitted
                result = cancelledResult("", 0.0);
            } else {
                result = engines.forCluster(cluster).execute(
                        livySessionId,
                        executionId,
                        code,
                        language,
                        username,
                        // The engine's own "finished" event is held back: the outcome is decided by the terminal
                        // write below and announced once, so SSE never contradicts history, cache and metrics
                        event -> {
                            if (!"finished".equals(event.get("type"))) {
                                broadcastExecutionEvent(executionId, event);
                            }
                        },
                        cancelSignal
                );
            }

            SparkEngine.ExecutionResult outcome = completeOutcome(executionId, result, true);
            resultCache.put(executionId, toCached(username, outcome));
            returnSessionToIdle(sessionId);
            sparkMetrics.recordStatement(clusterId, language, outcome.status(), System.nanoTime() - startNanos);
            recorded = true;
            broadcastExecutionEvent(executionId, terminalEvent(outcome.status(), outcome.totalRows(),
                    outcome.executionTimeMs(), outcome.logs(), outcome.errorMessage()));
        } catch (Throwable e) {
            // Throwable, not Exception: an Error from the engine (NoClassDefFoundError, LinkageError,
            // StackOverflowError) would otherwise leave the history RUNNING and the session busy forever
            String message = failureMessage(e);
            log.error("Ошибка исполнения кода {}: {}", executionId, message, e);
            if (recorded) {
                // The terminal outcome is already stored and recorded; do not turn it into FAILED
                rethrowIfFatal(e);
                return;
            }
            // A failure caused by a cancel (an interrupted engine call, a cancel on another instance) is CANCELLED
            SparkEngine.ExecutionResult candidate = cancelSignal.get()
                    ? cancelledResult("", 0.0)
                    : new SparkEngine.ExecutionResult(List.of(), List.of(), 0, "", 0.0, "FAILED", message);
            SparkEngine.ExecutionResult outcome;
            try {
                outcome = completeOutcome(executionId, candidate, false);
            } catch (IOException | RuntimeException historyError) {
                // The outcome stays in memory and the subscribers still get the terminal event below
                log.error("Не удалось сохранить статус {} выполнения {}: {}", candidate.status(), executionId,
                        historyError.getMessage(), historyError);
                outcome = candidate;
            }
            sparkMetrics.recordStatement(clusterId, language, outcome.status(), System.nanoTime() - startNanos);
            recorded = true;
            // Kept in memory as well, so GET /result and late subscribers see the outcome even if the database failed
            resultCache.put(executionId, toCached(username, outcome));
            releaseSessionAfterFailure(sessionId, e);
            // The UI finishes the run on a "finished" event and reads the text from "error"
            broadcastExecutionEvent(executionId, "FAILED".equals(outcome.status())
                    ? Map.of(
                            "type", "finished",
                            "status", "FAILED",
                            "error", message,
                            "logs", "",
                            "execution_time_ms", 0.0
                    )
                    : terminalEvent(outcome.status(), outcome.totalRows(), outcome.executionTimeMs(),
                            outcome.logs(), outcome.errorMessage()));
            rethrowIfFatal(e);
        } finally {
            activeExecutions.remove(executionId);
        }
    }

    private static String failureMessage(Throwable e) {
        String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        // An Error is a defect of the service, not of the statement: keep its type visible to the user
        return e instanceof Error && e.getMessage() != null ? e.getClass().getSimpleName() + ": " + message : message;
    }

    /** The JVM itself is in trouble: after the outcome is recorded, let the executor see the error. */
    private static void rethrowIfFatal(Throwable e) {
        if (e instanceof VirtualMachineError fatal) {
            throw fatal;
        }
    }

    /**
     * Writes the candidate outcome only while the record is still QUEUED or RUNNING. If another terminal status
     * won (a cancel accepted meanwhile), that status is the outcome: CANCELLED without rows, otherwise what is stored.
     */
    private SparkEngine.ExecutionResult completeOutcome(String executionId, SparkEngine.ExecutionResult candidate,
                                                        boolean hasCachedResult) throws IOException {
        SparkEngine.ExecutionResult normalized = "CANCELLED".equals(candidate.status())
                ? cancelledResult(candidate.logs(), candidate.executionTimeMs())
                : candidate;
        String columnsJson = objectMapper.writeValueAsString(normalized.columns());
        int updated = historyRepository.completeIfActive(executionId, normalized.status(), normalized.totalRows(),
                normalized.executionTimeMs(), columnsJson, normalized.logs(), normalized.errorMessage(),
                hasCachedResult, Instant.now());
        if (updated == 1) {
            return normalized;
        }
        return historyRepository.findById(executionId)
                .map(h -> {
                    if ("CANCELLED".equals(h.getStatus())) {
                        return cancelledResult(normalized.logs(), normalized.executionTimeMs());
                    }
                    if (TERMINAL_EXECUTION_STATUSES.contains(h.getStatus())) {
                        return new SparkEngine.ExecutionResult(parseColumns(h.getColumnsJson()), List.of(),
                                h.getRowsCount(), h.getLogs(), h.getExecutionTimeMs(), h.getStatus(), h.getErrorMessage());
                    }
                    return normalized;
                })
                // No record (deleted meanwhile): the engine result is all there is
                .orElse(normalized);
    }

    private static CachedExecutionResult toCached(String username, SparkEngine.ExecutionResult outcome) {
        return new CachedExecutionResult(
                username,
                outcome.status(),
                outcome.columns(),
                outcome.rows(),
                outcome.totalRows(),
                outcome.logs(),
                outcome.errorMessage(),
                outcome.executionTimeMs(),
                Instant.now()
        );
    }

    /**
     * A failure here must not change the outcome of the statement, which is already decided. A session stopped
     * (killed) or found dead while the statement ran stays so: the update is conditional.
     */
    private void returnSessionToIdle(String sessionId) {
        try {
            sessionRepository.markIdleIfActive(sessionId, Instant.now());
        } catch (RuntimeException sessionError) {
            log.error("Не удалось вернуть сессию {} в idle: {}", sessionId, sessionError.getMessage(), sessionError);
        }
    }

    /**
     * After a failed statement: Livy 409 (LivySparkEngine#waitUntilReady saw dead, error, killed...) means the
     * session will never run code again, so the record becomes dead and the user is told to start a new session
     * instead of getting 409 on every statement. A 404 is checked against GET /sessions/{id}: Livy also answers
     * 404 for an unknown statement of a live session. Anything else returns the session to idle.
     */
    private void releaseSessionAfterFailure(String sessionId, Throwable failure) {
        if (!(failure instanceof LivyException livy) || (livy.getStatus() != 409 && livy.getStatus() != 404)) {
            returnSessionToIdle(sessionId);
            return;
        }
        try {
            Optional<SparkSessionRecord> record = sessionRepository.findById(sessionId);
            if (record.isEmpty()) {
                return;
            }
            if (livy.getStatus() == 409) {
                markDead(record.get(), livy.getMessage());
                return;
            }
            if (!TERMINAL_SESSION_STATES.contains(refreshFromEngine(record.get()).getStatus())) {
                returnSessionToIdle(sessionId);
            }
        } catch (RuntimeException sessionError) {
            log.error("Не удалось обновить статус сессии {} после ошибки Livy: {}", sessionId,
                    sessionError.getMessage(), sessionError);
        }
    }

    private void markDead(SparkSessionRecord record, String reason) {
        log.warn("Сессия Spark {} (Livy #{}) переведена в dead: {}", record.getId(), record.getLivySessionId(), reason);
        sessionRepository.updateStateIfActive(record.getId(), "dead", record.getYarnApplicationId(), Instant.now());
    }

    private static SparkEngine.ExecutionResult cancelledResult(String logs, double executionTimeMs) {
        return new SparkEngine.ExecutionResult(List.of(), List.of(), 0, logs != null ? logs : "", executionTimeMs,
                "CANCELLED", CANCELLED_MESSAGE);
    }

    public boolean cancelExecution(String executionId, String username, boolean isAdmin) {
        Optional<SparkExecutionHistory> opt = historyRepository.findById(executionId);
        if (opt.isEmpty()) return false;
        SparkExecutionHistory h = opt.get();
        if (!isAdmin && !h.getUsername().equals(username)) return false;

        // A terminal status is final: cancelling a finished, failed or cancelled statement changes nothing
        if (TERMINAL_EXECUTION_STATUSES.contains(h.getStatus())) {
            return true;
        }
        AtomicBoolean cancel = activeExecutions.get(executionId);
        if (cancel != null) {
            cancel.set(true);
        }
        // Conditional write: a terminal status saved by the execution task in the meantime is not overwritten.
        // Without a local cancel signal (left by a previous process or running on another instance) this only
        // clears the QUEUED/RUNNING record. With one, the task records the CANCELLED outcome and its metric.
        historyRepository.markCancelledIfActive(executionId, Instant.now());
        return true;
    }

    /**
     * Result of a statement owned by {@code username} (any statement for an admin). A statement of another user is
     * reported as absent, so the caller cannot tell a foreign id from an unknown one.
     */
    public Optional<StatementResultResponse> getResult(String executionId, String username, boolean isAdmin, int offset, int limit) {
        CachedExecutionResult data = resultCache.get(executionId);
        if (data != null) {
            if (!isAdmin && !Objects.equals(data.username(), username)) {
                return Optional.empty();
            }
            int total = data.rows().size();
            int fromIdx = Math.min(offset, total);
            int toIdx = Math.min(fromIdx + limit, total);
            return Optional.of(new StatementResultResponse(
                    executionId,
                    data.status(),
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

        return historyRepository.findById(executionId)
                .filter(h -> isAdmin || Objects.equals(h.getUsername(), username))
                .map(h -> {
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

    /**
     * Progress stream of a statement owned by {@code username} (any statement for an admin); empty for a statement
     * of another user or an unknown id. submitCode writes the history record before it returns the id.
     */
    public Optional<SseEmitter> registerStream(String executionId, String username, boolean isAdmin) {
        if (!isAdmin && !username.equals(ownerOf(executionId))) {
            return Optional.empty();
        }
        SseEmitter emitter = new SseEmitter(180_000L);
        executionEmitters.computeIfAbsent(executionId, k -> new CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> removeEmitter(executionId, emitter));
        emitter.onTimeout(() -> removeEmitter(executionId, emitter));
        emitter.onError(e -> removeEmitter(executionId, emitter));

        // The task may already be over when the UI subscribes; the in-memory outcome survives a database failure
        terminalEvent(executionId).ifPresent(event -> {
            try {
                emitter.send(SseEmitter.event().data(objectMapper.writeValueAsString(event)));
            } catch (IOException | IllegalStateException ignored) {
                // the subscriber is already gone
            }
        });

        return Optional.of(emitter);
    }

    /** Owner from the in-memory outcome first: it survives a failed terminal write to the database. */
    private String ownerOf(String executionId) {
        CachedExecutionResult cached = resultCache.get(executionId);
        if (cached != null) {
            return cached.username();
        }
        return historyRepository.findById(executionId).map(SparkExecutionHistory::getUsername).orElse(null);
    }

    private Optional<Map<String, Object>> terminalEvent(String executionId) {
        CachedExecutionResult cached = resultCache.get(executionId);
        if (cached != null) {
            return Optional.of(terminalEvent(cached.status(), cached.totalRows(), cached.executionTimeMs(),
                    cached.logs(), cached.errorMessage()));
        }
        return historyRepository.findById(executionId)
                .filter(h -> TERMINAL_EXECUTION_STATUSES.contains(h.getStatus()))
                .map(h -> terminalEvent(h.getStatus(), h.getRowsCount(), h.getExecutionTimeMs(),
                        h.getLogs(), h.getErrorMessage()));
    }

    private static Map<String, Object> terminalEvent(String status, long totalRows, double executionTimeMs,
                                                     String logs, String error) {
        return Map.of(
                "type", "finished",
                "status", status,
                "total_rows", totalRows,
                "execution_time_ms", executionTimeMs,
                "logs", logs != null ? logs : "",
                "error", error != null ? error : ""
        );
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
            } catch (IOException | RuntimeException e) {
                // IllegalStateException: the emitter has already completed. A delivery failure to one subscriber
                // must neither stop delivery to the others nor change the outcome of the statement
                log.debug("SSE delivery failed, dropping emitter: {}", e.getMessage());
                dropEmitter(list, emitter);
            }
        }
    }

    private void dropEmitter(List<SseEmitter> list, SseEmitter emitter) {
        list.remove(emitter);
        try {
            emitter.complete();
        } catch (RuntimeException ignored) {
            // already completed
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
        for (SparkSessionRecord s : sessionRepository.findByStatusAndLastActivityAtBefore("idle", threshold)) {
            try {
                // Conditional claim: a statement started since the query, or another instance, wins
                if (sessionRepository.markKilledIfIdleBefore(s.getId(), threshold, Instant.now()) == 1) {
                    log.info("Завершение неактивной сессии Spark: {}", s.getId());
                    // The scheduler thread is shared by all @Scheduled tasks: the Livy call (up to
                    // read-timeout per session) goes to the executor
                    executor.submit(() -> {
                        try {
                            deleteRemoteSession(s);
                        } catch (RuntimeException e) {
                            // submit() would keep the exception in a Future nobody reads
                            log.error("Сессия Livy неактивной сессии Spark {} не удалена: {}", s.getId(), e.getMessage(), e);
                        }
                    });
                }
            } catch (RuntimeException e) {
                // One broken record must not stop the cleanup of the others
                log.error("Не удалось завершить неактивную сессию Spark {}: {}", s.getId(), e.getMessage(), e);
            }
        }
    }

    @Scheduled(fixedDelayString = "${spark.session-sync-interval-ms:5000}",
            initialDelayString = "${spark.session-sync-interval-ms:5000}")
    public void scheduledSessionSync() {
        syncSessions();
    }

    /**
     * Pulls the state of non-terminal sessions from Livy: one GET /sessions per cluster, each cluster in its own
     * task of the executor, so a slow or unreachable Livy delays only its own cluster. A cluster still syncing
     * from the previous run is skipped; after 503/504 it is paused for spark.session-sync-pause-seconds.
     * Every instance runs this; the conditional updates make concurrent runs safe, the price is one extra
     * GET /sessions per cluster and instance per interval. The returned future completes when all started
     * cluster tasks are done.
     */
    public CompletableFuture<Void> syncSessions() {
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        for (SparkProperties.SparkClusterConfig cluster : sparkProperties.getClusters()) {
            SparkEngine engine;
            try {
                engine = engines.forCluster(cluster);
            } catch (RuntimeException e) {
                log.warn("Синхронизация сессий кластера {} пропущена: {}", cluster.getId(), e.getMessage());
                continue;
            }
            Instant pausedUntil = syncPausedUntil.get(cluster.getId());
            if (!engine.remote() || (pausedUntil != null && Instant.now().isBefore(pausedUntil))
                    || !syncInFlight.add(cluster.getId())) {
                continue;
            }
            try {
                tasks.add(CompletableFuture.runAsync(() -> {
                    try {
                        syncCluster(cluster, engine);
                    } finally {
                        syncInFlight.remove(cluster.getId());
                    }
                }, executor));
            } catch (RejectedExecutionException e) {
                syncInFlight.remove(cluster.getId()); // the context is shutting down
            }
        }
        return CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new));
    }

    private void syncCluster(SparkProperties.SparkClusterConfig cluster, SparkEngine engine) {
        String clusterId = cluster.getId();
        try {
            // Records first, Livy second: every record read here was saved after its Livy session was created,
            // so a session missing from the listing really is gone
            List<SparkSessionRecord> records = sessionRepository.findByClusterIdAndStatusNotIn(clusterId, TERMINAL_SESSION_STATES)
                    .stream().filter(r -> r.getLivySessionId() != null).toList();
            if (records.isEmpty()) {
                return;
            }
            SparkEngine.SessionListing listing;
            try {
                listing = engine.listSessions();
            } catch (LivyException e) {
                // Any Livy failure pauses the cluster: an unreachable Livy or a broken SPNEGO setup then costs one
                // request and one WARN per pause, not one per sync interval
                syncPausedUntil.put(clusterId, Instant.now().plusSeconds(sparkProperties.getSessionSyncPauseSeconds()));
                log.warn("Состояние сессий кластера {} не обновлено из Livy (HTTP {}), следующая попытка через {} с: {}",
                        clusterId, e.getStatus(), sparkProperties.getSessionSyncPauseSeconds(), e.getMessage());
                return;
            }
            syncPausedUntil.remove(clusterId);
            Map<Integer, SparkEngine.SessionHandle> byId = new HashMap<>();
            for (SparkEngine.SessionHandle handle : listing.sessions()) {
                byId.put(handle.livySessionId(), handle);
            }
            for (SparkSessionRecord record : records) {
                SparkEngine.SessionHandle handle = byId.get(record.getLivySessionId());
                if (handle == null) {
                    // An incomplete listing (more sessions than one page) proves nothing about the missing ones
                    if (listing.complete()) {
                        markDead(record, "сессии Livy #" + record.getLivySessionId() + " нет в GET /sessions");
                    }
                } else if (LivySparkEngine.isForeign(handle, record.getUsername(), cluster)) {
                    // Not deleted: it is somebody else's session that reuses the id
                    markDead(record, "id Livy #" + record.getLivySessionId() + " занят чужой сессией (proxyUser "
                            + handle.proxyUser() + ", name " + handle.name() + ")");
                } else {
                    applyState(record, handle.state(), handle.appId());
                }
            }
        } catch (RuntimeException e) {
            log.error("Ошибка синхронизации сессий кластера {}: {}", clusterId, e.getMessage(), e);
        }
    }

    /** Stops the executor with the context; running statements get a short grace period, the rest is logged. */
    @PreDestroy
    void shutdownExecutor() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                List<Runnable> pending = executor.shutdownNow();
                log.warn("Пул SparkSessionManager остановлен принудительно: прервано выполнений {}, не начато задач {}",
                        activeExecutions.size(), pending.size());
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * POST /api/v1/auth/logout closes every session of the user found in spark_sessions, in all browsers
     * (spark.close-sessions-on-logout). The event is published only on the instance that served the logout;
     * that instance deletes the sessions in Livy, whichever instance created them.
     */
    @EventListener
    public void onUserLoggedOut(UserLoggedOutEvent event) {
        if (!sparkProperties.isCloseSessionsOnLogout()) {
            return;
        }
        // The listener runs on the logout request thread: Livy calls go to the executor, so an unreachable
        // Livy does not delay the logout response
        executor.submit(() -> {
            List<SparkSessionRecord> sessions;
            try {
                sessions = sessionRepository.findByUsernameAndStatusNotInOrderByCreatedAtDesc(
                        event.username(), List.of("dead", "killed"));
            } catch (RuntimeException e) {
                log.error("Не удалось прочитать сессии Spark пользователя {} после выхода: {}",
                        event.username(), e.getMessage(), e);
                return;
            }
            for (SparkSessionRecord s : sessions) {
                log.info("Закрытие сессии Spark {} после выхода пользователя {}", s.getId(), event.username());
                try {
                    stopSession(s.getId(), event.username(), false);
                } catch (RuntimeException e) {
                    log.error("Не удалось закрыть сессию Spark {} после выхода: {}", s.getId(), e.getMessage(), e);
                }
            }
        });
    }

    /**
     * Pulls the state and YARN application id from Livy. A session Livy no longer knows (404, e.g. after a
     * Livy restart) becomes dead; any other Livy failure keeps the stored state.
     */
    private SparkSessionRecord refreshFromEngine(SparkSessionRecord record) {
        if (record.getLivySessionId() == null || TERMINAL_SESSION_STATES.contains(record.getStatus())) {
            return record;
        }
        Optional<SparkProperties.SparkClusterConfig> cluster = clusterService.findCluster(record.getClusterId());
        if (cluster.isEmpty()) {
            return record;
        }
        SparkEngine engine = engines.forCluster(cluster.get());
        if (!engine.remote()) {
            return record;
        }
        SparkEngine.SessionHandle handle;
        try {
            handle = engine.refreshSession(record.getLivySessionId());
        } catch (LivyException e) {
            if (e.getStatus() != 404) {
                log.warn("Состояние сессии Spark {} не обновлено из Livy: {}", record.getId(), e.getMessage());
                return record;
            }
            log.warn("Сессия Livy #{} не найдена, сессия Spark {} переведена в dead", record.getLivySessionId(), record.getId());
            return applyState(record, "dead", null);
        }
        if (LivySparkEngine.isForeign(handle, record.getUsername(), cluster.get())) {
            log.warn("id Livy #{} занят чужой сессией (proxyUser {}, name {}), сессия Spark {} переведена в dead",
                    record.getLivySessionId(), handle.proxyUser(), handle.name(), record.getId());
            return applyState(record, "dead", null);
        }
        return applyState(record, handle.state(), handle.appId());
    }

    /** Writes the state (and the app id, if Livy reported one) unless the record turned dead or killed meanwhile. */
    private SparkSessionRecord applyState(SparkSessionRecord record, String state, String reportedAppId) {
        String appId = reportedAppId != null ? reportedAppId : record.getYarnApplicationId();
        if (state.equals(record.getStatus()) && Objects.equals(appId, record.getYarnApplicationId())) {
            return record;
        }
        Instant stoppedAt = TERMINAL_SESSION_STATES.contains(state) ? Instant.now() : null;
        // Conditional: a stop accepted meanwhile (killed) is not overwritten by a state read just before it
        if (sessionRepository.updateStateIfActive(record.getId(), state, appId, stoppedAt) == 0) {
            return sessionRepository.findById(record.getId()).orElse(record);
        }
        record.setStatus(state);
        record.setYarnApplicationId(appId);
        record.setStoppedAt(stoppedAt);
        return record;
    }

    /** A Livy failure is logged, not thrown: the session would otherwise stay "alive" in the UI forever. */
    private void deleteRemoteSession(SparkSessionRecord record) {
        if (record.getLivySessionId() == null) {
            return;
        }
        clusterService.findCluster(record.getClusterId())
                .map(engines::forCluster)
                .filter(SparkEngine::remote)
                .ifPresent(engine -> {
                    try {
                        engine.deleteSession(record.getLivySessionId());
                    } catch (LivyException e) {
                        log.warn("Сессия Livy #{} не удалена: {}. Livy закроет ее по livy.server.session.timeout",
                                record.getLivySessionId(), e.getMessage());
                    }
                });
    }
}
