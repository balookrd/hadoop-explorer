package io.github.balookrd.pristan.spark.service.engine.livy;

import io.github.balookrd.pristan.spark.config.SparkProperties;
import io.github.balookrd.pristan.spark.dto.CreateSessionRequest;
import io.github.balookrd.pristan.spark.service.engine.SparkEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Spark through Apache Livy interactive sessions; used for type: livy with mock-storage: false. */
public class LivySparkEngine implements SparkEngine {

    private static final Logger log = LoggerFactory.getLogger(LivySparkEngine.class);

    private static final Set<String> READY_STATES = Set.of("idle", "busy");      // busy: Livy queues the statement
    private static final Set<String> STARTING_STATES = Set.of("not_started", "starting");

    private final SparkProperties.SparkClusterConfig cluster;
    private final LivyClient client;
    private final LivySessionPayloadBuilder payloads;

    public LivySparkEngine(SparkProperties.SparkClusterConfig cluster, LivyClient client, LivySessionPayloadBuilder payloads) {
        this.cluster = cluster;
        this.client = client;
        this.payloads = payloads;
    }

    /** Livy session state -> spark_sessions.status understood by the UI. */
    public static String sessionState(String livyState) {
        return switch (livyState) {
            case "not_started", "starting" -> "starting";
            case "idle" -> "idle";
            case "busy" -> "busy";
            case "killed" -> "killed";
            default -> "dead"; // shutting_down, error, dead, success, unknown
        };
    }

    /** UI language -> Livy statement kind. */
    public static String statementKind(String language) {
        return switch (language == null ? "pyspark" : language) {
            case "scalaspark", "spark", "scala" -> "spark";
            case "sql" -> "sql";
            default -> "pyspark";
        };
    }

    /**
     * true when the Livy session is not the one created for this user. Livy without session recovery reuses ids
     * from 0 after a restart, so a stored id may point to somebody else's session. LivySessionPayloadBuilder always
     * sets the name hexp-<user>-..., so a missing or different name is foreign. With livy.proxy-user: true the
     * proxyUser must be the user as well: null means Livy ignored it (livy.impersonation.enabled = false).
     */
    public static boolean isForeign(String proxyUser, String name, String username, boolean proxyUserRequired) {
        if (name == null || !name.startsWith(LivySessionPayloadBuilder.sessionNamePrefix(username))) {
            return true;
        }
        if (proxyUserRequired) {
            return !username.equals(proxyUser);
        }
        return proxyUser != null && !proxyUser.equals(username);
    }

    public static boolean isForeign(SessionHandle handle, String username, SparkProperties.SparkClusterConfig cluster) {
        return isForeign(handle.proxyUser(), handle.name(), username, cluster.getLivy().isProxyUser());
    }

    private static SessionHandle handle(LivyClient.LivySession session) {
        return new SessionHandle(session.id(), session.appId(), sessionState(session.state()), session.proxyUser(), session.name());
    }

    @Override
    public SessionHandle createSession(String username, CreateSessionRequest request) {
        return handle(client.createSession(payloads.build(cluster, username, request)));
    }

    @Override
    public SessionHandle refreshSession(int livySessionId) {
        return handle(client.getSession(livySessionId));
    }

    @Override
    public SessionListing listSessions() {
        LivyClient.LivySessionList list = client.listSessions();
        return new SessionListing(list.sessions().stream().map(LivySparkEngine::handle).toList(),
                list.sessions().size() >= list.total());
    }

    @Override
    public void deleteSession(int livySessionId) {
        client.deleteSession(livySessionId);
    }

    @Override
    public ExecutionResult execute(int livySessionId, String executionId, String code, String language, String username,
                                   Consumer<Map<String, Object>> events, AtomicBoolean cancel) {
        long startNanos = System.nanoTime();
        SparkProperties.LivyConfig livy = cluster.getLivy();
        if (!waitUntilReady(livySessionId, username, events, cancel, startNanos + Duration.ofSeconds(livy.getSessionStartTimeoutSeconds()).toNanos())) {
            return cancelled(startNanos);
        }
        LivyClient.LivyStatement statement = client.submitStatement(livySessionId, code, statementKind(language));
        long deadline = System.nanoTime() + Duration.ofSeconds(livy.getStatementTimeoutSeconds()).toNanos();
        while (true) {
            if (cancel.get()) {
                client.cancelStatement(livySessionId, statement.id());
                return cancelled(startNanos);
            }
            if (System.nanoTime() - deadline >= 0) {
                cancelQuietly(livySessionId, statement.id());
                throw new LivyException(504, "Превышен лимит времени выполнения statement в Livy: "
                        + livy.getStatementTimeoutSeconds() + " с (livy.statement-timeout-seconds)");
            }
            statement = client.getStatement(livySessionId, statement.id());
            switch (statement.state()) {
                case "available" -> {
                    return completed(statement, startNanos);
                }
                case "cancelling", "cancelled" -> {
                    return cancelled(startNanos);
                }
                case "error" -> throw new LivyException(502, "Livy statement #" + statement.id() + " завершился ошибкой");
                default -> events.accept(Map.of(
                        "type", "status",
                        "status", "RUNNING",
                        "progress", statement.progress(),
                        "message", "Livy statement #" + statement.id() + ": " + statement.state()));
            }
            if (!pause(livy.getPollIntervalMs())) {
                // pause() leaves the interrupt flag cleared: with it set the JDK HttpClient would refuse to send
                // the cancel. The flag is restored for the caller afterwards.
                cancel.set(true);
                cancelQuietly(livySessionId, statement.id());
                Thread.currentThread().interrupt();
                return cancelled(startNanos);
            }
        }
    }

    private boolean waitUntilReady(int livySessionId, String username, Consumer<Map<String, Object>> events,
                                   AtomicBoolean cancel, long deadline) {
        while (true) {
            if (cancel.get()) {
                return false;
            }
            LivyClient.LivySession session = client.getSession(livySessionId);
            if (isForeign(session.proxyUser(), session.name(), username, cluster.getLivy().isProxyUser())) {
                // 409: SparkSessionManager marks the record dead; the foreign session is not touched
                throw new LivyException(409, "Сессия Livy #" + livySessionId + " не принадлежит пользователю " + username
                        + " (proxyUser " + session.proxyUser() + ", name " + session.name()
                        + "): Livy перезапущен без восстановления сессий или работает с livy.impersonation.enabled = false;"
                        + " запустите новую сессию");
            }
            String state = session.state();
            if (READY_STATES.contains(state)) {
                return true;
            }
            if (!STARTING_STATES.contains(state)) {
                throw new LivyException(409, "Сессия Livy #" + livySessionId + " в состоянии " + state + ", запустите новую сессию");
            }
            if (System.nanoTime() - deadline >= 0) {
                throw new LivyException(504, "Сессия Livy #" + livySessionId + " не запустилась за "
                        + cluster.getLivy().getSessionStartTimeoutSeconds() + " с (livy.session-start-timeout-seconds)");
            }
            events.accept(Map.of("type", "status", "status", "RUNNING", "progress", 0.0,
                    "message", "Ожидание запуска сессии Livy #" + livySessionId + " (" + state + ")..."));
            if (!pause(cluster.getLivy().getPollIntervalMs())) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    // No "finished" event here: SparkSessionManager announces the outcome once, after recording it
    private static ExecutionResult completed(LivyClient.LivyStatement statement, long startNanos) {
        LivyOutputParser.Parsed parsed = LivyOutputParser.parse(statement.output());
        double durationMs = (System.nanoTime() - startNanos) / 1_000_000.0;
        if (parsed.error() != null) {
            return new ExecutionResult(List.of(), List.of(), 0, parsed.text(), durationMs, "FAILED", parsed.error());
        }
        return new ExecutionResult(parsed.columns(), parsed.rows(), parsed.rows().size(), parsed.text(), durationMs, "FINISHED", null);
    }

    private static ExecutionResult cancelled(long startNanos) {
        double durationMs = (System.nanoTime() - startNanos) / 1_000_000.0;
        return new ExecutionResult(List.of(), List.of(), 0, "", durationMs, "CANCELLED", "Отменено");
    }

    /** Best effort: a failed cancel must not hide why the statement is abandoned. */
    private void cancelQuietly(int livySessionId, int statementId) {
        try {
            client.cancelStatement(livySessionId, statementId);
        } catch (LivyException e) {
            log.warn("Не удалось отменить statement #{} в сессии Livy #{}: {}", statementId, livySessionId, e.getMessage());
        }
    }

    /** false when interrupted; the interrupt flag stays cleared, the caller restores it after cleaning up. */
    private static boolean pause(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
}
