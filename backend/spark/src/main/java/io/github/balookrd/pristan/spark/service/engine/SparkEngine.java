package io.github.balookrd.pristan.spark.service.engine;

import io.github.balookrd.pristan.spark.dto.ColumnMetadata;
import io.github.balookrd.pristan.spark.dto.CreateSessionRequest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Spark execution backend of one cluster: Apache Livy or the in-process emulator. */
public interface SparkEngine {

    /** state uses the values of spark_sessions.status: starting, idle, busy, dead, killed. */
    record SessionHandle(int livySessionId, String appId, String state, String proxyUser, String name) {
        public SessionHandle(int livySessionId, String appId, String state) {
            this(livySessionId, appId, state, null, null);
        }
    }

    /** complete is false when the engine knows more sessions than it returned (one page of GET /sessions). */
    record SessionListing(List<SessionHandle> sessions, boolean complete) {}

    record ExecutionResult(
            List<ColumnMetadata> columns,
            List<List<Object>> rows,
            long totalRows,
            String logs,
            double executionTimeMs,
            String status,
            String errorMessage
    ) {}

    SessionHandle createSession(String username, CreateSessionRequest request);

    SessionHandle refreshSession(int livySessionId);

    void deleteSession(int livySessionId);

    /** All sessions of the engine, for the background state sync; only remote engines support it. */
    default SessionListing listSessions() {
        throw new UnsupportedOperationException("listSessions is supported by remote engines only");
    }

    /**
     * Runs code and streams status/log events; the terminal "finished" event is sent by the engine.
     * Infrastructure failures are thrown; a failed user program is returned with status FAILED.
     */
    ExecutionResult execute(int livySessionId, String executionId, String code, String language, String username,
                            Consumer<Map<String, Object>> eventConsumer, AtomicBoolean cancelSignal);

    /** false for the emulator: its session state lives only in the spark_sessions row. */
    default boolean remote() {
        return true;
    }
}
