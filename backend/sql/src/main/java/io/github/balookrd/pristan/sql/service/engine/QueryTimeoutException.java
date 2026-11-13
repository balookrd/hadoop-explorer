package io.github.balookrd.pristan.sql.service.engine;

/** The query ran longer than sql.clusters[].jdbc.query-timeout-seconds and was cancelled. */
public class QueryTimeoutException extends SqlEngineException {

    public QueryTimeoutException(String message) {
        super(message);
    }
}
