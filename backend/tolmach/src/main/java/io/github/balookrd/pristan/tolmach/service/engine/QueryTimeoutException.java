package io.github.balookrd.pristan.tolmach.service.engine;

/** The query ran longer than tolmach.clusters[].jdbc.query-timeout-seconds and was cancelled. */
public class QueryTimeoutException extends SqlEngineException {

    public QueryTimeoutException(String message) {
        super(message);
    }
}
