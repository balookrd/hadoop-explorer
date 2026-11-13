package io.github.balookrd.pristan.sql.service.engine;

/** Failure reported by a real query engine (connection, authentication, SQL error). */
public class SqlEngineException extends RuntimeException {

    public SqlEngineException(String message) {
        super(message);
    }

    public SqlEngineException(String message, Throwable cause) {
        super(message, cause);
    }
}
