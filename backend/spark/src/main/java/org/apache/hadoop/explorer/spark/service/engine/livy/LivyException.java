package org.apache.hadoop.explorer.spark.service.engine.livy;

/** Livy call failed: HTTP error (status from Livy), unreachable server (503) or authentication (401). */
public class LivyException extends RuntimeException {

    private final int status;

    public LivyException(int status, String message) {
        super(message);
        this.status = status;
    }

    public LivyException(int status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
