package io.github.balookrd.pristan.iskra.service.engine.livy;

/** The session request contradicts the cluster configuration (unknown profile, denied spark_conf key). */
public class InvalidSessionRequestException extends RuntimeException {

    public InvalidSessionRequestException(String message) {
        super(message);
    }
}
