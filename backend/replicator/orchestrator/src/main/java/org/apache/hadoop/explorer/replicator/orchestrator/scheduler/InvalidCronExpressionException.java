package org.apache.hadoop.explorer.replicator.orchestrator.scheduler;

/** A job schedule that cannot be parsed or never fires. */
public class InvalidCronExpressionException extends IllegalArgumentException {

    public InvalidCronExpressionException(String expression, String reason) {
        super("Некорректное cron-выражение '" + expression + "': " + reason);
    }
}
