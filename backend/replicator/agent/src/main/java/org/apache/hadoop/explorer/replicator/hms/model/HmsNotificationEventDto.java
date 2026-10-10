package org.apache.hadoop.explorer.replicator.hms.model;

public record HmsNotificationEventDto(
        long eventId,
        int eventTime,
        String eventType,
        String dbName,
        String tableName,
        String message,
        String messageFormat
) {
}
