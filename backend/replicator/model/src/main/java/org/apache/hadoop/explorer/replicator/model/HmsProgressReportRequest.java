package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record HmsProgressReportRequest(
        String status,
        int totalTables,
        int replicatedTables,
        int totalPartitions,
        int replicatedPartitions,
        Long lastProcessedEventId,
        Long bootstrapEventId,
        Long eventLag,
        String message,
        List<HmsEventReportDto> events,
        String agentId,
        // Чекпоинт bootstrap (JSON): какие таблицы перенесены и докуда дошла текущая
        String bootstrapCheckpoint
) {
    public HmsProgressReportRequest(
            String status,
            int totalTables,
            int replicatedTables,
            int totalPartitions,
            int replicatedPartitions,
            Long lastProcessedEventId,
            Long bootstrapEventId,
            Long eventLag,
            String message,
            List<HmsEventReportDto> events,
            String agentId
    ) {
        this(status, totalTables, replicatedTables, totalPartitions, replicatedPartitions,
                lastProcessedEventId, bootstrapEventId, eventLag, message, events, agentId, null);
    }

    public HmsProgressReportRequest(
            String status,
            int totalTables,
            int replicatedTables,
            int totalPartitions,
            int replicatedPartitions,
            Long lastProcessedEventId,
            Long bootstrapEventId,
            Long eventLag,
            String message,
            List<HmsEventReportDto> events
    ) {
        this(status, totalTables, replicatedTables, totalPartitions, replicatedPartitions,
                lastProcessedEventId, bootstrapEventId, eventLag, message, events, null, null);
    }
}
