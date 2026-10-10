package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
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
        List<HmsEventReportDto> events
) {}
