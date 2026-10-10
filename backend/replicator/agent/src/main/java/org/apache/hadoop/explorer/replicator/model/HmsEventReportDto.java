package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record HmsEventReportDto(
        long eventId,
        String eventType,
        String tableName,
        String partitionName,
        String sourceUri,
        String targetUri,
        String subjobId,
        String status,
        String errorMessage
) {}
