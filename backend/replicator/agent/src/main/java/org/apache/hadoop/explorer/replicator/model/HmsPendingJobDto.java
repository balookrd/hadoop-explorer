package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record HmsPendingJobDto(
        String id,
        String sourceClusterId,
        String targetClusterId,
        String sourceDbName,
        String targetDbName,
        String tableIncludePattern,
        boolean dropExtraneousTables,
        boolean dropExtraneousPartitions,
        String targetAgentGrpcAddress,
        String status,
        Long lastProcessedEventId
) {}
