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
        Long lastProcessedEventId,
        String assignedAgentId,
        String executionPrincipal
) {
    public HmsPendingJobDto(
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
    ) {
        this(id, sourceClusterId, targetClusterId, sourceDbName, targetDbName,
                tableIncludePattern, dropExtraneousTables, dropExtraneousPartitions,
                targetAgentGrpcAddress, status, lastProcessedEventId, null, null);
    }

    public HmsPendingJobDto(
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
            Long lastProcessedEventId,
            String assignedAgentId
    ) {
        this(id, sourceClusterId, targetClusterId, sourceDbName, targetDbName,
                tableIncludePattern, dropExtraneousTables, dropExtraneousPartitions,
                targetAgentGrpcAddress, status, lastProcessedEventId, assignedAgentId, null);
    }
}
