package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
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
