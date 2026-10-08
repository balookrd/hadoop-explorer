package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record QueryHistoryItem(
        String id,
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("cluster_name") String clusterName,
        @JsonProperty("engine_type") String engineType,
        @JsonProperty("query_text") String queryText,
        String status,
        @JsonProperty("rows_count") long rowsCount,
        @JsonProperty("execution_time_ms") double executionTimeMs,
        @JsonProperty("has_cached_result") boolean hasCachedResult,
        @JsonProperty("is_in_queue") boolean isInQueue,
        @JsonProperty("error_message") String errorMessage,
        @JsonProperty("created_at") Instant createdAt,
        @JsonProperty("finished_at") Instant finishedAt
) {}
