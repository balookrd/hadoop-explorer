package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record HistoryItemResponse(
        String id,
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("cluster_id") String clusterId,
        String language,
        String code,
        String status,
        @JsonProperty("rows_count") long rowsCount,
        @JsonProperty("execution_time_ms") double executionTimeMs,
        @JsonProperty("error_message") String errorMessage,
        @JsonProperty("has_cached_result") boolean hasCachedResult,
        @JsonProperty("created_at") Instant createdAt,
        @JsonProperty("finished_at") Instant finishedAt
) {}
