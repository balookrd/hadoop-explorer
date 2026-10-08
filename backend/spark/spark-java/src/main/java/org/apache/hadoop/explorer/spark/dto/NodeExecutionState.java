package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record NodeExecutionState(
        String status, // pending, running, success, failed
        String logs,
        String error,
        @JsonProperty("duration_seconds") double durationSeconds
) {}
