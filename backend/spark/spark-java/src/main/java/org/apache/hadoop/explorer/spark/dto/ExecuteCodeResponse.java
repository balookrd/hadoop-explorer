package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ExecuteCodeResponse(
        @JsonProperty("execution_id") String executionId,
        String status,
        String message
) {}
