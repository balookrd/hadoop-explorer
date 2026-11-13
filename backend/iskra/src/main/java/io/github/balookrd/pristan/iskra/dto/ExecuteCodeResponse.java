package io.github.balookrd.pristan.iskra.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ExecuteCodeResponse(
        @JsonProperty("execution_id") String executionId,
        String status,
        String message
) {}
