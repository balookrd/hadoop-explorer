package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record ExecuteCodeRequest(
        @JsonProperty("session_id") @NotBlank String sessionId,
        @NotBlank String code,
        String language
) {}
