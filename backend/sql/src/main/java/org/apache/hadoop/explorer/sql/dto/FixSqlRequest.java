package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record FixSqlRequest(
        @NotBlank String sql,
        @JsonProperty("error_message") @NotBlank String errorMessage,
        String dialect,
        @JsonProperty("cluster_id") String clusterId
) {}
