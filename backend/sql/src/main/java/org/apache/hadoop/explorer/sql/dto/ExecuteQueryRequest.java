package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record ExecuteQueryRequest(
        @JsonProperty("cluster_id") @NotBlank String clusterId,
        @NotBlank String query
) {}
