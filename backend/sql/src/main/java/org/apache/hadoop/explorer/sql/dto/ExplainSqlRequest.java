package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record ExplainSqlRequest(
        @NotBlank String sql,
        String dialect,
        @JsonProperty("cluster_id") String clusterId
) {}
