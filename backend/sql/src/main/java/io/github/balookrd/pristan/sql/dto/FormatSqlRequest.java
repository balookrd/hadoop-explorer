package io.github.balookrd.pristan.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record FormatSqlRequest(
        @NotBlank String sql,
        String dialect,
        @JsonProperty("cluster_id") String clusterId
) {}
