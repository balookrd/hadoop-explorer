package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public record CheckSqlRequest(
        @NotBlank String sql,
        String dialect,
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("catalog_context") Map<String, Object> catalogContext
) {}
