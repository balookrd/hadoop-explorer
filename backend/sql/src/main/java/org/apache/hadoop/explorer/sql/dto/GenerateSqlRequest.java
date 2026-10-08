package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public record GenerateSqlRequest(
        @NotBlank String prompt,
        String dialect,
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("catalog_context") Map<String, Object> catalogContext
) {}
