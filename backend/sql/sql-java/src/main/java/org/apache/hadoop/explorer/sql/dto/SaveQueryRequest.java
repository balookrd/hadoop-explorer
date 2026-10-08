package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record SaveQueryRequest(
        @NotBlank String title,
        @JsonProperty("query_text") @NotBlank String queryText,
        @JsonProperty("cluster_id") String clusterId,
        String description,
        @JsonProperty("is_shared") boolean shared
) {}
