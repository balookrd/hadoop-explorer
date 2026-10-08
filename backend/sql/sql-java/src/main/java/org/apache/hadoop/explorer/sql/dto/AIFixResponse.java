package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AIFixResponse(
        @JsonProperty("original_sql") String originalSql,
        @JsonProperty("fixed_sql") String fixedSql,
        String explanation,
        String model,
        String provider,
        @JsonProperty("execution_time_ms") double executionTimeMs
) {}
