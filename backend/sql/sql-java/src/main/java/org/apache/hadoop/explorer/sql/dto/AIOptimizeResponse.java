package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record AIOptimizeResponse(
        @JsonProperty("original_sql") String originalSql,
        @JsonProperty("optimized_sql") String optimizedSql,
        List<String> optimizations,
        @JsonProperty("diff_summary") String diffSummary,
        String model,
        String provider,
        @JsonProperty("execution_time_ms") double executionTimeMs
) {}
