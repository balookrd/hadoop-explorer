package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record AIExplainResponse(
        String explanation,
        String summary,
        @JsonProperty("tables_used") List<String> tablesUsed,
        List<String> operations,
        String model,
        String provider,
        @JsonProperty("execution_time_ms") double executionTimeMs
) {}
