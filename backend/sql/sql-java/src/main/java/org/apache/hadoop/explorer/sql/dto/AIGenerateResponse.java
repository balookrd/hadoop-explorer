package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record AIGenerateResponse(
        String prompt,
        @JsonProperty("generated_sql") String generatedSql,
        String explanation,
        @JsonProperty("tables_used") List<String> tablesUsed,
        String model,
        String provider,
        @JsonProperty("execution_time_ms") double executionTimeMs,
        @JsonProperty("fallback_used") boolean fallbackUsed
) {}
