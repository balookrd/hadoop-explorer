package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record AICheckResponse(
        @JsonProperty("is_valid") boolean valid,
        List<AIIssue> issues,
        String summary,
        @JsonProperty("complexity_score") int complexityScore,
        @JsonProperty("complexity_level") String complexityLevel,
        @JsonProperty("estimated_notes") List<String> estimatedNotes,
        String model,
        String provider,
        @JsonProperty("execution_time_ms") double executionTimeMs,
        @JsonProperty("fallback_used") boolean fallbackUsed
) {}
