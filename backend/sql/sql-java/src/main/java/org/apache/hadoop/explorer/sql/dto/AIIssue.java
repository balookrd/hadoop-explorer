package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AIIssue(
        int line,
        int column,
        @JsonProperty("end_line") Integer endLine,
        @JsonProperty("end_column") Integer endColumn,
        String severity,
        String category,
        String message,
        String rule,
        String suggestion
) {}
