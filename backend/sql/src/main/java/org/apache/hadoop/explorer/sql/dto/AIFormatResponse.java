package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AIFormatResponse(
        @JsonProperty("original_sql") String originalSql,
        @JsonProperty("formatted_sql") String formattedSql,
        String dialect
) {}
