package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ExecuteQueryResponse(
        @JsonProperty("query_id") String queryId,
        String status,
        String message
) {}
