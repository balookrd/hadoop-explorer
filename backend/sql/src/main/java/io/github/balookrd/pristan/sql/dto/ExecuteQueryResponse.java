package io.github.balookrd.pristan.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ExecuteQueryResponse(
        @JsonProperty("query_id") String queryId,
        String status,
        String message
) {}
