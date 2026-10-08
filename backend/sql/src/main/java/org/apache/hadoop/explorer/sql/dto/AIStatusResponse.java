package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AIStatusResponse(
        boolean enabled,
        String provider,
        String model,
        @JsonProperty("base_url") String baseUrl,
        boolean available,
        String message,
        @JsonProperty("latency_ms") Double latencyMs
) {}
