package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ClusterSummary(
        String id,
        String name,
        String type,
        String host,
        int port,
        @JsonProperty("impersonation_enabled") boolean impersonationEnabled,
        @JsonProperty("impersonation_method") String impersonationMethod,
        String catalog,
        @JsonProperty("schema_") String schema
) {}
