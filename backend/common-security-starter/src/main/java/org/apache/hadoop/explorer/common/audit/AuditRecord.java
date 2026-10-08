package org.apache.hadoop.explorer.common.audit;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Map;

public record AuditRecord(
    @JsonProperty("timestamp") Instant timestamp,
    @JsonProperty("service") String service,
    @JsonProperty("username") String username,
    @JsonProperty("client_ip") String clientIp,
    @JsonProperty("action") String action,
    @JsonProperty("resource") String resource,
    @JsonProperty("status") String status,
    @JsonProperty("duration_ms") long durationMs,
    @JsonProperty("details") Map<String, Object> details
) {}
