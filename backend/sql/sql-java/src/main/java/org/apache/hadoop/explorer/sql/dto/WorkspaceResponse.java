package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Map;

public record WorkspaceResponse(
        String username,
        Map<String, Object> state,
        @JsonProperty("updated_at") Instant updatedAt
) {}
