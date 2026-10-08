package org.apache.hadoop.explorer.sql.dto;

import java.util.Map;

public record WorkspacePayload(Map<String, Object> state) {}
