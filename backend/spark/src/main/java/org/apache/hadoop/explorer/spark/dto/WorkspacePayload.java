package org.apache.hadoop.explorer.spark.dto;

import java.util.Map;

public record WorkspacePayload(Map<String, Object> state) {}
