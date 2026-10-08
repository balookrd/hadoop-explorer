package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record PipelineRun(
        String id,
        @JsonProperty("pipeline_id") String pipelineId,
        String status, // running, success, failed
        @JsonProperty("node_states") Map<String, NodeExecutionState> nodeStates,
        @JsonProperty("created_at") double createdAt,
        @JsonProperty("finished_at") Double finishedAt
) {}
