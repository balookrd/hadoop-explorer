package io.github.balookrd.pristan.iskra.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PipelineEdge(
        @JsonProperty("from_node_id") String fromNodeId,
        @JsonProperty("to_node_id") String toNodeId
) {}
