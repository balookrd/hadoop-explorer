package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record PipelineNode(
        String id,
        String name,
        String type, // pyspark, spark_sql, spark_submit, wait
        String code,
        @JsonProperty("timeout_seconds") int timeoutSeconds,
        Map<String, Object> config,
        NodePosition position
) {
    public PipelineNode {
        if (type == null) type = "pyspark";
        if (code == null) code = "";
        if (timeoutSeconds <= 0) timeoutSeconds = 300;
        if (config == null) config = Map.of();
        if (position == null) position = new NodePosition();
    }
}
