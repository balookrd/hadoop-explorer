package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record SparkVersionItem(
        String id,
        String name,
        @JsonProperty("is_default") boolean defaultVersion,
        @JsonProperty("python_versions") List<PythonEnvItem> pythonVersions
) {}
