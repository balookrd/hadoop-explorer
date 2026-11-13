package io.github.balookrd.pristan.iskra.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record SparkVersionItem(
        String id,
        String name,
        @JsonProperty("is_default") boolean defaultVersion,
        @JsonProperty("python_versions") List<PythonEnvItem> pythonVersions
) {}
