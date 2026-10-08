package org.apache.hadoop.explorer.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record CrossClusterCopyRequest(
    @NotBlank @JsonProperty("source_cluster_id") String sourceClusterId,
    @NotBlank @JsonProperty("source_path") String sourcePath,
    @NotBlank @JsonProperty("target_cluster_id") String targetClusterId,
    @NotBlank @JsonProperty("target_path") String targetPath,
    @JsonProperty("overwrite") boolean overwrite
) {}
