package org.apache.hadoop.explorer.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrossClusterCopyResponse(
    @JsonProperty("success") boolean success,
    @JsonProperty("message") String message,
    @JsonProperty("source_cluster_id") String sourceClusterId,
    @JsonProperty("source_path") String sourcePath,
    @JsonProperty("target_cluster_id") String targetClusterId,
    @JsonProperty("target_path") String targetPath,
    @JsonProperty("copied_files") int copiedFiles,
    @JsonProperty("copied_bytes") long copiedBytes
) {}
