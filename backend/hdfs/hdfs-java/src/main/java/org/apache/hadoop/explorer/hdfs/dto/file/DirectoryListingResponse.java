package org.apache.hadoop.explorer.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record DirectoryListingResponse(
    @JsonProperty("cluster_id") String clusterId,
    @JsonProperty("path") String path,
    @JsonProperty("parent_path") String parentPath,
    @JsonProperty("files") List<HdfsFileStatus> files,
    @JsonProperty("total_files") int totalFiles,
    @JsonProperty("total_directories") int totalDirectories,
    @JsonProperty("total_size") long totalSize,
    @JsonProperty("can_write") boolean canWrite,
    @JsonProperty("can_read") boolean canRead
) {}
