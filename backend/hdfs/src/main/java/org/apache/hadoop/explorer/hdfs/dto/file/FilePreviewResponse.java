package org.apache.hadoop.explorer.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record FilePreviewResponse(
    @JsonProperty("cluster_id") String clusterId,
    @JsonProperty("path") String path,
    @JsonProperty("file_type") String fileType,
    @JsonProperty("size") long size,
    @JsonProperty("truncated") boolean truncated,
    @JsonProperty("content") String content,
    @JsonProperty("columns") List<String> columns,
    @JsonProperty("rows") List<List<Object>> rows,
    @JsonProperty("row_count") Integer rowCount,
    @JsonProperty("error") String error
) {}
