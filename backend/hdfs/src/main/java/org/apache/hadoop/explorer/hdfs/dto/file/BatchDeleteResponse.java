package org.apache.hadoop.explorer.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public record BatchDeleteResponse(
    @JsonProperty("deleted") List<String> deleted,
    @JsonProperty("failed") List<Map<String, String>> failed,
    @JsonProperty("total_requested") int totalRequested,
    @JsonProperty("success") boolean success
) {}
