package org.apache.hadoop.explorer.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public record BatchDeleteRequest(
    @JsonProperty("paths") List<String> paths,
    @JsonProperty("recursive") boolean recursive
) {}
