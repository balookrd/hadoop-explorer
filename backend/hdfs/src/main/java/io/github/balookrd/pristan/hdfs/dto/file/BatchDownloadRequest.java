package io.github.balookrd.pristan.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record BatchDownloadRequest(
    @JsonProperty("paths") List<String> paths
) {}
