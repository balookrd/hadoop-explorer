package org.apache.hadoop.explorer.hdfs.dto.cluster;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ClusterPublicInfo(
    @JsonProperty("id") String id,
    @JsonProperty("name") String name,
    @JsonProperty("description") String description,
    @JsonProperty("default_path") String defaultPath,
    @JsonProperty("is_read_only") boolean isReadOnly,
    @JsonProperty("is_admin") boolean isAdmin
) {}
