package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record ClusterDetailResponse(
        String id,
        String name,
        String description,
        String type,
        @JsonProperty("yarn_cluster_id") String yarnClusterId,
        @JsonProperty("spark_versions") List<SparkVersionItem> sparkVersions,
        List<MetastoreItem> metastores,
        @JsonProperty("yarn_queues") List<String> yarnQueues,
        @JsonProperty("default_queue") String defaultQueue,
        @JsonProperty("resource_profiles") List<ResourceProfileItem> resourceProfiles,
        @JsonProperty("default_repositories") List<String> defaultRepositories
) {}
