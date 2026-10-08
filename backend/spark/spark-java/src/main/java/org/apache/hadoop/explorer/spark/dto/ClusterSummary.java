package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ClusterSummary(
        String id,
        String name,
        String description,
        String type,
        @JsonProperty("livy_url") String livyUrl,
        @JsonProperty("yarn_cluster_id") String yarnClusterId
) {}
