package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SessionResponse(
        String id,
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("spark_version_id") String sparkVersionId,
        @JsonProperty("python_env_id") String pythonEnvId,
        @JsonProperty("custom_python_archive") String customPythonArchive,
        @JsonProperty("custom_python_path") String customPythonPath,
        @JsonProperty("metastore_id") String metastoreId,
        @JsonProperty("yarn_queue") String yarnQueue,
        @JsonProperty("resource_profile") String resourceProfile,
        String kind,
        String status,
        @JsonProperty("yarn_application_id") String yarnApplicationId,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("last_activity_at") String lastActivityAt
) {}
