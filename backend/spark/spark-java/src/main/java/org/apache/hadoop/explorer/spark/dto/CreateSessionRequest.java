package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;

public record CreateSessionRequest(
        @JsonProperty("cluster_id") @NotBlank String clusterId,
        @JsonProperty("spark_version_id") @NotBlank String sparkVersionId,
        @JsonProperty("metastore_id") @NotBlank String metastoreId,
        @JsonProperty("yarn_queue") @NotBlank String yarnQueue,
        @JsonProperty("resource_profile") @NotBlank String resourceProfile,
        String kind,
        @JsonProperty("python_env_id") String pythonEnvId,
        @JsonProperty("custom_python_archive") String customPythonArchive,
        @JsonProperty("custom_python_path") String customPythonPath,
        List<String> packages,
        List<String> jars,
        @JsonProperty("py_files") List<String> pyFiles,
        @JsonProperty("spark_conf") Map<String, String> sparkConf
) {}
