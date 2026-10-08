package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ResourceProfileItem(
        String id,
        String name,
        @JsonProperty("driver_memory") String driverMemory,
        @JsonProperty("driver_cores") int driverCores,
        @JsonProperty("executor_memory") String executorMemory,
        @JsonProperty("executor_cores") int executorCores,
        @JsonProperty("num_executors") int numExecutors
) {}
