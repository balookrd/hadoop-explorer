package io.github.balookrd.pristan.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MetastoreItem(
        String id,
        String name,
        @JsonProperty("is_default") boolean defaultMetastore
) {}
