package org.apache.hadoop.explorer.replicator.hms.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record HmsPartitionDto(
        String catName,
        String dbName,
        String tableName,
        List<String> values,
        String location,
        Map<String, String> parameters
) {
    public HmsPartitionDto {
        if (catName == null || catName.isBlank()) catName = "hive";
        if (values == null) values = Collections.emptyList();
        if (parameters == null) parameters = Collections.emptyMap();
        else parameters = new HashMap<>(parameters);
    }

    public String getPartitionName(List<String> partKeys) {
        if (partKeys == null || values == null || partKeys.size() != values.size()) {
            return String.join(",", values);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partKeys.size(); i++) {
            if (i > 0) sb.append("/");
            sb.append(partKeys.get(i)).append("=").append(values.get(i));
        }
        return sb.toString();
    }
}
