package org.apache.hadoop.explorer.replicator.hms.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record HmsTableDto(
        String catName,
        String dbName,
        String tableName,
        String tableType,
        String sdLocation,
        Map<String, String> parameters,
        List<String> partitionKeys,
        String inputFormat,
        String outputFormat,
        String serdeLib
) {
    public HmsTableDto {
        if (catName == null || catName.isBlank()) catName = "hive";
        if (parameters == null) parameters = Collections.emptyMap();
        else parameters = new HashMap<>(parameters);
        if (partitionKeys == null) partitionKeys = Collections.emptyList();
    }

    public boolean isPartitioned() {
        return partitionKeys != null && !partitionKeys.isEmpty();
    }
}
