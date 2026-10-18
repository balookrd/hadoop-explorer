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
        String serdeLib,
        List<HmsFieldSchemaDto> columns,
        List<HmsFieldSchemaDto> partitionKeyFields,
        Map<String, String> serdeParameters
) {
    public HmsTableDto {
        if (catName == null || catName.isBlank()) catName = "hive";
        if (parameters == null) parameters = Collections.emptyMap();
        else parameters = new HashMap<>(parameters);
        if (partitionKeys == null) {
            partitionKeys = (partitionKeyFields != null)
                    ? partitionKeyFields.stream().map(HmsFieldSchemaDto::name).toList()
                    : Collections.emptyList();
        }
        if (columns == null) columns = Collections.emptyList();
        if (partitionKeyFields == null) {
            partitionKeyFields = partitionKeys.stream()
                    .map(pk -> new HmsFieldSchemaDto(pk, "string", null))
                    .toList();
        }
        if (serdeParameters == null) serdeParameters = Collections.emptyMap();
        else serdeParameters = new HashMap<>(serdeParameters);
    }

    /**
     * Конструктор обратной совместимости (10 аргументов).
     */
    public HmsTableDto(
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
        this(catName, dbName, tableName, tableType, sdLocation, parameters, partitionKeys, inputFormat, outputFormat, serdeLib,
                Collections.emptyList(),
                partitionKeys != null ? partitionKeys.stream().map(pk -> new HmsFieldSchemaDto(pk, "string", null)).toList() : Collections.emptyList(),
                Collections.emptyMap());
    }

    public boolean isPartitioned() {
        return (partitionKeys != null && !partitionKeys.isEmpty()) || (partitionKeyFields != null && !partitionKeyFields.isEmpty());
    }
}
