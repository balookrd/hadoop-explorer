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
        Map<String, String> parameters,
        List<HmsFieldSchemaDto> columns,
        Map<String, String> serdeParameters,
        String inputFormat,
        String outputFormat,
        String serdeLib
) {
    public HmsPartitionDto {
        if (catName == null || catName.isBlank()) catName = "hive";
        if (values == null) values = Collections.emptyList();
        if (parameters == null) parameters = Collections.emptyMap();
        else parameters = new HashMap<>(parameters);
        if (columns == null) columns = Collections.emptyList();
        if (serdeParameters == null) serdeParameters = Collections.emptyMap();
        else serdeParameters = new HashMap<>(serdeParameters);
    }

    /**
     * Конструктор обратной совместимости (6 аргументов).
     */
    public HmsPartitionDto(
            String catName,
            String dbName,
            String tableName,
            List<String> values,
            String location,
            Map<String, String> parameters
    ) {
        this(catName, dbName, tableName, values, location, parameters, Collections.emptyList(), Collections.emptyMap());
    }

    /**
     * Конструктор обратной совместимости (8 аргументов, без формата хранения).
     */
    public HmsPartitionDto(
            String catName,
            String dbName,
            String tableName,
            List<String> values,
            String location,
            Map<String, String> parameters,
            List<HmsFieldSchemaDto> columns,
            Map<String, String> serdeParameters
    ) {
        this(catName, dbName, tableName, values, location, parameters, columns, serdeParameters, null, null, null);
    }

    /**
     * Та же партиция в целевой таблице: меняются только БД, таблица и расположение.
     *
     * <p>Формат хранения и SerDe переносятся как есть: без них приемник подставит текстовый
     * формат, и Parquet/ORC-партиции станут нечитаемыми.
     */
    public HmsPartitionDto relocatedTo(String targetDbName, String targetTableName, String targetLocation) {
        return new HmsPartitionDto(catName, targetDbName, targetTableName, values, targetLocation,
                parameters, columns, serdeParameters, inputFormat, outputFormat, serdeLib);
    }

    /** Та же партиция с другими параметрами (очищенными для приемника). */
    public HmsPartitionDto withParameters(Map<String, String> newParameters) {
        return new HmsPartitionDto(catName, dbName, tableName, values, location,
                newParameters, columns, serdeParameters, inputFormat, outputFormat, serdeLib);
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
