package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record StatementResultResponse(
        @JsonProperty("execution_id") String executionId,
        String status,
        List<ColumnMetadata> columns,
        List<List<Object>> rows,
        @JsonProperty("total_rows") int totalRows,
        int offset,
        int limit,
        String logs,
        @JsonProperty("error_message") String errorMessage,
        @JsonProperty("execution_time_ms") double executionTimeMs
) {}
