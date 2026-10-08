package org.apache.hadoop.explorer.sql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record CachedResultResponse(
        @JsonProperty("query_id") String queryId,
        List<ColumnMetadata> columns,
        List<List<Object>> rows,
        @JsonProperty("total_rows") int totalRows,
        int offset,
        int limit
) {}
