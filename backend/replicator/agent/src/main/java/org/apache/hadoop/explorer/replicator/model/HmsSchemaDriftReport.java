package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.List;

/**
 * Отчет о результатах сравнения схемы таблицы между источником и приемником (Schema Drift Report).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record HmsSchemaDriftReport(
        String dbName,
        String tableName,
        boolean hasDrift,
        List<String> discrepancies,
        Instant checkedAt
) {
    public static HmsSchemaDriftReport noDrift(String db, String table) {
        return new HmsSchemaDriftReport(db, table, false, List.of(), Instant.now());
    }

    public static HmsSchemaDriftReport withDrift(String db, String table, List<String> discrepancies) {
        return new HmsSchemaDriftReport(db, table, true, discrepancies, Instant.now());
    }
}
