package org.apache.hadoop.explorer.replicator.hms.service;

import org.apache.hadoop.explorer.replicator.hms.model.HmsFieldSchemaDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.model.HmsSchemaDriftReport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HmsSchemaDriftDetectorTest {

    private final HmsSchemaDriftDetector detector = new HmsSchemaDriftDetector();

    @Test
    @DisplayName("Идентичные схемы не должны регистрировать дрейф")
    void testNoDrift() {
        HmsTableDto src = new HmsTableDto(
                "hive", "test_db", "users", "EXTERNAL_TABLE",
                "hdfs://target-ns/data/users", Map.of(),
                List.of("dt"),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                List.of(new HmsFieldSchemaDto("id", "bigint", null), new HmsFieldSchemaDto("name", "string", null)),
                List.of(new HmsFieldSchemaDto("dt", "string", null)),
                Map.of()
        );

        HmsTableDto dst = new HmsTableDto(
                "hive", "test_db", "users", "EXTERNAL_TABLE",
                "hdfs://target-ns/data/users", Map.of(),
                List.of("dt"),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                List.of(new HmsFieldSchemaDto("id", "bigint", null), new HmsFieldSchemaDto("name", "string", null)),
                List.of(new HmsFieldSchemaDto("dt", "string", null)),
                Map.of()
        );

        HmsSchemaDriftReport report = detector.detectDrift(src, dst);
        assertFalse(report.hasDrift());
        assertTrue(report.discrepancies().isEmpty());
    }

    @Test
    @DisplayName("Дрейф при изменении колонок и несовпадении типов")
    void testColumnDrift() {
        HmsTableDto src = new HmsTableDto(
                "hive", "test_db", "orders", "EXTERNAL_TABLE",
                "hdfs://target-ns/data/orders", Map.of(),
                List.of(),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                List.of(
                        new HmsFieldSchemaDto("order_id", "bigint", null),
                        new HmsFieldSchemaDto("amount", "double", null),
                        new HmsFieldSchemaDto("created_at", "timestamp", null)
                ),
                List.of(),
                Map.of()
        );

        HmsTableDto dstWithDrift = new HmsTableDto(
                "hive", "test_db", "orders", "EXTERNAL_TABLE",
                "hdfs://target-ns/data/orders", Map.of(),
                List.of(),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                List.of(
                        new HmsFieldSchemaDto("order_id", "string", null), // Дрейф типа!
                        new HmsFieldSchemaDto("amount", "double", null),
                        new HmsFieldSchemaDto("legacy_col", "int", null)    // Дополнительная колонка в таргете, отсутствует created_at
                ),
                List.of(),
                Map.of()
        );

        HmsSchemaDriftReport report = detector.detectDrift(src, dstWithDrift);
        assertTrue(report.hasDrift());
        assertEquals(3, report.discrepancies().size());
        assertTrue(report.discrepancies().stream().anyMatch(d -> d.contains("order_id")));
        assertTrue(report.discrepancies().stream().anyMatch(d -> d.contains("created_at")));
        assertTrue(report.discrepancies().stream().anyMatch(d -> d.contains("legacy_col")));
    }
}
