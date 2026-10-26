package org.apache.hadoop.explorer.replicator.hms.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Разбор сообщений NOTIFICATION_LOG в форматах Hive 3 и Hive 4.
 */
@DisplayName("HMS: разбор сообщений событий метастора")
class HmsEventMessageDecoderTest {

    private static String gzipBase64(String json) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bytes)) {
            gz.write(json.getBytes(StandardCharsets.UTF_8));
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }

    @Test
    @DisplayName("Hive 3 json-0.2, ADD_PARTITION: значения в порядке ключей партиционирования")
    void hive3AddPartition() {
        String msg = "{\"db\":\"sales\",\"table\":\"orders\",\"partitions\":"
                + "[{\"dt\":\"2026-10-01\",\"region\":\"eu\"},{\"dt\":\"2026-10-01\",\"region\":\"us\"}],"
                + "\"partitionListJson\":[\"{}\"]}";

        assertEquals(List.of(List.of("2026-10-01", "eu"), List.of("2026-10-01", "us")),
                HmsEventMessageDecoder.partitionValues(msg, "json-0.2"));
    }

    @Test
    @DisplayName("Hive 3 json-0.2, ALTER_PARTITION: значения из keyValues")
    void hive3AlterPartition() {
        String msg = "{\"db\":\"sales\",\"table\":\"orders\",\"keyValues\":{\"dt\":\"2026-10-01\",\"region\":\"eu\"},"
                + "\"partitionObjBeforeJson\":\"{}\",\"partitionObjAfterJson\":\"{}\"}";

        assertEquals(List.of(List.of("2026-10-01", "eu")), HmsEventMessageDecoder.partitionValues(msg, "json-0.2"));
    }

    @Test
    @DisplayName("Hive 4 gzip(json-2.0): сообщение распаковывается")
    void hive4GzipMessage() throws Exception {
        String json = "{\"db\":\"sales\",\"table\":\"orders\",\"partitions\":[{\"dt\":\"2026-10-02\"}]}";

        assertEquals(List.of(List.of("2026-10-02")),
                HmsEventMessageDecoder.partitionValues(gzipBase64(json), "gzip(json-2.0)"));
    }

    @Test
    @DisplayName("Неразобранное сообщение — пустой результат, а не исключение")
    void garbageYieldsEmpty() {
        assertTrue(HmsEventMessageDecoder.partitionValues("not json", "json-0.2").isEmpty());
        assertTrue(HmsEventMessageDecoder.partitionValues("{}", "json-0.2").isEmpty());
    }

    @Test
    @DisplayName("ALTER_TABLE с переименованием: имена до и после из Thrift-JSON объектов таблицы")
    void renameFromThriftJson() {
        String msg = "{\"db\":\"sales\",\"table\":\"orders\","
                + "\"tableObjBeforeJson\":\"{\\\"1\\\":{\\\"str\\\":\\\"orders\\\"},\\\"2\\\":{\\\"str\\\":\\\"sales\\\"}}\","
                + "\"tableObjAfterJson\":\"{\\\"1\\\":{\\\"str\\\":\\\"orders_v2\\\"},\\\"2\\\":{\\\"str\\\":\\\"sales\\\"}}\"}";

        var rename = HmsEventMessageDecoder.tableRename(msg, "json-0.2").orElseThrow();

        assertEquals("orders", rename.before());
        assertEquals("orders_v2", rename.after());
        assertTrue(rename.isRename());
    }

    @Test
    @DisplayName("INSERT (Hive 3 json-0.2): партиция из ptnObjJson, имена файлов без контрольных сумм")
    void hive3Insert() {
        String msg = "{\"db\":\"sales\",\"table\":\"orders\",\"replace\":\"true\","
                + "\"ptnObjJson\":\"{\\\"1\\\":{\\\"lst\\\":[\\\"str\\\",2,\\\"2026-10-01\\\",\\\"eu\\\"]}}\","
                + "\"files\":[\"hdfs://nn:8020/wh/orders/dt=2026-10-01/region=eu/000000_0#abc123#\","
                + "\"hdfs://nn:8020/wh/orders/dt=2026-10-01/region=eu/000001_0\"]}";

        var insert = HmsEventMessageDecoder.insert(msg, "json-0.2").orElseThrow();

        assertEquals(List.of("2026-10-01", "eu"), insert.partitionValues());
        assertEquals(java.util.Set.of("000000_0", "000001_0"), insert.fileNames());
        assertTrue(insert.replace());
    }
}
