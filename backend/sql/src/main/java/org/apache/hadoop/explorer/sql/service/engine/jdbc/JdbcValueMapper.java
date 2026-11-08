package org.apache.hadoop.explorer.sql.service.engine.jdbc;

import org.apache.hadoop.explorer.sql.dto.ColumnMetadata;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Converts JDBC values into types Jackson serializes without precision loss and the result grid can render.
 */
public final class JdbcValueMapper {

    static final int MAX_BINARY_PREVIEW_BYTES = 64;
    /** Longest CLOB prefix put into a result cell; the grid is not meant for documents. */
    static final int MAX_CLOB_PREVIEW_CHARS = 65_536;

    private JdbcValueMapper() {
    }

    public static List<ColumnMetadata> columns(ResultSetMetaData metaData) throws SQLException {
        int count = metaData.getColumnCount();
        List<ColumnMetadata> columns = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            columns.add(new ColumnMetadata(metaData.getColumnLabel(i), typeName(metaData.getColumnTypeName(i))));
        }
        return columns;
    }

    /** Lower-cased driver type name; "unknown" when the driver does not report one. */
    public static String typeName(String driverTypeName) {
        return driverTypeName == null ? "unknown" : driverTypeName.toLowerCase(Locale.ROOT);
    }

    public static Object toJson(Object value) throws SQLException {
        if (value == null || value instanceof String || value instanceof Boolean
                || value instanceof Integer || value instanceof Long || value instanceof Short
                || value instanceof Byte || value instanceof Double || value instanceof Float) {
            return value;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof BigInteger integer) {
            return integer.toString();
        }
        // java.sql.Timestamp/Date/Time carry the driver's wall clock; their local-date-time views do not depend
        // on the JVM time zone (toInstant() would shift TIMESTAMP WITHOUT TIME ZONE by the JVM offset)
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime().toString();
        }
        if (value instanceof java.sql.Date date) {
            return date.toLocalDate().toString();
        }
        if (value instanceof java.sql.Time time) {
            return time.toLocalTime().toString();
        }
        if (value instanceof java.util.Date date) {
            return date.toInstant().toString();
        }
        if (value instanceof TemporalAccessor) {
            return value.toString();
        }
        if (value instanceof byte[] bytes) {
            return binaryPreview(bytes, bytes.length);
        }
        if (value instanceof Blob blob) {
            try {
                long length = blob.length();
                int shown = (int) Math.min(length, MAX_BINARY_PREVIEW_BYTES);
                return binaryPreview(shown == 0 ? new byte[0] : blob.getBytes(1L, shown), length);
            } finally {
                blob.free();
            }
        }
        if (value instanceof Clob clob) {
            try {
                long length = clob.length();
                int shown = (int) Math.min(length, MAX_CLOB_PREVIEW_CHARS);
                String text = shown == 0 ? "" : clob.getSubString(1L, shown);
                return length > shown ? text + "… (" + length + " символов)" : text;
            } finally {
                clob.free();
            }
        }
        if (value instanceof SQLXML xml) {
            try {
                return xml.getString();
            } finally {
                xml.free();
            }
        }
        if (value instanceof Array array) {
            try {
                return toJson(array.getArray());
            } finally {
                array.free();
            }
        }
        if (value instanceof Struct struct) {
            return toJson(struct.getAttributes());
        }
        if (value instanceof Object[] items) {
            List<Object> out = new ArrayList<>(items.length);
            for (Object item : items) {
                out.add(toJson(item));
            }
            return out;
        }
        if (value.getClass().isArray()) {
            // primitive arrays (int[], double[], boolean[], ...); byte[] is handled above as binary
            int length = java.lang.reflect.Array.getLength(value);
            List<Object> out = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                out.add(toJson(java.lang.reflect.Array.get(value, i)));
            }
            return out;
        }
        if (value instanceof Collection<?> items) {
            List<Object> out = new ArrayList<>(items.size());
            for (Object item : items) {
                out.add(toJson(item));
            }
            return out;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                out.put(String.valueOf(entry.getKey()), toJson(entry.getValue()));
            }
            return out;
        }
        return value.toString();
    }

    /** @param totalLength full size of the value; {@code bytes} may hold only its prefix */
    private static String binaryPreview(byte[] bytes, long totalLength) {
        int shown = (int) Math.min(Math.min(bytes.length, totalLength), MAX_BINARY_PREVIEW_BYTES);
        String hex = HexFormat.of().formatHex(bytes, 0, shown);
        return "0x" + hex + (totalLength > shown ? "… (" + totalLength + " байт)" : "");
    }
}
