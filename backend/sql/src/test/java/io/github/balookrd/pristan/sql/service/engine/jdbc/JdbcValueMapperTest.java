package io.github.balookrd.pristan.sql.service.engine.jdbc;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSetMetaData;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JdbcValueMapperTest {

    @Test
    void keepsJsonNativeValues() throws Exception {
        assertNull(JdbcValueMapper.toJson(null));
        assertEquals(42L, JdbcValueMapper.toJson(42L));
        assertEquals("text", JdbcValueMapper.toJson("text"));
        assertEquals(true, JdbcValueMapper.toJson(true));
    }

    @Test
    void decimalsAreRenderedWithoutPrecisionLoss() throws Exception {
        assertEquals("12345678901234567890.123", JdbcValueMapper.toJson(new BigDecimal("12345678901234567890.123")));
    }

    @Test
    void temporalValuesBecomeStrings() throws Exception {
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2026, 11, 7, 10, 15, 30));
        // Wall clock as returned by the driver, independent of the JVM time zone
        assertEquals("2026-11-07T10:15:30", JdbcValueMapper.toJson(ts));
        assertEquals("2026-11-07", JdbcValueMapper.toJson(java.sql.Date.valueOf("2026-11-07")));
        assertEquals("10:15:30", JdbcValueMapper.toJson(java.sql.Time.valueOf("10:15:30")));
        assertEquals("2026-11-07T10:15:30", JdbcValueMapper.toJson(LocalDateTime.of(2026, 11, 7, 10, 15, 30)));
    }

    @Test
    void binaryIsShownAsTruncatedHex() throws Exception {
        byte[] bytes = new byte[100];
        String rendered = (String) JdbcValueMapper.toJson(bytes);
        assertTrue(rendered.startsWith("0x" + "00".repeat(64)));
        assertTrue(rendered.endsWith("(100 байт)"));
    }

    @Test
    void arraysAndMapsAreConvertedRecursively() throws Exception {
        assertEquals(List.of("a", "1.5"), JdbcValueMapper.toJson(new Object[]{"a", new BigDecimal("1.5")}));
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(1, new BigDecimal("2.50"));
        assertEquals(Map.of("1", "2.50"), JdbcValueMapper.toJson(map));
    }

    @Test
    void missingColumnTypeNameBecomesUnknown() throws Exception {
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(md.getColumnCount()).thenReturn(1);
        when(md.getColumnLabel(1)).thenReturn("c");
        when(md.getColumnTypeName(1)).thenReturn(null);

        assertEquals(List.of(new io.github.balookrd.pristan.sql.dto.ColumnMetadata("c", "unknown")),
                JdbcValueMapper.columns(md));
    }

    @Test
    void primitiveArraysBecomeLists() throws Exception {
        assertEquals(List.of(1, 2), JdbcValueMapper.toJson(new int[]{1, 2}));
        assertEquals(List.of(3L), JdbcValueMapper.toJson(new long[]{3L}));
        assertEquals(List.of(1.5d), JdbcValueMapper.toJson(new double[]{1.5d}));
        assertEquals(List.of(true, false), JdbcValueMapper.toJson(new boolean[]{true, false}));
    }

    @Test
    void sqlArrayIsReadAndFreed() throws Exception {
        Array array = mock(Array.class);
        when(array.getArray()).thenReturn(new Object[]{new BigDecimal("1.0"), "x"});

        assertEquals(List.of("1.0", "x"), JdbcValueMapper.toJson(array));
        verify(array).free();
    }

    @Test
    void clobIsReadAsStringAndFreed() throws Exception {
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn(5L);
        when(clob.getSubString(1, 5)).thenReturn("hello");

        assertEquals("hello", JdbcValueMapper.toJson(clob));
        verify(clob).free();
    }

    @Test
    void longClobIsTruncated() throws Exception {
        Clob clob = mock(Clob.class);
        long length = JdbcValueMapper.MAX_CLOB_PREVIEW_CHARS + 10L;
        when(clob.length()).thenReturn(length);
        when(clob.getSubString(1L, JdbcValueMapper.MAX_CLOB_PREVIEW_CHARS))
                .thenReturn("a".repeat(JdbcValueMapper.MAX_CLOB_PREVIEW_CHARS));

        String rendered = (String) JdbcValueMapper.toJson(clob);
        assertTrue(rendered.startsWith("a".repeat(JdbcValueMapper.MAX_CLOB_PREVIEW_CHARS)));
        assertTrue(rendered.endsWith("(" + length + " символов)"), rendered);
    }

    @Test
    void blobIsShownLikeBinaryAndFreed() throws Exception {
        Blob blob = mock(Blob.class);
        when(blob.length()).thenReturn(100L);
        when(blob.getBytes(1L, JdbcValueMapper.MAX_BINARY_PREVIEW_BYTES))
                .thenReturn(new byte[JdbcValueMapper.MAX_BINARY_PREVIEW_BYTES]);

        String rendered = (String) JdbcValueMapper.toJson(blob);
        assertTrue(rendered.startsWith("0x" + "00".repeat(64)));
        assertTrue(rendered.endsWith("(100 байт)"), rendered);
        verify(blob).free();
    }

    @Test
    void sqlXmlIsReadAsStringAndFreed() throws Exception {
        SQLXML xml = mock(SQLXML.class);
        when(xml.getString()).thenReturn("<a/>");

        assertEquals("<a/>", JdbcValueMapper.toJson(xml));
        verify(xml).free();
    }
}
