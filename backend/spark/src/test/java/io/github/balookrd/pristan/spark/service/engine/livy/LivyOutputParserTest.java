package io.github.balookrd.pristan.spark.service.engine.livy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balookrd.pristan.spark.dto.ColumnMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LivyOutputParserTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void sqlOutputBecomesTable() throws Exception {
        LivyOutputParser.Parsed parsed = LivyOutputParser.parse(mapper.readTree("""
                {"status":"ok","execution_count":0,"data":{"application/json":{
                  "schema":{"type":"struct","fields":[
                    {"name":"id","type":"long","nullable":true,"metadata":{}},
                    {"name":"tags","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}}]},
                  "data":[[1,["a"]],[2,null]]}}}"""));

        assertEquals(List.of(new ColumnMetadata("id", "long"), new ColumnMetadata("tags", "array")), parsed.columns());
        assertEquals(1L, parsed.rows().get(0).get(0));
        assertEquals("[\"a\"]", parsed.rows().get(0).get(1));
        assertNull(parsed.rows().get(1).get(1));
        assertNull(parsed.error());
    }

    @Test
    void decimalColumnIsExactTextAndDoubleColumnIsNumber() throws Exception {
        LivyOutputParser.Parsed parsed = LivyOutputParser.parse(LivyOutputParser.MAPPER.readTree("""
                {"status":"ok","data":{"application/json":{
                  "schema":{"type":"struct","fields":[
                    {"name":"price","type":"decimal(38,18)","nullable":true,"metadata":{}},
                    {"name":"whole","type":"decimal(20,0)","nullable":true,"metadata":{}},
                    {"name":"ratio","type":"double","nullable":true,"metadata":{}}]},
                  "data":[[0.100000000000000000000001,98765432109876543210,0.25]]}}}"""));

        assertEquals("0.100000000000000000000001", parsed.rows().get(0).get(0));
        assertEquals("98765432109876543210", parsed.rows().get(0).get(1));
        assertEquals(0.25, parsed.rows().get(0).get(2));
    }

    @Test
    void textOutputGoesToLogs() throws Exception {
        LivyOutputParser.Parsed parsed = LivyOutputParser.parse(mapper.readTree(
                "{\"status\":\"ok\",\"data\":{\"text/plain\":\"+---+\\n| id|\\n+---+\"}}"));

        assertTrue(parsed.columns().isEmpty());
        assertEquals("+---+\n| id|\n+---+", parsed.text());
    }

    @Test
    void errorOutputCarriesExceptionAndTraceback() throws Exception {
        LivyOutputParser.Parsed parsed = LivyOutputParser.parse(mapper.readTree(
                "{\"status\":\"error\",\"ename\":\"NameError\",\"evalue\":\"name 'x' is not defined\","
                        + "\"traceback\":[\"Traceback (most recent call last):\\n\",\"NameError: name 'x' is not defined\\n\"]}"));

        assertEquals("NameError: name 'x' is not defined", parsed.error());
        assertTrue(parsed.text().startsWith("Traceback"));
    }
}
