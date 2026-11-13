package io.github.balookrd.pristan.tolmach.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Existing H2 databases (jdbc:h2:file:..., ddl-auto: update) were created with @Lob, i.e. CLOB
 * columns. Hibernate update does not change column types, so the mapping must keep producing CLOB
 * on H2.
 */
@SpringBootTest
@DisplayName("Длинные текстовые колонки Tolmach на H2 совместимы с прежней схемой")
class H2TextColumnsTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("Текстовые колонки остаются CHARACTER LARGE OBJECT")
    void longTextColumnsStayClob() {
        for (String column : new String[] {"QUERY_TEXT", "ERROR_MESSAGE", "COLUMNS_JSON"}) {
            assertEquals("CHARACTER LARGE OBJECT", jdbcTemplate.queryForObject(
                    "select data_type from information_schema.columns where table_name = 'QUERY_HISTORY' and column_name = ?",
                    String.class, column), "QUERY_HISTORY." + column);
        }
        assertEquals("CHARACTER LARGE OBJECT", clobType("SAVED_QUERIES", "DESCRIPTION"));
        assertEquals("CHARACTER LARGE OBJECT", clobType("SAVED_QUERIES", "QUERY_TEXT"));
        assertEquals("CHARACTER LARGE OBJECT", clobType("SQL_USER_WORKSPACES", "STATE_JSON"));
    }

    private String clobType(String table, String column) {
        return jdbcTemplate.queryForObject(
                "select data_type from information_schema.columns where table_name = ? and column_name = ?",
                String.class, table, column);
    }
}
