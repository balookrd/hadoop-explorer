package org.apache.hadoop.explorer.spark.model;

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
@DisplayName("Длинные текстовые колонки Spark Explorer на H2 совместимы с прежней схемой")
class H2TextColumnsTest {

    private static final String[][] TEXT_COLUMNS = {
            {"SPARK_EXECUTION_HISTORY", "CODE"},
            {"SPARK_EXECUTION_HISTORY", "ERROR_MESSAGE"},
            {"SPARK_EXECUTION_HISTORY", "COLUMNS_JSON"},
            {"SPARK_EXECUTION_HISTORY", "LOGS"},
            {"SPARK_SESSIONS", "PACKAGES_JSON"},
            {"SPARK_SESSIONS", "JARS_JSON"},
            {"SPARK_SESSIONS", "PY_FILES_JSON"},
            {"SPARK_SESSIONS", "SPARK_CONF_JSON"},
            {"SPARK_USER_WORKSPACES", "STATE_JSON"},
    };

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("Текстовые колонки остаются CHARACTER LARGE OBJECT")
    void longTextColumnsStayClob() {
        for (String[] column : TEXT_COLUMNS) {
            assertEquals("CHARACTER LARGE OBJECT", jdbcTemplate.queryForObject(
                    "select data_type from information_schema.columns where table_name = ? and column_name = ?",
                    String.class, column[0], column[1]), column[0] + "." + column[1]);
        }
    }
}
