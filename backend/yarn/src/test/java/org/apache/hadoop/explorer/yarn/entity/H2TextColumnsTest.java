package org.apache.hadoop.explorer.yarn.entity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Existing H2 databases (jdbc:h2:file:..., ddl-auto: update) were created with @Lob and
 * columns with columnDefinition TEXT, which H2 stores as unbounded CHARACTER VARYING. Hibernate
 * update does not change column types, so the mapping must keep producing the same type on H2.
 */
@SpringBootTest
@DisplayName("Длинные текстовые колонки YARN Explorer на H2 совместимы с прежней схемой")
class H2TextColumnsTest {

    private static final String[] TEXT_COLUMNS = {
            "CHANGES_JSON", "DIFFS_JSON", "XML_CONTENT", "DEPLOYMENT_ERROR", "PATCH_JSON"};

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("change_requests: текстовые колонки остаются CHARACTER VARYING")
    void longTextColumnsStayVarying() {
        for (String column : TEXT_COLUMNS) {
            assertEquals("CHARACTER VARYING", jdbcTemplate.queryForObject(
                    "select data_type from information_schema.columns where table_name = 'CHANGE_REQUESTS' and column_name = ?",
                    String.class, column), "CHANGE_REQUESTS." + column);
        }
    }
}
