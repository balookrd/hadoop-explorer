package org.apache.hadoop.explorer.sql.model;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.apache.hadoop.explorer.sql.repository.QueryHistoryRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Long text columns on PostgreSQL. Runs only when TEST_POSTGRES_URL points to a database the test
 * may recreate tables in (ddl-auto=create-drop); CI provides one as a service container.
 */
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "TEST_POSTGRES_URL", matches = "jdbc:postgresql:.+")
@DisplayName("Длинные текстовые колонки SQL Explorer на PostgreSQL")
class PostgresTextColumnsTest {

    @DynamicPropertySource
    static void postgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> System.getenv("TEST_POSTGRES_URL"));
        registry.add("spring.datasource.username", () -> System.getenv().getOrDefault("TEST_POSTGRES_USER", "explorer"));
        registry.add("spring.datasource.password", () -> System.getenv().getOrDefault("TEST_POSTGRES_PASSWORD", "explorer"));
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private QueryHistoryRepository repository;

    @Test
    @DisplayName("Текстовые колонки имеют тип text, а не oid (Large Object)")
    void longTextColumnsAreText() {
        for (String column : new String[] {"query_text", "error_message", "columns_json"}) {
            assertEquals("text", columnType("query_history", column), "query_history." + column);
        }
        assertEquals("text", columnType("saved_queries", "description"), "saved_queries.description");
        assertEquals("text", columnType("saved_queries", "query_text"), "saved_queries.query_text");
        assertEquals("text", columnType("sql_user_workspaces", "state_json"), "sql_user_workspaces.state_json");
    }

    @Test
    @DisplayName("Текст в 1 МБ читается запросом вне транзакции")
    void longTextIsReadableOutsideTransaction() {
        String text = "select 1 -- " + "x".repeat(1_000_000);
        QueryHistory history = new QueryHistory();
        history.setId(UUID.randomUUID().toString());
        history.setUsername("analyst");
        history.setClusterId("trino-prod");
        history.setClusterName("Trino Production Analytics");
        history.setEngineType("trino");
        history.setQueryText(text);
        repository.save(history);

        // No transaction: with @Lob Hibernate maps the column to oid and fails here with
        // "Unable to access lob stream"
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            String read = entityManager
                    .createQuery("select q.queryText from QueryHistory q where q.id = :id", String.class)
                    .setParameter("id", history.getId())
                    .getSingleResult();
            assertEquals(text, read);
        } finally {
            entityManager.close();
        }
    }

    private String columnType(String table, String column) {
        return jdbcTemplate.queryForObject(
                "select data_type from information_schema.columns where table_name = ? and column_name = ?",
                String.class, table, column);
    }
}
