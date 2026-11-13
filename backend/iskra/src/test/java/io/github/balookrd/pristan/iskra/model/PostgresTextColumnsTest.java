package io.github.balookrd.pristan.iskra.model;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import io.github.balookrd.pristan.iskra.repository.SparkExecutionHistoryRepository;
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
@DisplayName("Длинные текстовые колонки Iskra на PostgreSQL")
class PostgresTextColumnsTest {

    private static final String[][] TEXT_COLUMNS = {
            {"spark_execution_history", "code"},
            {"spark_execution_history", "error_message"},
            {"spark_execution_history", "columns_json"},
            {"spark_execution_history", "logs"},
            {"spark_sessions", "packages_json"},
            {"spark_sessions", "jars_json"},
            {"spark_sessions", "py_files_json"},
            {"spark_sessions", "spark_conf_json"},
            {"spark_user_workspaces", "state_json"},
    };

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
    private SparkExecutionHistoryRepository repository;

    @Test
    @DisplayName("Текстовые колонки имеют тип text, а не oid (Large Object)")
    void longTextColumnsAreText() {
        for (String[] column : TEXT_COLUMNS) {
            assertEquals("text", jdbcTemplate.queryForObject(
                    "select data_type from information_schema.columns where table_name = ? and column_name = ?",
                    String.class, column[0], column[1]), column[0] + "." + column[1]);
        }
    }

    @Test
    @DisplayName("Код в 1 МБ читается запросом вне транзакции, пустые текстовые поля сохраняются")
    void longTextIsReadableOutsideTransaction() {
        String code = "print(1)\n# " + "x".repeat(1_000_000);
        SparkExecutionHistory history = new SparkExecutionHistory();
        history.setId(UUID.randomUUID().toString());
        history.setSessionId("session-1");
        history.setUsername("analyst");
        history.setClusterId("prod-analytics");
        history.setCode(code);
        // errorMessage, columnsJson and logs stay null: null binding of the text type is covered too
        repository.save(history);

        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            String read = entityManager
                    .createQuery("select h.code from SparkExecutionHistory h where h.id = :id", String.class)
                    .setParameter("id", history.getId())
                    .getSingleResult();
            assertEquals(code, read);
        } finally {
            entityManager.close();
        }
    }
}
