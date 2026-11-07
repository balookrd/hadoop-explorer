package org.apache.hadoop.explorer.yarn.entity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.apache.hadoop.explorer.yarn.repository.ChangeRequestRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Long text columns on PostgreSQL. Runs only when TEST_POSTGRES_URL points to a database the test
 * may recreate tables in (ddl-auto=create-drop); CI provides one as a service container.
 */
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "TEST_POSTGRES_URL", matches = "jdbc:postgresql:.+")
@DisplayName("Длинные текстовые колонки YARN Explorer на PostgreSQL")
class PostgresTextColumnsTest {

    private static final String[] TEXT_COLUMNS = {
            "changes_json", "diffs_json", "xml_content", "deployment_error", "patch_json"};

    @DynamicPropertySource
    static void postgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> System.getenv("TEST_POSTGRES_URL"));
        registry.add("spring.datasource.driverClassName", () -> "org.postgresql.Driver");
        registry.add("spring.datasource.username", () -> System.getenv().getOrDefault("TEST_POSTGRES_USER", "explorer"));
        registry.add("spring.datasource.password", () -> System.getenv().getOrDefault("TEST_POSTGRES_PASSWORD", "explorer"));
        registry.add("spring.jpa.database-platform", () -> "org.hibernate.dialect.PostgreSQLDialect");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private ChangeRequestRepository repository;

    @Test
    @DisplayName("change_requests: текстовые колонки имеют тип text, а не oid (Large Object)")
    void longTextColumnsAreText() {
        for (String column : TEXT_COLUMNS) {
            assertEquals("text", jdbcTemplate.queryForObject(
                    "select data_type from information_schema.columns where table_name = 'change_requests' and column_name = ?",
                    String.class, column), "change_requests." + column);
        }
    }

    @Test
    @DisplayName("Текст в 1 МБ читается запросом вне транзакции, пустые текстовые поля сохраняются")
    void longTextIsReadableOutsideTransaction() {
        String xml = "<configuration><!-- " + "x".repeat(1_000_000) + " --></configuration>";
        ChangeRequestEntity entity = ChangeRequestEntity.builder()
                .clusterId("prod-yarn")
                .title("Long text")
                .author("admin_user")
                .changesJson("[]")
                .xmlContent(xml)
                .build();
        // diffsJson, deploymentError and patchJson stay null: null binding of the text type is covered too
        Long id = repository.save(entity).getId();

        // No transaction: with @Lob Hibernate maps the column to oid and fails here with
        // "Unable to access lob stream"
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            String read = entityManager
                    .createQuery("select c.xmlContent from ChangeRequestEntity c where c.id = :id", String.class)
                    .setParameter("id", id)
                    .getSingleResult();
            assertEquals(xml, read);
        } finally {
            entityManager.close();
        }
    }
}
