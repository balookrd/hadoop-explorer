package org.apache.hadoop.explorer.replicator.orchestrator.schema;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Миграции на PostgreSQL — боевой базе оркестратора.
 *
 * <p>Запускается только с адресом пустой базы, например на временном контейнере:
 * <pre>
 * docker run -d --rm --name pg-mig -e POSTGRES_PASSWORD=pg -p 55432:5432 postgres:16-alpine
 * REPLICATOR_PG_TEST_URL=jdbc:postgresql://localhost:55432/postgres?user=postgres&amp;password=pg mvn test ...
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "REPLICATOR_PG_TEST_URL", matches = ".+")
@DisplayName("Схема: миграции применяются к PostgreSQL от начала до конца")
class MigrationsApplyOnPostgresTest {

    @Test
    @DisplayName("Все миграции применяются, DLQ не принимает дубль события задачи")
    void allMigrationsApplyAndDlqIsUnique() throws SQLException {
        String url = System.getenv("REPLICATOR_PG_TEST_URL");
        var result = Flyway.configure()
                .dataSource(url, null, null)
                .locations("classpath:db/migration/common", "classpath:db/migration/postgresql")
                .cleanDisabled(false)
                .load();
        result.clean();
        var migrated = result.migrate();
        assertTrue(migrated.migrationsExecuted >= 7, "применено миграций: " + migrated.migrationsExecuted);

        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement()) {
            st.execute("select paused_from_status, bootstrap_checkpoint from hms_replication_jobs where 1 = 0");
            st.execute("insert into hms_dead_letter_events (id, hms_job_id, event_id, event_type, status, retry_count, failed_at) "
                    + "values ('a', 'job', 42, 'ALTER_TABLE', 'NEW', 1, now())");
            assertThrows(SQLException.class, () -> st.execute(
                    "insert into hms_dead_letter_events (id, hms_job_id, event_id, event_type, status, retry_count, failed_at) "
                            + "values ('b', 'job', 42, 'ALTER_TABLE', 'NEW', 1, now())"));
        }
    }
}
