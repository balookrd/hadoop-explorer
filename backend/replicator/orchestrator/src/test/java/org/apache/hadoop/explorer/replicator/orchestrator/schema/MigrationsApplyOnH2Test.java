package org.apache.hadoop.explorer.replicator.orchestrator.schema;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Миграции действительно выполняются, а не только совпадают с моделью по тексту.
 */
@DisplayName("Схема: миграции применяются к H2 от начала до конца")
class MigrationsApplyOnH2Test {

    @Test
    @DisplayName("Все миграции применяются, DLQ не принимает дубль события задачи")
    void allMigrationsApplyAndDlqIsUnique() throws SQLException {
        String url = "jdbc:h2:mem:migrations_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        var result = Flyway.configure()
                .dataSource(url, "sa", "")
                .locations("classpath:db/migration/common", "classpath:db/migration/h2")
                .load()
                .migrate();
        assertTrue(result.migrationsExecuted >= 6, "применено миграций: " + result.migrationsExecuted);

        try (Connection c = DriverManager.getConnection(url, "sa", ""); Statement st = c.createStatement()) {
            st.execute("select paused_from_status from hms_replication_jobs where 1 = 0");
            st.execute("insert into hms_dead_letter_events (id, hms_job_id, event_id, event_type, status, retry_count, failed_at) "
                    + "values ('a', 'job', 42, 'ALTER_TABLE', 'NEW', 1, current_timestamp)");
            assertThrows(SQLException.class, () -> st.execute(
                    "insert into hms_dead_letter_events (id, hms_job_id, event_id, event_type, status, retry_count, failed_at) "
                            + "values ('b', 'job', 42, 'ALTER_TABLE', 'NEW', 1, current_timestamp)"));
        }
    }
}
