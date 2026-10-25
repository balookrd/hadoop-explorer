package org.apache.hadoop.explorer.replicator.orchestrator.schema;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Схему в работающем приложении создает Flyway, а не Hibernate.
 *
 * <p>Проверка смотрит в саму базу, а не в аннотации: что история миграций заполнена, что
 * индексы заданий физически созданы и что {@code ddl-auto} стоит в {@code validate}. Прежде
 * схему строил {@code ddl-auto: update}, и индексы, описанные в неподключенном DDL-скрипте,
 * существовали только на бумаге.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Схема: владелец — Flyway, индексы существуют в БД")
class SchemaOwnedByFlywayTest {

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private Environment environment;

    @Autowired
    private Flyway flyway;

    @Test
    @DisplayName("Базовая миграция применена и записана в историю Flyway")
    void testBaselineMigrationApplied() {
        List<String> applied = java.util.Arrays.stream(flyway.info().applied())
                .filter(info -> info.getState().isApplied() && !info.getState().isFailed())
                .map(MigrationInfo::getVersion)
                .filter(java.util.Objects::nonNull)
                .map(Object::toString)
                .toList();

        assertFalse(applied.isEmpty(),
                "История Flyway пуста: схему создал кто-то другой, то есть ddl-auto");
        assertTrue(applied.contains("1"),
                "Базовая миграция V1 не применена, применены: " + applied);
    }

    @Test
    @DisplayName("Индексы заданий созданы в базе, а не только описаны в аннотациях")
    void testJobIndexesExistInDatabase() {
        Set<String> indexes = jdbc.queryForList(
                        "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME = 'REPLICATION_JOBS'",
                        String.class).stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        for (String expected : List.of(
                "idx_jobs_status",
                "idx_jobs_type_status",
                "idx_jobs_parent_job_id",
                "idx_jobs_scheduled_next_run",
                "idx_jobs_clusters",
                "idx_jobs_created_by")) {
            assertTrue(indexes.contains(expected),
                    "Индекса " + expected + " нет в базе: запросы к заданиям идут полным просмотром"
                            + " таблицы. Созданы: " + indexes);
        }
    }

    @Test
    @DisplayName("Hibernate схему не меняет, а только сверяет")
    void testHibernateOnlyValidatesSchema() {
        String ddlAuto = environment.getProperty("spring.jpa.hibernate.ddl-auto");

        assertEquals("validate", ddlAuto,
                "ddl-auto должен быть validate: при update Hibernate молча подгоняет базу под"
                        + " модель, и дрейф схемы между окружениями остается незамеченным");
    }
}
