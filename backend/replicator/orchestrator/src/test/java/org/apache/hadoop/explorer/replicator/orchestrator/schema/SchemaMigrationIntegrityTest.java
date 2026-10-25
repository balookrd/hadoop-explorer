package org.apache.hadoop.explorer.replicator.orchestrator.schema;

import jakarta.persistence.Entity;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Схема базы данных должна описываться миграциями, и ровно теми таблицами, что ждет модель JPA.
 *
 * <p>Схему строил {@code ddl-auto: update} по аннотациям, а единственный DDL-скрипт
 * ({@code schema-postgresql.sql}) ни к чему не подключался — ни {@code spring.sql.init}, ни Flyway.
 * Поэтому он тихо разошелся с моделью: таблицы для журнала событий HMS и его DLQ в нем вообще
 * отсутствовали, снимки DR и аренда стримера назывались иначе, чем в сущностях, а блокировки
 * кластера были описаны дважды в двух несовместимых вариантах. В продуктиве с
 * {@code ddl-auto: validate} такое приложение не поднялось бы вовсе, а индексы, партиционирование
 * и частичные индексы из скрипта никогда и никуда не применялись.
 */
@DisplayName("Схема: миграции как единственный источник истины")
class SchemaMigrationIntegrityTest {

    private static final String ENTITY_PACKAGE =
            "org.apache.hadoop.explorer.replicator.orchestrator.entity";

    /** CREATE TABLE в миграции: имя таблицы без схемы и кавычек. */
    private static final Pattern CREATE_TABLE = Pattern.compile(
            "create\\s+table\\s+(?:if\\s+not\\s+exists\\s+)?\"?([a-z0-9_]+)\"?",
            Pattern.CASE_INSENSITIVE);

    private List<Class<?>> entityClasses() throws ClassNotFoundException {
        var scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Entity.class));
        List<Class<?>> classes = new ArrayList<>();
        for (BeanDefinition bd : scanner.findCandidateComponents(ENTITY_PACKAGE)) {
            classes.add(Class.forName(bd.getBeanClassName()));
        }
        assertFalse(classes.isEmpty(), "Сущности JPA не найдены: тест потерял цель проверки");
        return classes;
    }

    private Set<String> entityTables() throws ClassNotFoundException {
        Set<String> tables = new TreeSet<>();
        for (Class<?> entity : entityClasses()) {
            Table table = entity.getAnnotation(Table.class);
            assertNotNull(table, entity.getSimpleName()
                    + " без @Table: имя таблицы задается неявной стратегией и расходится с миграцией");
            assertFalse(table.name().isBlank(), entity.getSimpleName()
                    + ": @Table без name, имя таблицы выводится неявно");
            tables.add(table.name().toLowerCase());
        }
        return tables;
    }

    private List<Resource> migrations() throws IOException {
        var resolver = new PathMatchingResourcePatternResolver();
        List<Resource> found = new ArrayList<>(List.of(
                resolver.getResources("classpath*:db/migration/**/*.sql")));
        assertFalse(found.isEmpty(),
                "Миграций нет: схему по-прежнему строит ddl-auto, а дрейф модели никем не замечается");
        return found;
    }

    private Set<String> migrationTables() throws IOException {
        Set<String> tables = new TreeSet<>();
        for (Resource migration : migrations()) {
            String sql = new String(migration.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            Matcher matcher = CREATE_TABLE.matcher(sql);
            while (matcher.find()) {
                tables.add(matcher.group(1).toLowerCase());
            }
        }
        return tables;
    }

    @Test
    @DisplayName("Каждой сущности соответствует таблица в миграциях")
    void testEveryEntityTableIsCreatedByMigrations() throws Exception {
        Set<String> entityTables = entityTables();
        Set<String> migrated = migrationTables();

        Set<String> missing = entityTables.stream()
                .filter(t -> !migrated.contains(t))
                .collect(Collectors.toCollection(TreeSet::new));

        assertTrue(missing.isEmpty(),
                "Миграции не создают таблицы, которые ждет модель JPA: " + missing
                        + ". С ddl-auto: validate приложение не поднимется, с update — схема"
                        + " разъедется между окружениями");
    }

    @Test
    @DisplayName("Миграции не создают таблиц, которых нет в модели")
    void testMigrationsCreateNoOrphanTables() throws Exception {
        Set<String> entityTables = entityTables();
        Set<String> orphans = migrationTables().stream()
                .filter(t -> !entityTables.contains(t))
                .filter(t -> !t.startsWith("flyway_"))
                .collect(Collectors.toCollection(TreeSet::new));

        assertTrue(orphans.isEmpty(),
                "Миграции создают таблицы, с которыми не работает ни одна сущность: " + orphans
                        + ". Это либо устаревшее имя таблицы, либо дубль — и в обоих случаях"
                        + " данные пишутся не туда, где их ищут");
    }

    @Test
    @DisplayName("JobEntity имеет индексы под запросы планировщика")
    void testJobEntityIsIndexed() {
        Table table = org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity.class
                .getAnnotation(Table.class);
        assertNotNull(table);

        Set<String> indexedColumns = new TreeSet<>();
        for (Index index : table.indexes()) {
            indexedColumns.add(index.columnList().replace(" ", "").toLowerCase());
        }

        assertFalse(indexedColumns.isEmpty(),
                "replication_jobs без индексов: тик планировщика каждые 5 секунд и опрос задач"
                        + " агентами идут seq scan по всей таблице заданий");
        assertTrue(indexedColumns.stream().anyMatch(c -> c.startsWith("status")),
                "Нет индекса по status: выборка активных заданий сканирует таблицу целиком, "
                        + "найдено: " + indexedColumns);
        assertTrue(indexedColumns.stream().anyMatch(c -> c.contains("source_cluster_id")),
                "Нет индекса по кластеру-источнику: опрос заданий ЦОД агентами сканирует таблицу"
                        + " целиком, найдено: " + indexedColumns);
    }
}
