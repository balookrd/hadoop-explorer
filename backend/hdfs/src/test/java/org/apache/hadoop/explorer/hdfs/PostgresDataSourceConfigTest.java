package org.apache.hadoop.explorer.hdfs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("PostgreSQL как spring.datasource сервиса")
class PostgresDataSourceConfigTest {

    // Read from disk: src/test/resources may hold an application.yml that shadows the packaged one
    private static final String PACKAGED_CONFIG = "src/main/resources/application.yml";

    @Test
    @DisplayName("Драйвер org.postgresql.Driver есть на runtime classpath сервиса")
    void postgresDriverIsOnRuntimeClasspath() throws Exception {
        assertNotNull(Class.forName("org.postgresql.Driver"));
    }

    @Test
    @DisplayName("SPRING_DATASOURCE_URL=jdbc:postgresql:... выбирает драйвер PostgreSQL")
    void postgresUrlSelectsPostgresDriver() throws IOException {
        DataSourceProperties datasource = bindDatasource(
                Map.of("SPRING_DATASOURCE_URL", "jdbc:postgresql://db.example.com:5432/explorer"));

        assertEquals("org.postgresql.Driver", datasource.determineDriverClassName());
    }

    @Test
    @DisplayName("Без переменных окружения остается встроенная H2")
    void defaultUrlKeepsH2() throws IOException {
        assertEquals("org.h2.Driver", bindDatasource(Map.of()).determineDriverClassName());
    }

    /** OS environment over the packaged application.yml, as in the running service. */
    private static DataSourceProperties bindDatasource(Map<String, String> env) throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().replace(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        new HashMap<String, Object>(env)));
        new YamlPropertySourceLoader()
                .load("packaged application.yml", new FileSystemResource(PACKAGED_CONFIG))
                .forEach(environment.getPropertySources()::addLast);
        return Binder.get(environment).bind("spring.datasource", DataSourceProperties.class).get();
    }
}
