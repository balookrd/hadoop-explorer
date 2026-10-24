package org.apache.hadoop.explorer.replicator.orchestrator.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Требования к продуктивному профилю конфигурации.
 *
 * <p>Дефолтный профиль рассчитан на локальную разработку: H2 в памяти, {@code ddl-auto: update},
 * mock-аутентификация с тестовыми пользователями. Без отдельного профиля такая установка
 * уезжала в продуктив как есть: состояние задач теряется при рестарте, active-active
 * невозможен (каждый узел видит свою базу), а вход доступен по тестовым паролям.
 */
@DisplayName("Продуктивный профиль конфигурации")
class ProductionProfileTest {

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadProdProfile() {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("application-prod.yml")) {
            assertNotNull(in, "Профиль application-prod.yml обязателен: без него продуктив работает на дефолтах разработки");
            return (Map<String, Object>) new Yaml().load(in);
        } catch (Exception e) {
            throw new AssertionError("Не удалось прочитать application-prod.yml: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object path(Map<String, Object> root, String... keys) {
        Object current = root;
        for (String key : keys) {
            assertInstanceOf(Map.class, current, "Ожидалась секция перед ключом '" + key + "'");
            current = ((Map<String, Object>) current).get(key);
            assertNotNull(current, "В application-prod.yml не задан ключ: " + String.join(".", keys));
        }
        return current;
    }

    @Test
    @DisplayName("База данных продуктива — PostgreSQL, а не H2 в памяти")
    void testProfileUsesPostgres() {
        String url = path(loadProdProfile(), "spring", "datasource", "url").toString();
        assertTrue(url.startsWith("jdbc:postgresql:"),
                "Состояние задач и active-active требуют внешней СУБД, получено: " + url);
    }

    @Test
    @DisplayName("Схема не меняется автоматически")
    void testProfileDoesNotMutateSchema() {
        String ddlAuto = path(loadProdProfile(), "spring", "jpa", "hibernate", "ddl-auto").toString();
        assertEquals("validate", ddlAuto,
                "ddl-auto=update в продуктиве расходится с управляемой схемой и скрывает дрейф модели");
    }

    @Test
    @DisplayName("Mock-аутентификация выключена")
    void testProfileDisablesMockAuth() {
        String authMode = path(loadProdProfile(), "hadoop", "security", "auth", "mode").toString();
        assertNotEquals("mock", authMode, "Mock-аутентификация в продуктиве открывает вход по тестовым паролям");
    }

    @Test
    @DisplayName("TLS для gRPC control plane включен")
    void testProfileEnablesGrpcTls() {
        String tls = path(loadProdProfile(), "hadoop", "replicator", "grpc", "tls-enabled").toString();
        assertTrue(tls.contains("true"), "Канал control plane несет x-agent-secret и обязан быть защищен, получено: " + tls);
    }

    @Test
    @DisplayName("Cookie сессии помечены Secure")
    void testProfileMarksCookiesSecure() {
        String secure = path(loadProdProfile(), "hadoop", "security", "cookie", "secure").toString();
        assertTrue(secure.contains("true"), "Cookie аутентификации обязаны передаваться только по HTTPS, получено: " + secure);
    }

    @Test
    @DisplayName("Детали health-эндпоинта не раскрываются анонимно")
    void testProfileHidesHealthDetails() {
        String showDetails = path(loadProdProfile(), "management", "endpoint", "health", "show-details").toString();
        assertNotEquals("always", showDetails,
                "show-details=always раскрывает состояние БД и компонентов неаутентифицированным запросам");
    }
}
