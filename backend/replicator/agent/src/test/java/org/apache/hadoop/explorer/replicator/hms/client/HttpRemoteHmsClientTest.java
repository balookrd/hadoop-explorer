package org.apache.hadoop.explorer.replicator.hms.client;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Тесты удаленного REST-клиента Hive Metastore на реальном localhost HTTP-сервере.
 *
 * <p>Ключевая проверка (SEC/DATA-LOSS): мутирующие операции обязаны сигнализировать об ошибке,
 * если координатор вернул не-2xx код. Иначе агент репортит APPLIED, оркестратор продвигает
 * lastProcessedEventId, и события метаданных теряются безвозвратно.
 */
@DisplayName("HttpRemoteHmsClient Tests")
class HttpRemoteHmsClientTest {

    private HttpServer server;
    private String baseUrl;
    private final AtomicInteger responseStatus = new AtomicInteger(200);
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final java.util.concurrent.atomic.AtomicReference<String> lastRequestBody =
            new java.util.concurrent.atomic.AtomicReference<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requestCount.incrementAndGet();
            lastRequestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(responseStatus.get(), body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private HttpRemoteHmsClient client() {
        return new HttpRemoteHmsClient(baseUrl, "dc2", "test-secret");
    }

    private static HmsTableDto table() {
        return new HmsTableDto(
                "hive", "analytics", "sales_daily", "EXTERNAL_TABLE",
                "hdfs://ns-dc2:8020/warehouse/analytics.db/sales_daily",
                Map.of(), List.of("dt"),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                List.of(), List.of(), Map.of()
        );
    }

    private static HmsPartitionDto partition() {
        return new HmsPartitionDto(
                "hive", "analytics", "sales_daily", List.of("2026-10-24"),
                "hdfs://ns-dc2:8020/warehouse/analytics.db/sales_daily/dt=2026-10-24",
                Map.of()
        );
    }

    /** Мутирующие операции клиента, каждая из которых обязана падать при не-2xx ответе. */
    private static Stream<org.junit.jupiter.params.provider.Arguments> mutatingOperations() {
        return Stream.of(
                org.junit.jupiter.params.provider.Arguments.of("createDatabase",
                        (java.util.function.Consumer<HttpRemoteHmsClient>) c -> c.createDatabase("analytics", "hdfs://ns-dc2:8020/warehouse/analytics.db")),
                org.junit.jupiter.params.provider.Arguments.of("createTable",
                        (java.util.function.Consumer<HttpRemoteHmsClient>) c -> c.createTable(table())),
                org.junit.jupiter.params.provider.Arguments.of("alterTable",
                        (java.util.function.Consumer<HttpRemoteHmsClient>) c -> c.alterTable(table())),
                org.junit.jupiter.params.provider.Arguments.of("addPartitions",
                        (java.util.function.Consumer<HttpRemoteHmsClient>) c -> c.addPartitions("analytics", "sales_daily", List.of(partition()))),
                org.junit.jupiter.params.provider.Arguments.of("dropTable",
                        (java.util.function.Consumer<HttpRemoteHmsClient>) c -> c.dropTable("analytics", "sales_daily", false)),
                org.junit.jupiter.params.provider.Arguments.of("dropPartition",
                        (java.util.function.Consumer<HttpRemoteHmsClient>) c -> c.dropPartition("analytics", "sales_daily", List.of("2026-10-24"), false))
        );
    }

    @ParameterizedTest(name = "{0} сигнализирует об ошибке при HTTP 500")
    @MethodSource("mutatingOperations")
    @DisplayName("Мутирующая операция бросает исключение при не-2xx ответе координатора")
    void mutatingOperationFailsOnServerError(String name, java.util.function.Consumer<HttpRemoteHmsClient> operation) {
        responseStatus.set(500);
        HttpRemoteHmsClient client = client();

        RuntimeException ex = assertThrows(RuntimeException.class, () -> operation.accept(client),
                "Операция '" + name + "' обязана бросить исключение при HTTP 500, иначе события теряются");
        assertTrue(ex.getMessage().contains("500"),
                "Сообщение должно содержать HTTP-код, получено: " + ex.getMessage());
    }

    @ParameterizedTest(name = "{0} сигнализирует об ошибке при HTTP 401")
    @MethodSource("mutatingOperations")
    @DisplayName("Мутирующая операция бросает исключение при отказе авторизации")
    void mutatingOperationFailsOnUnauthorized(String name, java.util.function.Consumer<HttpRemoteHmsClient> operation) {
        responseStatus.set(401);
        HttpRemoteHmsClient client = client();

        RuntimeException ex = assertThrows(RuntimeException.class, () -> operation.accept(client),
                "Операция '" + name + "' обязана бросить исключение при HTTP 401");
        assertTrue(ex.getMessage().contains("401"),
                "Сообщение должно содержать HTTP-код, получено: " + ex.getMessage());
    }

    @ParameterizedTest(name = "{0} успешно завершается при HTTP 200")
    @MethodSource("mutatingOperations")
    @DisplayName("Мутирующая операция не бросает исключение при успешном ответе")
    void mutatingOperationSucceedsOnOk(String name, java.util.function.Consumer<HttpRemoteHmsClient> operation) {
        responseStatus.set(200);
        HttpRemoteHmsClient client = client();

        assertDoesNotThrow(() -> operation.accept(client),
                "Операция '" + name + "' не должна падать при HTTP 200");
        assertTrue(requestCount.get() > 0, "Запрос должен был дойти до сервера");
    }

    @Test
    @DisplayName("Формат хранения партиции передается координатору в теле запроса")
    void addPartitionsSendsStorageFormat() throws Exception {
        responseStatus.set(200);
        HmsPartitionDto parquetPartition = new HmsPartitionDto(
                "hive", "analytics", "sales_daily", List.of("2026-10-24"),
                "hdfs://ns-dc2:8020/warehouse/analytics.db/sales_daily/dt=2026-10-24",
                Map.of(), List.of(), Map.of(),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        );

        client().addPartitions("analytics", "sales_daily", List.of(parquetPartition));

        String body = lastRequestBody.get();
        assertNotNull(body, "Запрос должен был дойти до сервера");
        assertTrue(body.contains("MapredParquetInputFormat"),
                "Тело запроса обязано содержать input_format партиции, получено: " + body);
        assertTrue(body.contains("ParquetHiveSerDe"),
                "Тело запроса обязано содержать serde_lib партиции, получено: " + body);
    }

    @Test
    @DisplayName("Пустой список партиций не приводит к сетевым вызовам")
    void addPartitionsWithEmptyListSkipsRequest() {
        responseStatus.set(500);
        HttpRemoteHmsClient client = client();

        assertDoesNotThrow(() -> client.addPartitions("analytics", "sales_daily", List.of()));
        assertEquals(0, requestCount.get(), "Для пустого батча сетевой вызов не нужен");
    }
}
