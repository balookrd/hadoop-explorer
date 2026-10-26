package org.apache.hadoop.explorer.replicator.hms.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Удаленный REST-клиент Hive Metastore, обращающийся к координатору Оркестратора.
 * Используется агентами Replicator в распределенном Docker/K8s окружении.
 */
/**
 * Клиент метастора поверх HTTP-API оркестратора.
 *
 * <p>Используется там, где агент не ходит в метастор напрямую: демо-стенды и тестовые кластеры
 * (контроллер {@code /api/v1/hms/clusters}, выключенный по умолчанию). Имперсонации на этом
 * транспорте нет — {@code executeAs} из {@link HmsClient} просто выполняет действие, а DDL
 * применяет та сторона от своего имени. Перенос под пользователем — только на транспорте Thrift.
 */
public class HttpRemoteHmsClient implements HmsClient {

    private static final Logger log = LoggerFactory.getLogger(HttpRemoteHmsClient.class);

    /** Предельная длина фрагмента тела ответа, попадающего в сообщение об ошибке. */
    private static final int MAX_ERROR_BODY_CHARS = 500;

    private final String baseUrl;
    private final String clusterId;
    private final String agentSecret;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public HttpRemoteHmsClient(String orchestratorUrl, String clusterId, String agentSecret) {
        this(orchestratorUrl, clusterId, agentSecret, false);
    }

    public HttpRemoteHmsClient(String orchestratorUrl, String clusterId, String agentSecret, boolean insecureSkipVerify) {
        this.baseUrl = (orchestratorUrl != null ? orchestratorUrl : "http://localhost:8005").replaceAll("/+$", "");
        this.clusterId = clusterId != null ? clusterId : "dc1";
        this.agentSecret = agentSecret;

        HttpClient.Builder builder = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5));

        this.httpClient = builder.build();
        this.objectMapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private HttpRequest.Builder newRequestBuilder(String path) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");

        if (agentSecret != null && !agentSecret.isBlank()) {
            builder.header("X-Agent-Secret", agentSecret.trim());
        }
        return builder;
    }

    @Override
    public long getCurrentNotificationEventId() {
        try {
            HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/notifications/current-id")
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                Map<String, Object> map = objectMapper.readValue(response.body(), new TypeReference<>() {});
                Object val = map.get("current_event_id");
                if (val instanceof Number num) {
                    return num.longValue();
                }
            }
        } catch (Exception e) {
            log.warn("[HttpRemoteHmsClient {}] Ошибка получения currentNotificationEventId: {}", clusterId, e.getMessage());
        }
        return 0L;
    }

    @Override
    public List<HmsNotificationEventDto> getNextNotifications(long lastEventId, int maxEvents) {
        try {
            String path = "/api/v1/hms/clusters/" + encode(clusterId) + "/notifications?lastEventId=" + lastEventId + "&maxEvents=" + maxEvents;
            HttpRequest request = newRequestBuilder(path).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<HmsNotificationEventDto>>() {});
            }
        } catch (Exception e) {
            log.warn("[HttpRemoteHmsClient {}] Ошибка получения CDC уведомлений: {}", clusterId, e.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public List<String> getAllDatabases() {
        try {
            HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/databases").GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<String>>() {});
            }
        } catch (Exception e) {
            log.warn("[HttpRemoteHmsClient {}] Ошибка получения списка БД: {}", clusterId, e.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public List<String> getAllTables(String dbName) {
        try {
            HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/tables/" + encode(dbName)).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<String>>() {});
            }
        } catch (Exception e) {
            log.warn("[HttpRemoteHmsClient {}] Ошибка получения таблиц БД '{}': {}", clusterId, dbName, e.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public Optional<HmsTableDto> getTable(String dbName, String tableName) {
        try {
            HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/tables/" + encode(dbName) + "/" + encode(tableName)).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return Optional.of(objectMapper.readValue(response.body(), HmsTableDto.class));
            }
        } catch (Exception e) {
            log.debug("[HttpRemoteHmsClient {}] Таблица '{}.{}' не найдена или ошибка: {}", clusterId, dbName, tableName, e.getMessage());
        }
        return Optional.empty();
    }

    @Override
    public List<HmsPartitionDto> getPartitions(String dbName, String tableName) {
        try {
            HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/tables/" + encode(dbName) + "/" + encode(tableName) + "/partitions").GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<HmsPartitionDto>>() {});
            }
        } catch (Exception e) {
            log.warn("[HttpRemoteHmsClient {}] Ошибка получения партиций '{}.{}': {}", clusterId, dbName, tableName, e.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public void createDatabase(String dbName, String locationUri) {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("db_name", dbName);
        req.put("location_uri", locationUri);
        HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/databases")
                .POST(jsonBody(req))
                .build();
        sendMutating(request, String.format("создание базы '%s'", dbName));
    }

    @Override
    public void createTable(HmsTableDto table) {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("db_name", table.dbName());
        req.put("table_name", table.tableName());
        req.put("table_type", table.tableType());
        req.put("location", table.sdLocation());
        req.put("parameters", table.parameters());
        req.put("partition_keys", table.partitionKeys());
        req.put("input_format", table.inputFormat());
        req.put("output_format", table.outputFormat());
        req.put("serde_lib", table.serdeLib());
        req.put("emit_cdc_event", false);
        req.put("create_sample_data", false);

        HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/tables")
                .POST(jsonBody(req))
                .build();
        sendMutating(request, String.format("создание таблицы '%s.%s'", table.dbName(), table.tableName()));
    }

    @Override
    public void alterTable(HmsTableDto table) {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("db_name", table.dbName());
        req.put("table_name", table.tableName());
        req.put("table_type", table.tableType());
        req.put("location", table.sdLocation());
        req.put("parameters", table.parameters());
        req.put("partition_keys", table.partitionKeys());
        req.put("input_format", table.inputFormat());
        req.put("output_format", table.outputFormat());
        req.put("serde_lib", table.serdeLib());
        req.put("emit_cdc_event", false);

        HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/tables/" + encode(table.dbName()) + "/" + encode(table.tableName()))
                .PUT(jsonBody(req))
                .build();
        sendMutating(request, String.format("обновление таблицы '%s.%s'", table.dbName(), table.tableName()));
    }

    @Override
    public void addPartitions(String dbName, String tableName, List<HmsPartitionDto> partitions) {
        if (partitions == null || partitions.isEmpty()) return;
        for (HmsPartitionDto p : partitions) {
            Map<String, Object> req = new LinkedHashMap<>();
            req.put("values", p.values());
            req.put("location", p.location());
            req.put("parameters", p.parameters());
            req.put("input_format", p.inputFormat());
            req.put("output_format", p.outputFormat());
            req.put("serde_lib", p.serdeLib());
            req.put("emit_cdc_event", false);

            HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/tables/" + encode(dbName) + "/" + encode(tableName) + "/partitions")
                    .POST(jsonBody(req))
                    .build();
            sendMutating(request, String.format("добавление партиции %s в '%s.%s'", p.values(), dbName, tableName));
        }
    }

    @Override
    public void dropTable(String dbName, String tableName, boolean deleteData) {
        HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/tables/" + encode(dbName) + "/" + encode(tableName) + "?deleteData=" + deleteData + "&emitCdcEvent=false")
                .DELETE()
                .build();
        sendMutating(request, String.format("удаление таблицы '%s.%s'", dbName, tableName));
    }

    @Override
    public void dropPartition(String dbName, String tableName, List<String> partVals, boolean deleteData) {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("values", partVals);
        req.put("delete_data", deleteData);
        req.put("emit_cdc_event", false);

        HttpRequest request = newRequestBuilder("/api/v1/hms/clusters/" + encode(clusterId) + "/tables/" + encode(dbName) + "/" + encode(tableName) + "/partitions")
                .method("DELETE", jsonBody(req))
                .build();
        sendMutating(request, String.format("удаление партиции %s из '%s.%s'", partVals, dbName, tableName));
    }

    /**
     * Сериализация тела запроса. Ошибка сериализации — дефект кода, а не удаленной стороны,
     * поэтому она поднимается наверх, а не подменяется «успехом».
     */
    private HttpRequest.BodyPublisher jsonBody(Map<String, Object> payload) {
        try {
            return HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("[HttpRemoteHmsClient " + clusterId + "] Не удалось сериализовать тело запроса: " + e.getMessage(), e);
        }
    }

    /**
     * Выполнение мутирующего запроса к координатору с обязательной проверкой кода ответа.
     *
     * <p>Любой не-2xx код и любой сетевой сбой поднимаются исключением: вызывающая сторона
     * ({@code HmsTransferServiceImpl}) превращает его в {@code success=false}, и оркестратор
     * не продвигает {@code lastProcessedEventId} по непримененному событию. Молчаливое
     * проглатывание ошибки здесь означало бы безвозвратную потерю событий метаданных.
     */
    private void sendMutating(HttpRequest request, String operation) {
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int code = response.statusCode();
            if (code < 200 || code >= 300) {
                String body = response.body() != null ? response.body().strip() : "";
                if (body.length() > MAX_ERROR_BODY_CHARS) {
                    body = body.substring(0, MAX_ERROR_BODY_CHARS) + "…";
                }
                String message = String.format("[HttpRemoteHmsClient %s] Отказ координатора при %s: HTTP %d%s",
                        clusterId, operation, code, body.isEmpty() ? "" : " — " + body);
                log.error(message);
                throw new IllegalStateException(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String message = String.format("[HttpRemoteHmsClient %s] Вызов прерван при %s", clusterId, operation);
            log.error(message);
            throw new IllegalStateException(message, e);
        } catch (IOException e) {
            String message = String.format("[HttpRemoteHmsClient %s] Сетевой сбой при %s: %s",
                    clusterId, operation, e.getMessage());
            log.error(message);
            throw new IllegalStateException(message, e);
        }
    }

    private static String encode(String val) {
        return URLEncoder.encode(val != null ? val : "", StandardCharsets.UTF_8);
    }
}
