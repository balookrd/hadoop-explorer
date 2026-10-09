package org.apache.hadoop.explorer.replicator.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.replicator.model.*;
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
import java.util.Collections;
import java.util.List;

/**
 * HTTP REST-клиент для взаимодействия агента репликации с Оркестратором.
 *
 * <p>Использует встроенный в Java 17 {@link HttpClient} с поддержкой HTTP/1.1 и HTTP/2,
 * аутентификации по секретному токену (X-Agent-Secret) и автоматического парсинга JSON.
 */
public class OrchestratorClient {

    private static final Logger logger = LoggerFactory.getLogger(OrchestratorClient.class);

    private final String baseUrl;
    private final String agentSecret;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public OrchestratorClient(String orchestratorUrl, String agentSecret) {
        this(orchestratorUrl, agentSecret, false);
    }

    public OrchestratorClient(String orchestratorUrl, String agentSecret, boolean insecureSkipVerify) {
        this.baseUrl = (orchestratorUrl != null ? orchestratorUrl : "http://localhost:8005").replaceAll("/+$", "");
        this.agentSecret = agentSecret;

        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5));

        if (baseUrl.startsWith("https://") || insecureSkipVerify) {
            try {
                if (insecureSkipVerify) {
                    javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
                    sslContext.init(null, new javax.net.ssl.TrustManager[]{
                            new javax.net.ssl.X509TrustManager() {
                                public void checkClientTrusted(java.security.cert.X509Certificate[] c, String a) {}
                                public void checkServerTrusted(java.security.cert.X509Certificate[] c, String a) {}
                                public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new java.security.cert.X509Certificate[0]; }
                            }
                    }, new java.security.SecureRandom());
                    clientBuilder.sslContext(sslContext);
                    logger.warn("OrchestratorClient настроен с insecureSkipVerify=true для HTTPS");
                }
            } catch (Exception e) {
                logger.error("Не удалось настроить TLS для OrchestratorClient: {}", e.getMessage());
            }
        }

        this.httpClient = clientBuilder.build();
        this.objectMapper = new ObjectMapper();
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

    /**
     * Первичная регистрация агента в Оркестраторе (Service Discovery).
     */
    public boolean register(AgentRegisterRequest request) {
        try {
            String jsonBody = objectMapper.writeValueAsString(request);
            HttpRequest httpRequest = newRequestBuilder("/api/v1/agents/register")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200 || response.statusCode() == 201) {
                logger.info("Агент '{}' успешно зарегистрирован в Оркестраторе {} (HTTP {})",
                        request.getAgentId(), baseUrl, response.statusCode());
                return true;
            } else {
                logger.warn("Ошибка регистрации агента в Оркестраторе: HTTP {} - {}",
                        response.statusCode(), response.body());
                return false;
            }
        } catch (Exception e) {
            logger.warn("Не удалось подключиться к Оркестратору при регистрации: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Отправка периодического keepalive/heartbeat пинга.
     */
    public boolean heartbeat(AgentHeartbeatRequest request) {
        try {
            String jsonBody = objectMapper.writeValueAsString(request);
            HttpRequest httpRequest = newRequestBuilder("/api/v1/agents/heartbeat")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 || response.statusCode() == 201;
        } catch (Exception e) {
            logger.debug("Ошибка отправки heartbeat агента '{}': {}", request.getAgentId(), e.getMessage());
            return false;
        }
    }

    /**
     * Снятие агента с регистрации при штатной остановке (перевод в статус OFFLINE).
     */
    public boolean unregister(String agentId) {
        try {
            String encodedId = URLEncoder.encode(agentId, StandardCharsets.UTF_8);
            HttpRequest httpRequest = newRequestBuilder("/api/v1/agents/unregister?agent_id=" + encodedId)
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                logger.info("Агент '{}' успешно снят с регистрации в Оркестраторе (OFFLINE)", agentId);
                return true;
            }
        } catch (Exception e) {
            logger.debug("Ошибка дерегистрации агента '{}': {}", agentId, e.getMessage());
        }
        return false;
    }

    /**
     * Запрос глобальной сетевой квоты у Оркестратора (Hierarchical Token Bucket).
     *
     * @return время паузы в секундах
     */
    public double requestNetworkTokens(TokenRequest request) {
        try {
            String jsonBody = objectMapper.writeValueAsString(request);
            HttpRequest httpRequest = newRequestBuilder("/api/v1/tokens/request")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                TokenResponse tokenResponse = objectMapper.readValue(response.body(), TokenResponse.class);
                return tokenResponse.getWaitSeconds();
            } else {
                logger.warn("Не удалось запросить сетевую квоту: HTTP {} - {}", response.statusCode(), response.body());
            }
        } catch (Exception e) {
            logger.debug("Исключение при запросе квоты у Оркестратора: {}", e.getMessage());
        }
        return 0.0;
    }

    /**
     * Получение всех задач репликации из Оркестратора.
     */
    public List<JobDto> getJobs() {
        try {
            HttpRequest httpRequest = newRequestBuilder("/api/v1/jobs")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<JobDto>>() {});
            }
        } catch (Exception e) {
            logger.warn("Ошибка получения списка задач из Оркестратора: {}", e.getMessage());
        }
        return Collections.emptyList();
    }

    /**
     * Обновление статуса и прогресса выполнения задачи (PATCH /api/v1/jobs/{jobId}).
     */
    public void updateJobProgress(String jobId, UpdateJobRequest request) {
        try {
            String jsonBody = objectMapper.writeValueAsString(request);
            HttpRequest httpRequest = newRequestBuilder("/api/v1/jobs/" + URLEncoder.encode(jobId, StandardCharsets.UTF_8))
                    .method("PATCH", HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                logger.warn("Ошибка обновления прогресса задачи '{}': HTTP {} - {}",
                        jobId, response.statusCode(), response.body());
            }
        } catch (Exception e) {
            logger.warn("Исключение при обновлении прогресса задачи '{}': {}", jobId, e.getMessage());
        }
    }

    /**
     * Получение списка кластеров из топологии Оркестратора для обнаружения gRPC адресов.
     */
    public List<ClusterDto> getClusters() {
        try {
            HttpRequest httpRequest = newRequestBuilder("/api/v1/clusters")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<ClusterDto>>() {});
            }
        } catch (Exception e) {
            logger.debug("Ошибка получения топологии кластеров: {}", e.getMessage());
        }
        return Collections.emptyList();
    }

    /**
     * Пакетная регистрация пофайловых задач в пуле Оркестратора после этапа анализа.
     */
    public boolean batchCreateTasks(BatchCreateTasksRequest request) {
        try {
            String jsonBody = objectMapper.writeValueAsString(request);
            HttpRequest httpRequest = newRequestBuilder("/api/v1/jobs/" + URLEncoder.encode(request.getJobId(), StandardCharsets.UTF_8) + "/tasks/batch")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 || response.statusCode() == 201;
        } catch (Exception e) {
            logger.warn("Ошибка пакетной регистрации задач для job '{}': {}", request.getJobId(), e.getMessage());
            return false;
        }
    }

    /**
     * Забор задач из распределенного пула Оркестратора воркером.
     */
    public List<TaskItemDto> claimTasks(ClaimTasksRequest request) {
        try {
            String jsonBody = objectMapper.writeValueAsString(request);
            HttpRequest httpRequest = newRequestBuilder("/api/v1/tasks/claim")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<TaskItemDto>>() {});
            }
        } catch (Exception e) {
            logger.debug("Ошибка получения задач из пула Оркестратора: {}", e.getMessage());
        }
        return Collections.emptyList();
    }

    /**
     * Фиксация успешного завершения пофайловой задачи воркером.
     */
    public boolean completeTask(CompleteTaskRequest request) {
        try {
            String jsonBody = objectMapper.writeValueAsString(request);
            HttpRequest httpRequest = newRequestBuilder("/api/v1/tasks/" + URLEncoder.encode(request.getTaskId(), StandardCharsets.UTF_8) + "/complete")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            logger.warn("Ошибка фиксации завершения задачи '{}': {}", request.getTaskId(), e.getMessage());
            return false;
        }
    }

    /**
     * Фиксация ошибки передачи файла воркером.
     */
    public boolean failTask(FailTaskRequest request) {
        try {
            String jsonBody = objectMapper.writeValueAsString(request);
            HttpRequest httpRequest = newRequestBuilder("/api/v1/tasks/" + URLEncoder.encode(request.getTaskId(), StandardCharsets.UTF_8) + "/fail")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            logger.warn("Ошибка фиксации сбоя задачи '{}': {}", request.getTaskId(), e.getMessage());
            return false;
        }
    }

    public String getBaseUrl() {
        return baseUrl;
    }
}
