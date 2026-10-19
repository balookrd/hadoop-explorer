package org.apache.hadoop.explorer.replicator.kafka.schema.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.replicator.kafka.security.SpnegoAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Легковесный клиент REST API Hortonworks / Cloudera Schema Registry на базе HttpClient
 * с поддержкой аутентификации Kerberos SPNEGO (RFC 4559 Negotiate).
 */
public class HwxRegistryClient {

    private static final Logger log = LoggerFactory.getLogger(HwxRegistryClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String baseUrl;
    private final HttpClient httpClient;
    private final SpnegoAuthenticator spnegoAuthenticator;

    public HwxRegistryClient(String baseUrl) {
        this(baseUrl, HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build(), null);
    }

    public HwxRegistryClient(String baseUrl, SpnegoAuthenticator spnegoAuthenticator) {
        this(baseUrl, HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build(), spnegoAuthenticator);
    }

    public HwxRegistryClient(String baseUrl, HttpClient httpClient) {
        this(baseUrl, httpClient, null);
    }

    public HwxRegistryClient(String baseUrl, HttpClient httpClient, SpnegoAuthenticator spnegoAuthenticator) {
        this.baseUrl = SchemaRegistryUrlValidator.validate(baseUrl);
        this.httpClient = httpClient;
        this.spnegoAuthenticator = spnegoAuthenticator;
        SchemaRegistryUrlValidator.checkSpnegoSecurity(this.baseUrl, spnegoAuthenticator != null);
    }

    /**
     * Получает JSON текст схемы по её Version ID.
     */
    public String getSchemaByVersionId(long versionId) throws IOException, InterruptedException {
        String url = baseUrl + "/api/v1/schemaregistry/schemas/versions/" + versionId;
        HttpRequest.Builder builder = createRequestBuilder(url)
                .header("Accept", "application/json")
                .GET();

        HttpResponse<String> response = sendWithAuthRetry(builder);
        if (response.statusCode() != 200) {
            throw new IOException("Failed to get HWX schema by versionId " + versionId +
                    ", status: " + response.statusCode() + ", body: " + response.body());
        }

        JsonNode root = mapper.readTree(response.body());
        if (root.has("schemaText")) {
            return root.get("schemaText").asText();
        }
        return response.body();
    }

    /**
     * Регистрирует новую версию схемы и возвращает Version ID.
     */
    public long registerSchemaVersion(String schemaName, String schemaText) throws IOException, InterruptedException {
        ensureSchemaMetadataExists(schemaName);

        String url = baseUrl + "/api/v1/schemaregistry/schemas/" + schemaName + "/versions";
        String body = mapper.createObjectNode()
                .put("schemaText", schemaText)
                .put("description", "Replicated schema by Kafka gRPC Replicator")
                .toString();

        HttpRequest.Builder builder = createRequestBuilder(url)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));

        HttpResponse<String> response = sendWithAuthRetry(builder);
        if (response.statusCode() != 200 && response.statusCode() != 201) {
            throw new IOException("Failed to register HWX schema version for " + schemaName +
                    ", status: " + response.statusCode() + ", body: " + response.body());
        }

        JsonNode root = mapper.readTree(response.body());
        if (root.isNumber()) {
            return root.asLong();
        }
        if (root.has("versionId")) {
            return root.get("versionId").asLong();
        }
        if (root.has("id")) {
            return root.get("id").asLong();
        }
        return Long.parseLong(response.body().trim());
    }

    private void ensureSchemaMetadataExists(String schemaName) {
        try {
            String checkUrl = baseUrl + "/api/v1/schemaregistry/schemas/" + schemaName;
            HttpRequest.Builder checkReq = createRequestBuilder(checkUrl)
                    .timeout(Duration.ofSeconds(3))
                    .GET();
            HttpResponse<String> checkResp = sendWithAuthRetry(checkReq);
            if (checkResp.statusCode() == 200) {
                return; // Схема уже существует
            }

            // Создаем метаданные схемы
            String createUrl = baseUrl + "/api/v1/schemaregistry/schemas";
            String createBody = mapper.createObjectNode()
                    .put("type", "avro")
                    .put("schemaGroup", "kafka")
                    .put("name", schemaName)
                    .put("description", "Auto-created by Kafka Replicator")
                    .put("compatibility", "BACKWARD")
                    .toString();

            HttpRequest.Builder createReq = createRequestBuilder(createUrl)
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(createBody));

            sendWithAuthRetry(createReq);
        } catch (Exception e) {
            log.debug("Notice while checking/creating HWX schema metadata for {}: {}", schemaName, e.getMessage());
        }
    }

    private HttpRequest.Builder createRequestBuilder(String url) {
        URI uri = URI.create(url);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(5));

        if (spnegoAuthenticator != null) {
            String negotiateHeader = spnegoAuthenticator.generateNegotiateHeader(uri);
            if (negotiateHeader != null && !negotiateHeader.isBlank()) {
                builder.header("Authorization", negotiateHeader);
            }
        }
        return builder;
    }

    private HttpResponse<String> sendWithAuthRetry(HttpRequest.Builder builder) throws IOException, InterruptedException {
        HttpRequest request = builder.build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        // При получении 401 с вызовом SPNEGO Negotiate
        if (response.statusCode() == 401 && spnegoAuthenticator != null) {
            String wwwAuth = response.headers().firstValue("WWW-Authenticate").orElse("");
            if (wwwAuth.regionMatches(true, 0, "Negotiate", 0, 9) || wwwAuth.toLowerCase().contains("negotiate")) {
                String token = spnegoAuthenticator.generateNegotiateHeader(request.uri());
                if (token != null && !token.isBlank()) {
                    HttpRequest retryReq = HttpRequest.newBuilder(request, (k, v) -> true)
                            .setHeader("Authorization", token)
                            .build();
                    return httpClient.send(retryReq, HttpResponse.BodyHandlers.ofString());
                }
            }
        }
        return response;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public SpnegoAuthenticator getSpnegoAuthenticator() {
        return spnegoAuthenticator;
    }
}
