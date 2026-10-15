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
 * Легковесный клиент REST API Confluent Schema Registry на базе нативного Java HttpClient
 * с поддержкой аутентификации Kerberos SPNEGO (RFC 4559 Negotiate).
 */
public class ConfluentRegistryClient {

    private static final Logger log = LoggerFactory.getLogger(ConfluentRegistryClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String baseUrl;
    private final HttpClient httpClient;
    private final SpnegoAuthenticator spnegoAuthenticator;

    public ConfluentRegistryClient(String baseUrl) {
        this(baseUrl, HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build(), null);
    }

    public ConfluentRegistryClient(String baseUrl, SpnegoAuthenticator spnegoAuthenticator) {
        this(baseUrl, HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build(), spnegoAuthenticator);
    }

    public ConfluentRegistryClient(String baseUrl, HttpClient httpClient) {
        this(baseUrl, httpClient, null);
    }

    public ConfluentRegistryClient(String baseUrl, HttpClient httpClient, SpnegoAuthenticator spnegoAuthenticator) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("baseUrl must not be blank");
        }
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.httpClient = httpClient;
        this.spnegoAuthenticator = spnegoAuthenticator;
    }

    /**
     * Получает текст схемы по её Schema ID.
     */
    public String getSchemaById(int id) throws IOException, InterruptedException {
        String url = baseUrl + "/schemas/ids/" + id;
        HttpRequest.Builder builder = createRequestBuilder(url)
                .header("Accept", "application/vnd.schemaregistry.v1+json")
                .GET();

        HttpResponse<String> response = sendWithAuthRetry(builder);
        if (response.statusCode() != 200) {
            throw new IOException("Failed to get schema by id " + id + ", status: " + response.statusCode() + ", body: " + response.body());
        }

        JsonNode root = mapper.readTree(response.body());
        return root.has("schema") ? root.get("schema").asText() : response.body();
    }

    /**
     * Регистрирует схему в Subject и возвращает присвоенный Schema ID.
     */
    public int registerSchema(String subject, String schemaJson) throws IOException, InterruptedException {
        String url = baseUrl + "/subjects/" + subject + "/versions";
        String requestBody = mapper.createObjectNode()
                .put("schema", schemaJson)
                .toString();

        HttpRequest.Builder builder = createRequestBuilder(url)
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .header("Accept", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody));

        HttpResponse<String> response = sendWithAuthRetry(builder);
        if (response.statusCode() != 200) {
            throw new IOException("Failed to register schema for subject " + subject + ", status: " + response.statusCode() + ", body: " + response.body());
        }

        JsonNode root = mapper.readTree(response.body());
        return root.get("id").asInt();
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

        // При получении 401 Unauthorized с вызовом SPNEGO Negotiate
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
