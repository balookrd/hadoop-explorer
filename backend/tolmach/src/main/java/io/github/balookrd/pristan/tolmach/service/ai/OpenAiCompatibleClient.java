package io.github.balookrd.pristan.tolmach.service.ai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balookrd.pristan.tolmach.config.SqlProperties;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Chat Completions API of OpenAI-compatible servers (vLLM, Ollama /v1, LM Studio, OpenAI).
 *
 * <p>The JDK client is used directly instead of Spring {@code RestClient}: the read timeout of
 * {@code JdkClientHttpRequestFactory} (Spring 6.1) ends once the response headers arrive, so a server that
 * stalls in the middle of the body would hold the request thread. Here {@code timeout-seconds} is a deadline for
 * the whole answer, after which the exchange is cancelled.
 *
 * <p>Error messages go to the log; {@link LlmUnavailableException#getSummary()} is the short form for API
 * responses. Both carry the URL without user info, the API key and any {@code Bearer}/{@code Basic} token are
 * masked, model and server texts are cut.
 */
public class OpenAiCompatibleClient {

    /** Larger answers are dropped: a chat completion with the requested JSON is a few kilobytes. */
    public static final int MAX_RESPONSE_BYTES = 1024 * 1024;

    private static final Duration MAX_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final int MAX_TEXT_IN_MESSAGE = 200;
    private static final Pattern AUTH_TOKEN = Pattern.compile("(?i)\\b(Bearer|Basic)\\s+[A-Za-z0-9+/=._~-]+");

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final String model;
    private final URI baseUri;
    private final String displayUrl;
    private final String apiKey;
    private final Duration timeout;
    private final Duration statusTimeout;
    private final int maxConcurrentRequests;
    private final Semaphore permits;

    public OpenAiCompatibleClient(SqlProperties.AiConfig config, ObjectMapper mapper) {
        requirePositive("tolmach.ai.timeout-seconds", config.getTimeoutSeconds());
        requirePositive("tolmach.ai.status-timeout-seconds", config.getStatusTimeoutSeconds());
        requirePositive("tolmach.ai.max-concurrent-requests", config.getMaxConcurrentRequests());
        this.displayUrl = displayUrl(config.getUrl());
        this.baseUri = parseBaseUri(config.getUrl(), displayUrl);
        this.apiKey = normalizeApiKey(config.getApiKey());
        this.timeout = Duration.ofSeconds(config.getTimeoutSeconds());
        this.statusTimeout = Duration.ofSeconds(Math.min(config.getStatusTimeoutSeconds(), config.getTimeoutSeconds()));
        this.maxConcurrentRequests = config.getMaxConcurrentRequests();
        this.permits = new Semaphore(maxConcurrentRequests);
        this.http = HttpClient.newBuilder()
                .connectTimeout(timeout.compareTo(MAX_CONNECT_TIMEOUT) < 0 ? timeout : MAX_CONNECT_TIMEOUT)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        this.mapper = mapper;
        this.model = config.getModel();
    }

    /**
     * Sends one system and one user message and returns the JSON object the model answered with. When
     * {@code max-concurrent-requests} calls are already waiting for the model, fails at once instead of queueing.
     */
    public JsonNode completeJson(String systemPrompt, String userPrompt) throws LlmUnavailableException {
        if (!permits.tryAcquire()) {
            throw new LlmUnavailableException("LLM " + displayUrl + ": уже выполняется " + maxConcurrentRequests
                    + " вызовов (tolmach.ai.max-concurrent-requests)", "занята", null);
        }
        try {
            return doCompleteJson(systemPrompt, userPrompt);
        } finally {
            permits.release();
        }
    }

    /** Round trip to GET /models in milliseconds, bounded by {@code status-timeout-seconds}. */
    public double ping() throws LlmUnavailableException {
        long start = System.nanoTime();
        send("GET /models", () -> request("/models", statusTimeout).GET().build(), statusTimeout);
        return (System.nanoTime() - start) / 1_000_000.0;
    }

    /** Models often wrap JSON in ```json ... ``` despite the instruction. */
    public static String stripCodeFence(String content) {
        String text = content.strip();
        if (text.startsWith("```")) {
            int firstNewline = text.indexOf('\n');
            int closing = text.lastIndexOf("```");
            if (firstNewline > 0 && closing > firstNewline) {
                return text.substring(firstNewline + 1, closing).strip();
            }
        }
        return text;
    }

    /** Base URL for the status response and messages: without user info, query and fragment. */
    public static String displayUrl(String url) {
        try {
            URI uri = URI.create(url);
            if (uri.getHost() == null) {
                return "<invalid tolmach.ai.url>";
            }
            return uri.getScheme() + "://" + uri.getHost() + (uri.getPort() >= 0 ? ":" + uri.getPort() : "")
                    + (uri.getRawPath() != null ? uri.getRawPath() : "");
        } catch (RuntimeException e) {
            return "<invalid tolmach.ai.url>";
        }
    }

    private JsonNode doCompleteJson(String systemPrompt, String userPrompt) throws LlmUnavailableException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", model);
        body.put("temperature", 0);
        body.put("messages", List.of(
                Map.of("role", "system", "content", systemPrompt),
                Map.of("role", "user", "content", userPrompt)));
        String json;
        try {
            json = mapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new LlmUnavailableException("запрос к модели не сериализуется в JSON", e);
        }
        String responseBody = send("POST /chat/completions", () -> request("/chat/completions", timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build(), timeout);

        JsonNode response;
        try {
            response = mapper.readTree(responseBody);
        } catch (JsonProcessingException e) {
            throw new LlmUnavailableException("LLM " + displayUrl + " POST /chat/completions: ответ не является JSON", e);
        }
        String content = response == null ? "" : response.path("choices").path(0).path("message").path("content").asText("");
        if (content.isBlank()) {
            throw new LlmUnavailableException("пустой ответ модели");
        }
        try {
            JsonNode result = mapper.readTree(stripCodeFence(content));
            if (result == null || !result.isObject()) {
                throw new LlmUnavailableException("ответ модели не JSON-объект");
            }
            return result;
        } catch (JsonProcessingException e) {
            throw new LlmUnavailableException("ответ модели не JSON: " + safeText(content), e);
        }
    }

    private HttpRequest.Builder request(String path, Duration requestTimeout) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(baseUri + path))
                .timeout(requestTimeout)
                .header("Accept", "application/json");
        if (apiKey != null) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        return builder;
    }

    private String send(String operation, Supplier<HttpRequest> requestFactory, Duration deadline) throws LlmUnavailableException {
        CompletableFuture<HttpResponse<String>> future;
        try {
            future = http.sendAsync(requestFactory.get(), info -> new LimitedStringSubscriber(MAX_RESPONSE_BYTES));
        } catch (RuntimeException e) {
            // e.g. IllegalArgumentException from header validation: its text may contain the header value
            throw new LlmUnavailableException("LLM " + displayUrl + " " + operation + ": запрос не отправлен: "
                    + e.getClass().getSimpleName() + (e.getMessage() != null ? ": " + safeText(e.getMessage()) : ""),
                    e.getClass().getSimpleName(), null);
        }
        HttpResponse<String> response;
        try {
            response = future.get(deadline.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw timedOut(operation, deadline, e);
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new LlmUnavailableException("LLM " + displayUrl + " " + operation + ": ожидание прервано", "прервано", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof HttpConnectTimeoutException) {
                long connectMs = Math.min(timeout.toMillis(), MAX_CONNECT_TIMEOUT.toMillis());
                throw new LlmUnavailableException("LLM " + displayUrl + ": соединение не установлено за "
                        + connectMs + " мс (" + operation + ")", "таймаут подключения", cause);
            }
            if (cause instanceof HttpTimeoutException) {
                throw timedOut(operation, deadline, cause);
            }
            throw new LlmUnavailableException("LLM " + displayUrl + " недоступна (" + operation + "): "
                    + cause.getClass().getSimpleName() + (cause.getMessage() != null ? ": " + safeText(cause.getMessage()) : ""),
                    cause.getClass().getSimpleName(), cause);
        }
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            String body = response.body();
            throw new LlmUnavailableException("LLM " + displayUrl + " " + operation + ": HTTP " + status
                    + (body == null || body.isBlank() ? "" : " " + safeText(body)), "HTTP " + status, null);
        }
        return response.body();
    }

    private LlmUnavailableException timedOut(String operation, Duration deadline, Throwable cause) {
        return new LlmUnavailableException("LLM " + displayUrl + " не ответила за " + deadline.toSeconds()
                + " с (" + operation + ")", "таймаут " + deadline.toSeconds() + " с", cause);
    }

    /** Server and model texts may echo the request: the key and auth tokens are masked, the text is cut. */
    private String safeText(String text) {
        String masked = apiKey != null ? text.replace(apiKey, "***") : text;
        masked = AUTH_TOKEN.matcher(masked).replaceAll("$1 ***");
        return masked.length() <= MAX_TEXT_IN_MESSAGE ? masked : masked.substring(0, MAX_TEXT_IN_MESSAGE) + "…";
    }

    private static void requirePositive(String property, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(property + " должен быть больше 0, задано: " + value);
        }
    }

    /** Surrounding whitespace (a trailing newline from a secret file) is dropped; the value is never echoed. */
    private static String normalizeApiKey(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String key = raw.strip();
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c < 0x20 || c >= 0x7F) {
                throw new IllegalArgumentException("tolmach.ai.api-key содержит недопустимый символ в позиции " + (i + 1)
                        + " (управляющий или не ASCII); значение в сообщение не выводится");
            }
        }
        return key;
    }

    private static URI parseBaseUri(String url, String displayUrl) {
        URI uri;
        try {
            uri = URI.create(url.endsWith("/") ? url.substring(0, url.length() - 1) : url);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("tolmach.ai.url не является URL: " + displayUrl);
        }
        if (uri.getHost() == null || !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
            throw new IllegalArgumentException("tolmach.ai.url должен быть http(s)-URL: " + displayUrl);
        }
        if (uri.getRawUserInfo() != null) {
            throw new IllegalArgumentException("tolmach.ai.url не должен содержать учетные данные (" + displayUrl
                    + "), ключ задается в tolmach.ai.api-key");
        }
        if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new IllegalArgumentException("tolmach.ai.url не должен содержать query и fragment (" + displayUrl
                    + "): к нему дописываются /chat/completions и /models");
        }
        return uri;
    }

    /** Collects the body as UTF-8 text and fails the exchange once it exceeds {@code maxBytes}. */
    private static final class LimitedStringSubscriber implements HttpResponse.BodySubscriber<String> {

        private final int maxBytes;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final CompletableFuture<String> result = new CompletableFuture<>();
        private Flow.Subscription subscription;

        LimitedStringSubscriber(int maxBytes) {
            this.maxBytes = maxBytes;
        }

        @Override
        public CompletionStage<String> getBody() {
            return result;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            if (result.isDone()) {
                return;
            }
            for (ByteBuffer item : items) {
                if (buffer.size() + item.remaining() > maxBytes) {
                    subscription.cancel();
                    result.completeExceptionally(new IOException("ответ больше " + maxBytes + " байт"));
                    return;
                }
                byte[] bytes = new byte[item.remaining()];
                item.get(bytes);
                buffer.write(bytes, 0, bytes.length);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            result.complete(buffer.toString(StandardCharsets.UTF_8));
        }
    }
}
