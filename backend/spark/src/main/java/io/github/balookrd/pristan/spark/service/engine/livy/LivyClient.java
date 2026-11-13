package io.github.balookrd.pristan.spark.service.engine.livy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Apache Livy REST API: /sessions and /sessions/{id}/statements.
 *
 * <p>The JDK client is used directly instead of Spring {@code RestClient}: the read timeout of
 * {@code JdkClientHttpRequestFactory} (Spring 6.1) ends once the response headers arrive, so a Livy that stalls
 * in the middle of the body would hold the calling thread. Here {@code read-timeout} is a deadline for the whole
 * answer (headers and body), after which the exchange is cancelled; bodies above {@link #MAX_RESPONSE_BYTES}
 * are dropped.
 */
public class LivyClient {

    /** proxyUser and name as Livy reports them; null when Livy omits them. */
    public record LivySession(int id, String appId, String state, String proxyUser, String name) {}

    /** One page of GET /sessions; total counts all sessions Livy knows. */
    public record LivySessionList(int total, List<LivySession> sessions) {}

    /** Page size of GET /sessions: sessions beyond it make the listing incomplete. */
    static final int LIST_PAGE_SIZE = 1000;

    /** Larger answers are dropped: GET /sessions?size=1000 carries the last log lines of every session. */
    static final int MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

    public record LivyStatement(int id, String state, double progress, JsonNode output) {}

    // Livy answers with a JSON {"msg": ...}; anything longer is cut before it reaches logs and API responses
    private static final int MAX_BODY_IN_MESSAGE = 500;
    private static final Pattern AUTH_TOKEN = Pattern.compile("(?i)\\b(Negotiate|Bearer|Basic)\\s+[A-Za-z0-9+/=._~-]+");

    /** Livy URL for error messages: without user info, query and fragment. */
    private final String displayUrl;
    /** Base URL without a trailing slash; request paths are appended to it. */
    private final String baseUrl;
    private final LivyAuthHeaderProvider auth;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final HttpClient http;

    public LivyClient(String baseUrl, LivyAuthHeaderProvider auth, Duration connectTimeout, Duration readTimeout) {
        this.displayUrl = displayUrl(baseUrl);
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.auth = auth;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        // Redirects are not followed (HttpClient.Redirect.NEVER by default): a 3xx is reported as a Livy error
        this.http = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    public LivySession createSession(Map<String, Object> body) {
        String operation = "POST /sessions";
        return toSession(operation, call(operation, "POST", "/sessions", body));
    }

    public LivySession getSession(int sessionId) {
        String operation = "GET /sessions/" + sessionId;
        return toSession(operation, call(operation, "GET", "/sessions/" + sessionId, null));
    }

    public LivySessionList listSessions() {
        String operation = "GET /sessions";
        JsonNode node = parse(operation, call(operation, "GET", "/sessions?from=0&size=" + LIST_PAGE_SIZE, null));
        List<LivySession> sessions = new ArrayList<>();
        for (JsonNode session : node.path("sessions")) {
            sessions.add(toSession(session));
        }
        return new LivySessionList(node.path("total").asInt(sessions.size()), sessions);
    }

    /** Idempotent: a session Livy already forgot (404) counts as deleted. */
    public void deleteSession(int sessionId) {
        try {
            call("DELETE /sessions/" + sessionId, "DELETE", "/sessions/" + sessionId, null);
        } catch (LivyException e) {
            if (e.getStatus() != 404) {
                throw e;
            }
        }
    }

    public LivyStatement submitStatement(int sessionId, String code, String kind) {
        String operation = "POST /sessions/" + sessionId + "/statements";
        return toStatement(operation, call(operation, "POST", "/sessions/" + sessionId + "/statements",
                Map.of("code", code, "kind", kind)));
    }

    public LivyStatement getStatement(int sessionId, int statementId) {
        String operation = "GET /sessions/" + sessionId + "/statements/" + statementId;
        return toStatement(operation, call(operation, "GET", "/sessions/" + sessionId + "/statements/" + statementId, null));
    }

    public void cancelStatement(int sessionId, int statementId) {
        String path = "/sessions/" + sessionId + "/statements/" + statementId + "/cancel";
        call("POST " + path, "POST", path, null);
    }

    /**
     * Sends one request and returns the body of a 2xx answer. {@code jsonBody} is serialized as JSON; a POST
     * without it sends an empty body. Exceptions of the auth provider (e.g. a failed Kerberos login, 401) reach
     * the caller unchanged: the request never left the service.
     */
    private String call(String operation, String method, String path, Object jsonBody) {
        URI target;
        try {
            target = URI.create(baseUrl + path);
        } catch (IllegalArgumentException e) {
            throw new LivyException(503, "Livy " + displayUrl + " " + operation + ": некорректный livy-url", e);
        }
        String authorization = auth.authorizationHeader(target).orElse(null);
        HttpRequest request = buildRequest(operation, method, target, jsonBody, authorization);

        CompletableFuture<HttpResponse<String>> future = http.sendAsync(request,
                info -> new LimitedStringSubscriber(MAX_RESPONSE_BYTES));
        HttpResponse<String> response;
        try {
            response = future.get(readTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw readTimedOut(operation, e);
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new LivyException(503, "Livy " + displayUrl + " " + operation + ": ожидание ответа прервано", e);
        } catch (ExecutionException e) {
            throw failure(operation, e.getCause() != null ? e.getCause() : e);
        }
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            // Also a 3xx (redirects are not followed) or a code outside the known range from a proxy
            throw new LivyException(status, "Livy " + displayUrl + " " + operation + ": HTTP " + status
                    + responseBody(response.body()));
        }
        return response.body();
    }

    private HttpRequest buildRequest(String operation, String method, URI target, Object jsonBody, String authorization) {
        HttpRequest.BodyPublisher body;
        if (jsonBody == null) {
            body = HttpRequest.BodyPublishers.noBody();
        } else {
            try {
                body = HttpRequest.BodyPublishers.ofString(LivyOutputParser.MAPPER.writeValueAsString(jsonBody), StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                throw new LivyException(400, "Livy " + operation + ": тело запроса не сериализуется в JSON", e);
            }
        }
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(target)
                    // A second guard next to future.get(): the JDK client also drops the exchange on its own
                    .timeout(readTimeout)
                    .header("Accept", "application/json")
                    // Required when livy.server.csrf-protection.enabled = true, ignored otherwise
                    .header("X-Requested-By", "pristan")
                    .method(method, body);
            if (jsonBody != null) {
                builder.header("Content-Type", "application/json");
            }
            if (authorization != null) {
                builder.header("Authorization", authorization);
            }
            return builder.build();
        } catch (IllegalArgumentException e) {
            // Header validation: the message may contain the Authorization value, so only the type is reported
            throw new LivyException(503, "Livy " + displayUrl + " " + operation + ": запрос не сформирован ("
                    + e.getClass().getSimpleName() + ")");
        }
    }

    private LivyException failure(String operation, Throwable cause) {
        if (inChain(cause, HttpConnectTimeoutException.class)) {
            return new LivyException(504, "Livy " + displayUrl + ": соединение не установлено за "
                    + connectTimeout.toMillis() + " мс (" + operation + ", livy.connect-timeout-seconds)", cause);
        }
        if (inChain(cause, HttpTimeoutException.class)) {
            return readTimedOut(operation, cause);
        }
        if (inChain(cause, ResponseTooLargeException.class)) {
            return new LivyException(502, "Livy " + displayUrl + " " + operation + ": ответ больше "
                    + MAX_RESPONSE_BYTES + " байт", cause);
        }
        Throwable root = rootCause(cause);
        // The root cause message (e.g. "Connection refused") carries no request URI or credentials
        return new LivyException(503, "Livy " + displayUrl + " недоступен (" + operation + "): "
                + root.getClass().getSimpleName() + (root.getMessage() != null ? ": " + mask(root.getMessage()) : ""), cause);
    }

    private LivyException readTimedOut(String operation, Throwable cause) {
        return new LivyException(504, "Livy " + displayUrl + " не ответил за " + readTimeout.toMillis()
                + " мс (" + operation + ", livy.read-timeout-seconds)", cause);
    }

    private static boolean inChain(Throwable e, Class<? extends Throwable> type) {
        for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }

    private static String responseBody(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        String text = body.length() > MAX_BODY_IN_MESSAGE ? body.substring(0, MAX_BODY_IN_MESSAGE) + "..." : body;
        return " " + mask(text);
    }

    private static String mask(String text) {
        return AUTH_TOKEN.matcher(text).replaceAll("$1 ***");
    }

    private static Throwable rootCause(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }

    static String displayUrl(String baseUrl) {
        try {
            URI uri = URI.create(baseUrl);
            if (uri.getHost() == null) {
                return "<invalid livy-url>";
            }
            return uri.getScheme() + "://" + uri.getHost() + (uri.getPort() >= 0 ? ":" + uri.getPort() : "")
                    + (uri.getRawPath() != null ? uri.getRawPath() : "");
        } catch (IllegalArgumentException e) {
            return "<invalid livy-url>";
        }
    }

    private LivySession toSession(String operation, String body) {
        return toSession(parse(operation, body));
    }

    private static LivySession toSession(JsonNode node) {
        return new LivySession(node.path("id").asInt(), node.path("appId").asText(null), node.path("state").asText("unknown"),
                node.path("proxyUser").asText(null), node.path("name").asText(null));
    }

    private LivyStatement toStatement(String operation, String body) {
        JsonNode node = parse(operation, body);
        return new LivyStatement(node.path("id").asInt(), node.path("state").asText("unknown"),
                node.path("progress").asDouble(0.0), node.path("output"));
    }

    /** A 2xx answer without a JSON object (empty body, HTML from a proxy) is a Livy failure, not an NPE. */
    private JsonNode parse(String operation, String body) {
        if (body == null || body.isBlank()) {
            throw new LivyException(502, "Livy " + displayUrl + " " + operation + ": пустой ответ");
        }
        try {
            JsonNode node = LivyOutputParser.MAPPER.readTree(body);
            if (node == null || !node.isObject()) {
                throw new LivyException(502, "Livy " + displayUrl + " " + operation + ": ответ не является JSON-объектом");
            }
            return node;
        } catch (JsonProcessingException e) {
            throw new LivyException(502, "Livy " + displayUrl + " " + operation + ": ответ не является JSON"
                    + responseBody(body), e);
        }
    }

    /** Body above the limit: Livy answers are small JSON objects, a larger one is a misconfigured proxy or a bug. */
    private static final class ResponseTooLargeException extends IOException {
        ResponseTooLargeException(int maxBytes) {
            super("ответ больше " + maxBytes + " байт");
        }
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
                    result.completeExceptionally(new ResponseTooLargeException(maxBytes));
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
