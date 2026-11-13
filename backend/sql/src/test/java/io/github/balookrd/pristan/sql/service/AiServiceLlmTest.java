package io.github.balookrd.pristan.sql.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.github.balookrd.pristan.sql.config.SqlProperties;
import io.github.balookrd.pristan.sql.dto.AICheckResponse;
import io.github.balookrd.pristan.sql.dto.AIFixResponse;
import io.github.balookrd.pristan.sql.dto.AIGenerateResponse;
import io.github.balookrd.pristan.sql.dto.AIStatusResponse;
import io.github.balookrd.pristan.sql.service.ai.OpenAiCompatibleClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AiServiceLlmTest {

    @RegisterExtension
    static WireMockExtension llm = WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

    private final ObjectMapper mapper = new ObjectMapper();

    private AiService service(String apiKey, int timeoutSeconds) {
        SqlProperties properties = new SqlProperties();
        properties.getAi().setProvider("openai_compatible");
        properties.getAi().setUrl(llm.baseUrl() + "/v1");
        properties.getAi().setModel("qwen-test");
        properties.getAi().setApiKey(apiKey);
        properties.getAi().setTimeoutSeconds(timeoutSeconds);
        return new AiService(properties, mapper);
    }

    private String chat(String content) throws Exception {
        return mapper.writeValueAsString(Map.of("choices",
                List.of(Map.of("index", 0, "message", Map.of("role", "assistant", "content", content)))));
    }

    @Test
    void generateUsesModelAnswer() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat(
                "{\"generated_sql\":\"SELECT 1\",\"explanation\":\"тест\",\"tables_used\":[\"t\"]}"))));

        AIGenerateResponse response = service("", 5).generateQuery("покажи что-нибудь", "trino", Map.of());

        assertEquals("SELECT 1", response.generatedSql());
        assertEquals(List.of("t"), response.tablesUsed());
        assertFalse(response.fallbackUsed());
        assertEquals("qwen-test", response.model());
        assertEquals("openai_compatible", response.provider());
        llm.verify(postRequestedFor(urlEqualTo("/v1/chat/completions"))
                .withRequestBody(matchingJsonPath("$.model", equalTo("qwen-test")))
                .withRequestBody(matchingJsonPath("$.temperature", equalTo("0"))));
    }

    @Test
    void generateFallsBackToRulesWhenModelFails() {
        llm.stubFor(post("/v1/chat/completions").willReturn(serverError()));

        AIGenerateResponse response = service("", 5).generateQuery("покажи топ пользователей", "trino", Map.of());

        assertTrue(response.fallbackUsed());
        assertEquals("mock-builtin", response.provider());
        assertEquals("builtin-rules", response.model());
        assertTrue(response.generatedSql().contains("analytics.events.user_actions"));
    }

    @Test
    void checkFallsBackOnNonJsonAnswer() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("Извините, не могу ответить"))));

        AICheckResponse response = service("", 5).checkQuery("SELECT * FROM orders", "trino", Map.of());

        assertTrue(response.fallbackUsed());
        assertEquals("avoid-select-star-without-limit", response.issues().get(0).rule());
    }

    @Test
    void checkAcceptsCodeFencedJson() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("""
                ```json
                {"is_valid": false, "summary": "Нет условия JOIN", "complexity_score": 4, "estimated_notes": [],
                 "issues": [{"line": 1, "column": 15, "end_line": null, "end_column": null, "severity": "error",
                             "category": "syntax", "message": "JOIN без ON", "rule": "join-missing-on", "suggestion": "ON a.id = b.id"}]}
                ```"""))));

        AICheckResponse response = service("", 5).checkQuery("SELECT * FROM a JOIN b", "trino", Map.of());

        assertFalse(response.fallbackUsed());
        assertFalse(response.valid());
        assertEquals(4, response.complexityScore());
        assertEquals("join-missing-on", response.issues().get(0).rule());
    }

    @Test
    void fixWithoutRequiredFieldFallsBack() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("{\"explanation\":\"без поля fixed_sql\"}"))));

        AIFixResponse response = service("", 5).fixQuery("SELEC 1", "trino", "syntax error");

        assertEquals("mock-builtin", response.provider());
        assertEquals("SELEC 1;", response.fixedSql());
    }

    @Test
    void apiKeyIsSentAsBearerToken() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("{\"fixed_sql\":\"SELECT 1\",\"explanation\":\"ok\"}"))));

        service("secret-key", 5).fixQuery("SELEC 1", "trino", "syntax error");

        llm.verify(postRequestedFor(urlEqualTo("/v1/chat/completions")).withHeader("Authorization", equalTo("Bearer secret-key")));
    }

    @Test
    void slowModelFallsBackAfterTimeout() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("{\"generated_sql\":\"SELECT 2\"}")).withFixedDelay(3000)));

        AIGenerateResponse response = service("", 1).generateQuery("заказы", "trino", Map.of());

        assertTrue(response.fallbackUsed());
    }

    @Test
    void stalledResponseBodyFallsBackWithinTimeout() throws Exception {
        // Headers and part of the body arrive at once, then the server stalls: the timeout must cover the whole answer
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            Thread stalling = Thread.ofVirtual().start(() -> {
                try (Socket socket = server.accept()) {
                    socket.getInputStream().read(new byte[8192]);
                    OutputStream out = socket.getOutputStream();
                    out.write(("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"choices\":")
                            .getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    Thread.sleep(10_000);
                } catch (Exception ignored) {
                    // the client closes the connection after its timeout
                }
            });
            SqlProperties properties = new SqlProperties();
            properties.getAi().setProvider("openai_compatible");
            properties.getAi().setUrl("http://127.0.0.1:" + server.getLocalPort() + "/v1");
            properties.getAi().setTimeoutSeconds(1);

            long start = System.nanoTime();
            AIGenerateResponse response = new AiService(properties, mapper).generateQuery("заказы", "trino", Map.of());
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertTrue(response.fallbackUsed());
            assertTrue(elapsedMs < 3000, "fallback took " + elapsedMs + " ms");
            stalling.interrupt();
        }
    }

    @Test
    void statusReflectsModelAvailability() {
        llm.stubFor(get("/v1/models").willReturn(okJson("{\"data\":[{\"id\":\"qwen-test\"}]}")));
        AIStatusResponse up = service("", 5).getStatus();
        assertTrue(up.available());
        assertEquals("qwen-test", up.model());

        llm.stubFor(get("/v1/models").willReturn(serverError()));
        AIStatusResponse down = service("", 5).getStatus();
        assertFalse(down.available());
        assertEquals("mock-builtin", down.provider());
        assertEquals("builtin-rules", down.model());
        assertTrue(down.message().contains("встроенными правилами"), down.message());
        assertTrue(down.message().contains("qwen-test"), down.message());
    }

    @Test
    void apiKeyIsNotExposedInStatus() {
        // Some servers echo the rejected credentials in the error body
        llm.stubFor(get("/v1/models").willReturn(aResponse().withStatus(401)
                .withBody("{\"error\":\"Incorrect API key provided: sk-live-123456, header Bearer sk-live-123456\"}")));

        AIStatusResponse status = service("sk-live-123456", 5).getStatus();

        assertFalse(status.available());
        assertTrue(status.message().contains("HTTP 401"), status.message());
        assertFalse(status.message().contains("sk-live-123456"), status.message());
    }

    @Test
    void credentialsInUrlAreRejectedWithoutBeingExposed() {
        SqlProperties properties = new SqlProperties();
        properties.getAi().setProvider("openai_compatible");
        properties.getAi().setUrl("http://user:pa55word@llm.example:11434/v1");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new AiService(properties, mapper));

        assertTrue(e.getMessage().contains("http://llm.example:11434/v1"), e.getMessage());
        assertFalse(e.getMessage().contains("pa55word"), e.getMessage());
    }

    @Test
    void rulesModeStatusHidesCredentialsInUrl() {
        SqlProperties properties = new SqlProperties();
        properties.getAi().setUrl("http://user:pa55word@llm.example:11434/v1");

        AIStatusResponse status = new AiService(properties, mapper).getStatus();

        assertEquals("http://llm.example:11434/v1", status.baseUrl());
    }

    @Test
    void nonPositiveTimeoutIsRejectedAtStartup() {
        assertThrows(IllegalArgumentException.class, () -> service("", 0));
    }

    @Test
    void apiKeyWithSurroundingWhitespaceIsStripped() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("{\"fixed_sql\":\"SELECT 1\",\"explanation\":\"ok\"}"))));

        AIFixResponse response = service("k\n", 5).fixQuery("SELEC 1", "trino", "syntax error");

        assertEquals("openai_compatible", response.provider());
        llm.verify(postRequestedFor(urlEqualTo("/v1/chat/completions")).withHeader("Authorization", equalTo("Bearer k")));
    }

    @Test
    void apiKeyWithControlCharacterIsRejectedWithoutValue() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service("secret\u0007value", 5));

        assertTrue(e.getMessage().contains("sql.ai.api-key"), e.getMessage());
        assertFalse(e.getMessage().contains("secret"), e.getMessage());
        assertFalse(e.getMessage().contains("value"), e.getMessage());
    }

    @Test
    void urlWithQueryIsRejected() {
        SqlProperties properties = new SqlProperties();
        properties.getAi().setProvider("openai_compatible");
        properties.getAi().setUrl("http://llm.example:11434/v1?token=abc");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new AiService(properties, mapper));

        assertTrue(e.getMessage().contains("query"), e.getMessage());
        assertFalse(e.getMessage().contains("abc"), e.getMessage());
    }

    @Test
    void statusUsesShortTimeout() {
        llm.stubFor(get("/v1/models").willReturn(okJson("{\"data\":[]}").withFixedDelay(3000)));
        SqlProperties properties = new SqlProperties();
        properties.getAi().setProvider("openai_compatible");
        properties.getAi().setUrl(llm.baseUrl() + "/v1");
        properties.getAi().setTimeoutSeconds(30);
        properties.getAi().setStatusTimeoutSeconds(1);

        long start = System.nanoTime();
        AIStatusResponse status = new AiService(properties, mapper).getStatus();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertFalse(status.available());
        assertTrue(status.message().contains("таймаут 1 с"), status.message());
        assertTrue(elapsedMs < 2500, "status took " + elapsedMs + " ms");
    }

    @Test
    void statusMessageCarriesOnlyTheReason() {
        llm.stubFor(get("/v1/models").willReturn(aResponse().withStatus(500).withBody("stack trace at internal.Host")));

        AIStatusResponse status = service("", 5).getStatus();

        assertTrue(status.message().contains("HTTP 500"), status.message());
        assertFalse(status.message().contains("internal.Host"), status.message());
    }

    @Test
    void callsAboveConcurrencyLimitFallBackAtOnce() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("{\"generated_sql\":\"SELECT 2\"}")).withFixedDelay(2000)));
        SqlProperties properties = new SqlProperties();
        properties.getAi().setProvider("openai_compatible");
        properties.getAi().setUrl(llm.baseUrl() + "/v1");
        properties.getAi().setMaxConcurrentRequests(1);
        AiService service = new AiService(properties, mapper);

        Thread busy = Thread.ofVirtual().start(() -> service.generateQuery("заказы", "trino", Map.of()));
        long waitUntil = System.nanoTime() + 2_000_000_000L;
        while (llm.getAllServeEvents().isEmpty() && System.nanoTime() < waitUntil) {
            Thread.onSpinWait();
        }
        long start = System.nanoTime();
        AIGenerateResponse second = service.generateQuery("заказы", "trino", Map.of());
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        busy.join();

        assertTrue(second.fallbackUsed());
        assertTrue(elapsedMs < 1000, "second call took " + elapsedMs + " ms");
    }

    @Test
    void oversizedAnswerFallsBack() throws Exception {
        String huge = "x".repeat(OpenAiCompatibleClient.MAX_RESPONSE_BYTES + 1);
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat(huge))));

        AIGenerateResponse response = service("", 5).generateQuery("заказы", "trino", Map.of());

        assertTrue(response.fallbackUsed());
    }

    @Test
    void issuePositionsAndSeverityAreNormalized() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("""
                {"is_valid": true, "summary": "", "complexity_score": 99, "estimated_notes": [], "issues": [
                 {"line": 0, "column": -5, "end_line": 99999999, "end_column": null, "severity": "CRITICAL",
                  "category": "<b>x</b>", "message": "m", "rule": "r", "suggestion": null},
                 {"line": 3, "column": 2, "severity": " ERROR ", "category": "Security", "message": "m", "rule": "r"}]}"""))));

        AICheckResponse response = service("", 5).checkQuery("SELECT 1", "trino", Map.of());

        assertFalse(response.fallbackUsed());
        assertEquals(10, response.complexityScore());
        assertEquals(1, response.issues().get(0).line());
        assertEquals(1, response.issues().get(0).column());
        assertEquals(1_000_000, response.issues().get(0).endLine());
        assertEquals("warning", response.issues().get(0).severity());
        assertEquals("style", response.issues().get(0).category());
        assertEquals("error", response.issues().get(1).severity());
        assertEquals("security", response.issues().get(1).category());
    }

    @Test
    void fixWithoutErrorMessageDoesNotFail() throws Exception {
        AIFixResponse rules = new AiService(new SqlProperties(), mapper).fixQuery("SELEC 1", "trino", null);
        assertEquals("SELEC 1", rules.fixedSql());

        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("{\"fixed_sql\":\"SELECT 1\"}"))));
        AIFixResponse fixed = service("", 5).fixQuery("SELEC 1", "trino", null);
        assertEquals("SELECT 1", fixed.fixedSql());
        llm.verify(postRequestedFor(urlEqualTo("/v1/chat/completions")).withRequestBody(notContaining("Ошибка движка")));
    }

    @Test
    void llmAnswersReportCanonicalProvider() throws Exception {
        llm.stubFor(post("/v1/chat/completions").willReturn(okJson(chat("{\"fixed_sql\":\"SELECT 1\"}"))));
        SqlProperties properties = new SqlProperties();
        properties.getAi().setProvider("OpenAI_Compatible");
        properties.getAi().setUrl(llm.baseUrl() + "/v1");

        AIFixResponse response = new AiService(properties, mapper).fixQuery("SELEC 1", "trino", "x");

        assertEquals("openai_compatible", response.provider());
    }

    @Test
    void rulesProviderReportsUnavailableModel() {
        AIStatusResponse status = new AiService(new SqlProperties(), mapper).getStatus();

        assertFalse(status.available());
        assertEquals("mock-builtin", status.provider());
        assertEquals("builtin-rules", status.model());
    }
}
