package io.github.balookrd.pristan.spark.service.engine.livy;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.github.balookrd.pristan.spark.config.SparkProperties;
import io.github.balookrd.pristan.spark.dto.ColumnMetadata;
import io.github.balookrd.pristan.spark.service.engine.SparkEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LivySparkEngineTest {

    @RegisterExtension
    static WireMockExtension livy = WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

    private static final String SQL_OUTPUT = """
            {"id":0,"state":"available","progress":1.0,"output":{"status":"ok","execution_count":0,"data":{
              "application/json":{"schema":{"type":"struct","fields":[{"name":"namespace","type":"string","nullable":false,"metadata":{}}]},
              "data":[["default"],["analytics"]]}}}}""";

    private LivySparkEngine engine;

    @BeforeEach
    void setUp() {
        SparkProperties.SparkClusterConfig cluster = LivySessionPayloadBuilderTest.cluster();
        cluster.setLivyUrl(livy.baseUrl());
        cluster.getLivy().setPollIntervalMs(20);
        cluster.getLivy().setSessionStartTimeoutSeconds(5);
        LivyClient client = new LivyClient(livy.baseUrl(), LivyAuthHeaderProvider.NONE, Duration.ofSeconds(2), Duration.ofSeconds(5));
        engine = new LivySparkEngine(cluster, client, new LivySessionPayloadBuilder(new SparkProperties()));
    }

    private static String session(int id, String state) {
        // name and proxyUser as LivySessionPayloadBuilder sends them for the user "analyst"
        return "{\"id\":" + id + ",\"name\":\"hexp-analyst-0a1b2c3d\",\"proxyUser\":\"analyst\","
                + "\"appId\":\"application_1_0007\",\"state\":\"" + state + "\"}";
    }

    @Test
    void createSessionPostsPayloadWithCsrfHeader() {
        livy.stubFor(post("/sessions").willReturn(okJson("{\"id\":7,\"appId\":null,\"state\":\"starting\"}")));

        SparkEngine.SessionHandle handle = engine.createSession("analyst",
                LivySessionPayloadBuilderTest.request("pyspark", Map.of("spark.sql.shuffle.partitions", "8")));

        assertEquals(7, handle.livySessionId());
        assertEquals("starting", handle.state());
        livy.verify(postRequestedFor(urlEqualTo("/sessions"))
                .withHeader("X-Requested-By", equalTo("pristan"))
                .withRequestBody(matchingJsonPath("$.proxyUser", equalTo("analyst")))
                .withRequestBody(matchingJsonPath("$.queue", equalTo("root.analytics")))
                .withRequestBody(matchingJsonPath("$.conf['spark.sql.shuffle.partitions']", equalTo("8"))));
    }

    @Test
    void listSessionsReadsOwnerFieldsAndCompleteness() {
        livy.stubFor(get(urlPathEqualTo("/sessions")).withQueryParam("from", equalTo("0"))
                .willReturn(okJson("{\"from\":0,\"total\":3,\"sessions\":["
                        + "{\"id\":1,\"name\":\"hexp-analyst-0a1b2c3d\",\"proxyUser\":\"analyst\",\"appId\":null,\"state\":\"idle\"},"
                        + "{\"id\":2,\"name\":null,\"proxyUser\":null,\"appId\":\"application_1_0002\",\"state\":\"shutting_down\"}]}")));

        SparkEngine.SessionListing listing = engine.listSessions();

        assertEquals(2, listing.sessions().size());
        assertFalse(listing.complete(), "total 3 > 2 returned: the listing is one page only");
        assertEquals("analyst", listing.sessions().get(0).proxyUser());
        assertEquals("hexp-analyst-0a1b2c3d", listing.sessions().get(0).name());
        assertEquals("dead", listing.sessions().get(1).state());
        assertEquals(null, listing.sessions().get(1).proxyUser());
    }

    @Test
    void foreignSessionIsRecognisedByProxyUserOrName() {
        assertFalse(LivySparkEngine.isForeign("analyst", "hexp-analyst-0a1b2c3d", "analyst", true));
        assertTrue(LivySparkEngine.isForeign(null, null, "analyst", true), "our sessions always have a name");
        assertTrue(LivySparkEngine.isForeign(null, null, "analyst", false));
        assertTrue(LivySparkEngine.isForeign(null, "hexp-analyst-0a1b2c3d", "analyst", true),
                "proxy-user: true and no proxyUser: Livy ignored impersonation");
        assertFalse(LivySparkEngine.isForeign(null, "hexp-analyst-0a1b2c3d", "analyst", false), "proxy-user: false");
        assertTrue(LivySparkEngine.isForeign("other", "hexp-analyst-0a1b2c3d", "analyst", false));
        assertTrue(LivySparkEngine.isForeign("analyst", "hexp-other-0a1b2c3d", "analyst", true));
        assertTrue(LivySparkEngine.isForeign("analyst", "hexp-analyst2-0a1b2c3d", "analyst", true), "prefix includes the dash");
        assertTrue(LivySparkEngine.isForeign("analyst", "notebook", "analyst", true));
    }

    @Test
    void statementIsNotSubmittedToSessionWithoutNameAndProxyUser() {
        livy.stubFor(get("/sessions/7").willReturn(okJson("{\"id\":7,\"appId\":null,\"state\":\"idle\"}")));

        LivyException e = assertThrows(LivyException.class, () -> engine.execute(7, "e1", "print(1)", "pyspark", "analyst",
                event -> { }, new java.util.concurrent.atomic.AtomicBoolean()));

        assertEquals(409, e.getStatus());
        livy.verify(0, postRequestedFor(urlEqualTo("/sessions/7/statements")));
    }

    @Test
    void statementIsNotSubmittedToSessionOfAnotherUser() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(
                "{\"id\":7,\"name\":\"hexp-other-0a1b2c3d\",\"proxyUser\":\"other\",\"appId\":null,\"state\":\"idle\"}")));

        LivyException e = assertThrows(LivyException.class, () -> engine.execute(7, "e1", "print(1)", "pyspark", "analyst",
                event -> { }, new java.util.concurrent.atomic.AtomicBoolean()));

        assertEquals(409, e.getStatus());
        livy.verify(0, postRequestedFor(urlEqualTo("/sessions/7/statements")));
    }

    @Test
    void deniedConfIsRejectedBeforeCallingLivy() {
        assertThrows(InvalidSessionRequestException.class, () -> engine.createSession("analyst",
                LivySessionPayloadBuilderTest.request("pyspark", Map.of("spark.master", "local[*]"))));
        livy.verify(0, postRequestedFor(urlEqualTo("/sessions")));
    }

    @Test
    void executeWaitsForIdleThenReturnsSqlTable() {
        livy.stubFor(get("/sessions/7").inScenario("start").whenScenarioStateIs(STARTED)
                .willReturn(okJson(session(7, "starting"))).willSetStateTo("ready"));
        livy.stubFor(get("/sessions/7").inScenario("start").whenScenarioStateIs("ready")
                .willReturn(okJson(session(7, "idle"))));
        livy.stubFor(post("/sessions/7/statements")
                .willReturn(okJson("{\"id\":0,\"state\":\"waiting\",\"progress\":0.0,\"output\":null}")));
        livy.stubFor(get("/sessions/7/statements/0").inScenario("stmt").whenScenarioStateIs(STARTED)
                .willReturn(okJson("{\"id\":0,\"state\":\"running\",\"progress\":0.5,\"output\":null}")).willSetStateTo("done"));
        livy.stubFor(get("/sessions/7/statements/0").inScenario("stmt").whenScenarioStateIs("done")
                .willReturn(okJson(SQL_OUTPUT)));
        List<Map<String, Object>> events = new CopyOnWriteArrayList<>();

        SparkEngine.ExecutionResult result = engine.execute(7, "exec-1", "SHOW DATABASES", "sql", "analyst",
                events::add, new AtomicBoolean());

        assertEquals("FINISHED", result.status());
        assertEquals(List.of(new ColumnMetadata("namespace", "string")), result.columns());
        assertEquals(List.of(List.of("default"), List.of("analytics")), result.rows());
        livy.verify(postRequestedFor(urlEqualTo("/sessions/7/statements"))
                .withRequestBody(matchingJsonPath("$.kind", equalTo("sql")))
                .withRequestBody(matchingJsonPath("$.code", equalTo("SHOW DATABASES"))));
        // The terminal event is the manager's job after it records the outcome; the engine sends progress only
        assertTrue(events.stream().noneMatch(e -> "finished".equals(e.get("type"))), events::toString);
        assertTrue(events.stream().anyMatch(e -> Double.valueOf(0.5).equals(e.get("progress"))), events::toString);
    }

    @Test
    void scalaLanguageIsSentAsSparkKind() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "idle"))));
        livy.stubFor(post("/sessions/7/statements").willReturn(okJson("{\"id\":1,\"state\":\"waiting\",\"progress\":0.0}")));
        livy.stubFor(get("/sessions/7/statements/1").willReturn(okJson(
                "{\"id\":1,\"state\":\"available\",\"progress\":1.0,\"output\":{\"status\":\"ok\",\"data\":{\"text/plain\":\"res0: Int = 2\"}}}")));

        SparkEngine.ExecutionResult result = engine.execute(7, "exec-2", "1 + 1", "scalaspark", "analyst", e -> { }, new AtomicBoolean());

        assertEquals("res0: Int = 2", result.logs());
        livy.verify(postRequestedFor(urlEqualTo("/sessions/7/statements")).withRequestBody(matchingJsonPath("$.kind", equalTo("spark"))));
    }

    @Test
    void userCodeErrorIsFailedResult() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "idle"))));
        livy.stubFor(post("/sessions/7/statements").willReturn(okJson("{\"id\":2,\"state\":\"waiting\",\"progress\":0.0}")));
        livy.stubFor(get("/sessions/7/statements/2").willReturn(okJson(
                "{\"id\":2,\"state\":\"available\",\"progress\":1.0,\"output\":{\"status\":\"error\",\"ename\":\"NameError\","
                        + "\"evalue\":\"name 'x' is not defined\",\"traceback\":[]}}")));

        SparkEngine.ExecutionResult result = engine.execute(7, "exec-3", "x", "pyspark", "analyst", e -> { }, new AtomicBoolean());

        assertEquals("FAILED", result.status());
        assertEquals("NameError: name 'x' is not defined", result.errorMessage());
    }

    @Test
    void cancelSendsCancelToLivy() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "idle"))));
        livy.stubFor(post("/sessions/7/statements").willReturn(okJson("{\"id\":3,\"state\":\"waiting\",\"progress\":0.0}")));
        livy.stubFor(get("/sessions/7/statements/3").willReturn(okJson("{\"id\":3,\"state\":\"running\",\"progress\":0.1}")));
        livy.stubFor(post("/sessions/7/statements/3/cancel").willReturn(okJson("{\"msg\":\"canceled\"}")));
        AtomicBoolean cancel = new AtomicBoolean();

        // Cancel as soon as the statement reports progress
        SparkEngine.ExecutionResult result = engine.execute(7, "exec-4", "spark.range(10**12).count()", "pyspark", "analyst",
                e -> { if (String.valueOf(e.get("message")).contains("statement #3")) cancel.set(true); }, cancel);

        assertEquals("CANCELLED", result.status());
        livy.verify(postRequestedFor(urlEqualTo("/sessions/7/statements/3/cancel")));
    }

    @Test
    void cancelledAndFailedResultsSendNoFinishedEvent() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "idle"))));
        livy.stubFor(post("/sessions/7/statements").willReturn(okJson("{\"id\":2,\"state\":\"waiting\",\"progress\":0.0}")));
        livy.stubFor(get("/sessions/7/statements/2").willReturn(okJson(
                "{\"id\":2,\"state\":\"available\",\"progress\":1.0,\"output\":{\"status\":\"error\",\"ename\":\"E\",\"evalue\":\"x\"}}")));
        List<Map<String, Object>> events = new CopyOnWriteArrayList<>();
        AtomicBoolean cancelled = new AtomicBoolean(true);

        assertEquals("FAILED", engine.execute(7, "exec-7", "x", "pyspark", "analyst", events::add, new AtomicBoolean()).status());
        assertEquals("CANCELLED", engine.execute(7, "exec-8", "x", "pyspark", "analyst", events::add, cancelled).status());
        assertTrue(events.stream().noneMatch(e -> "finished".equals(e.get("type"))), events::toString);
    }

    @Test
    void interruptCancelsStatementWithInterruptFlagClearedThenRestoresIt() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "idle"))));
        livy.stubFor(post("/sessions/7/statements").willReturn(okJson("{\"id\":5,\"state\":\"waiting\",\"progress\":0.0}")));
        livy.stubFor(get("/sessions/7/statements/5").willReturn(okJson("{\"id\":5,\"state\":\"running\",\"progress\":0.1}")));
        livy.stubFor(post("/sessions/7/statements/5/cancel").willReturn(okJson("{\"msg\":\"canceled\"}")));

        // The event is delivered on the engine thread right before the poll pause, so the pause is interrupted
        SparkEngine.ExecutionResult result;
        boolean interruptedAfter;
        try {
            result = engine.execute(7, "exec-9", "x", "pyspark", "analyst",
                    e -> Thread.currentThread().interrupt(), new AtomicBoolean());
        } finally {
            interruptedAfter = Thread.interrupted(); // also clears the flag for the next tests
        }

        assertEquals("CANCELLED", result.status());
        // With the flag still set the JDK HttpClient would refuse to send the cancel
        livy.verify(postRequestedFor(urlEqualTo("/sessions/7/statements/5/cancel")));
        assertTrue(interruptedAfter, "the interrupt flag must be restored for the caller");
    }

    @Test
    void emptySuccessfulResponseIsLivyException() {
        livy.stubFor(get("/sessions/7").willReturn(aResponse().withStatus(200)));
        LivyException e = assertThrows(LivyException.class, () -> engine.refreshSession(7));
        assertEquals(502, e.getStatus());
    }

    @Test
    void unknownHttpStatusKeepsItsCode() {
        livy.stubFor(get("/sessions/7").willReturn(aResponse().withStatus(999).withBody("odd proxy")));
        LivyException e = assertThrows(LivyException.class, () -> engine.refreshSession(7));
        assertEquals(999, e.getStatus());
        assertTrue(e.getMessage().contains("HTTP 999"), e.getMessage());
    }

    @Test
    void decimalValuesKeepTheirPrecision() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "idle"))));
        livy.stubFor(post("/sessions/7/statements").willReturn(okJson("{\"id\":6,\"state\":\"waiting\",\"progress\":0.0}")));
        livy.stubFor(get("/sessions/7/statements/6").willReturn(okJson("""
                {"id":6,"state":"available","progress":1.0,"output":{"status":"ok","data":{"application/json":{
                  "schema":{"type":"struct","fields":[{"name":"amount","type":"decimal(38,18)","nullable":true,"metadata":{}}]},
                  "data":[[12345678901234567890.123456789012345678]]}}}}""")));

        SparkEngine.ExecutionResult result = engine.execute(7, "exec-10", "SELECT amount FROM t", "sql", "analyst", e -> { }, new AtomicBoolean());

        assertEquals("12345678901234567890.123456789012345678", result.rows().get(0).get(0));
    }

    @Test
    void deadSessionFailsExecution() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "dead"))));

        LivyException e = assertThrows(LivyException.class,
                () -> engine.execute(7, "exec-5", "1", "pyspark", "analyst", ev -> { }, new AtomicBoolean()));
        assertEquals(409, e.getStatus());
    }

    @Test
    void refreshMapsLivyStates() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "shutting_down"))));
        SparkEngine.SessionHandle handle = engine.refreshSession(7);
        assertEquals("dead", handle.state());
        assertEquals("application_1_0007", handle.appId());
    }

    @Test
    void deleteIgnoresMissingSession() {
        livy.stubFor(delete("/sessions/7").willReturn(notFound()));
        engine.deleteSession(7);
        livy.verify(deleteRequestedFor(urlEqualTo("/sessions/7")));
    }

    @Test
    void unreachableLivyIsReportedAs503() {
        LivyClient client = new LivyClient("http://127.0.0.1:1", LivyAuthHeaderProvider.NONE, Duration.ofSeconds(1), Duration.ofSeconds(1));
        LivyException e = assertThrows(LivyException.class, () -> client.getSession(1));
        assertEquals(503, e.getStatus());
    }

    @Test
    void hangingLivyIsCutByReadTimeout() {
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "idle")).withFixedDelay(3_000)));
        LivyClient client = new LivyClient(livy.baseUrl(), LivyAuthHeaderProvider.NONE, Duration.ofSeconds(1), Duration.ofMillis(300));

        long start = System.nanoTime();
        LivyException e = assertThrows(LivyException.class, () -> client.getSession(7));

        assertEquals(504, e.getStatus(), () -> e.getMessage() + " / cause chain: " + causeChain(e));
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(2)) < 0,
                "the request waited for the full response delay");
    }

    @Test
    void bodyStalledAfterHeadersIsCutByReadTimeout() {
        // Headers arrive at once, the body dribbles for 5 s: the timeout covers the whole answer, not only headers
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "idle")).withChunkedDribbleDelay(5, 5_000)));
        LivyClient client = new LivyClient(livy.baseUrl(), LivyAuthHeaderProvider.NONE, Duration.ofSeconds(1), Duration.ofMillis(300));

        long start = System.nanoTime();
        LivyException e = assertThrows(LivyException.class, () -> client.getSession(7));

        assertEquals(504, e.getStatus(), () -> e.getMessage() + " / cause chain: " + causeChain(e));
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(2)) < 0,
                "the request waited for the whole body");
    }

    @Test
    void bodyStalledAfterFlushedHeadersIsCutByReadTimeout() throws Exception {
        // WireMock may hold the headers until the first dribbled chunk; a raw socket flushes them for sure
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            Thread stalling = Thread.ofVirtual().start(() -> {
                try (Socket socket = server.accept()) {
                    socket.getInputStream().read(new byte[8192]);
                    OutputStream out = socket.getOutputStream();
                    out.write(("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"id\":")
                            .getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    Thread.sleep(10_000);
                } catch (Exception ignored) {
                    // the client closes the connection after its timeout
                }
            });
            LivyClient client = new LivyClient("http://127.0.0.1:" + server.getLocalPort(), LivyAuthHeaderProvider.NONE,
                    Duration.ofSeconds(1), Duration.ofMillis(300));

            long start = System.nanoTime();
            LivyException e = assertThrows(LivyException.class, () -> client.getSession(7));
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertEquals(504, e.getStatus(), () -> e.getMessage() + " / cause chain: " + causeChain(e));
            assertTrue(elapsedMs < 2_000, "the request waited " + elapsedMs + " ms for the body");
            stalling.interrupt();
        }
    }

    @Test
    void oversizedResponseIsRejected() {
        String huge = "{\"id\":7,\"state\":\"idle\",\"log\":\"" + "x".repeat(LivyClient.MAX_RESPONSE_BYTES) + "\"}";
        livy.stubFor(get("/sessions/7").willReturn(okJson(huge)));
        LivyClient client = new LivyClient(livy.baseUrl(), LivyAuthHeaderProvider.NONE, Duration.ofSeconds(1), Duration.ofSeconds(5));

        LivyException e = assertThrows(LivyException.class, () -> client.getSession(7));

        assertEquals(502, e.getStatus(), e::getMessage);
        assertTrue(e.getMessage().contains(String.valueOf(LivyClient.MAX_RESPONSE_BYTES)), e.getMessage());
    }

    @Test
    void redirectIsNotTreatedAsSuccess() {
        livy.stubFor(get("/sessions/7").willReturn(aResponse().withStatus(302).withHeader("Location", "/login")
                .withBody("{\"id\":7,\"state\":\"idle\"}")));

        LivyException e = assertThrows(LivyException.class, () -> engine.refreshSession(7));

        assertEquals(302, e.getStatus(), e::getMessage);
    }

    @Test
    void statementTimeoutCancelsStatementAndFails() {
        SparkProperties.SparkClusterConfig cluster = LivySessionPayloadBuilderTest.cluster();
        cluster.getLivy().setPollIntervalMs(20);
        cluster.getLivy().setStatementTimeoutSeconds(1);
        LivySparkEngine shortEngine = new LivySparkEngine(cluster,
                new LivyClient(livy.baseUrl(), LivyAuthHeaderProvider.NONE, Duration.ofSeconds(2), Duration.ofSeconds(5)),
                new LivySessionPayloadBuilder(new SparkProperties()));
        livy.stubFor(get("/sessions/7").willReturn(okJson(session(7, "idle"))));
        livy.stubFor(post("/sessions/7/statements").willReturn(okJson("{\"id\":4,\"state\":\"waiting\",\"progress\":0.0}")));
        livy.stubFor(get("/sessions/7/statements/4").willReturn(okJson("{\"id\":4,\"state\":\"running\",\"progress\":0.1}")));
        livy.stubFor(post("/sessions/7/statements/4/cancel").willReturn(serverError()));

        LivyException e = assertThrows(LivyException.class,
                () -> shortEngine.execute(7, "exec-6", "1", "pyspark", "analyst", ev -> { }, new AtomicBoolean()));

        // A failed cancel must not hide the timeout
        assertEquals(504, e.getStatus());
        livy.verify(postRequestedFor(urlEqualTo("/sessions/7/statements/4/cancel")));
    }

    @Test
    void authorizationHeaderIsSentButNeverReported() {
        String token = "Negotiate c2VjcmV0LXNwbmVnby10b2tlbg==";
        livy.stubFor(get("/sessions/7").withHeader("Authorization", equalTo(token))
                .willReturn(aResponse().withStatus(401).withBody("rejected credentials: " + token)));
        LivyClient client = new LivyClient(livy.baseUrl(), target -> Optional.of(token), Duration.ofSeconds(1), Duration.ofSeconds(2));

        LivyException e = assertThrows(LivyException.class, () -> client.getSession(7));

        assertEquals(401, e.getStatus());
        assertFalse(e.getMessage().contains("c2VjcmV0LXNwbmVnby10b2tlbg"), e.getMessage());
        livy.verify(getRequestedFor(urlEqualTo("/sessions/7")).withHeader("Authorization", equalTo(token)));
    }

    @Test
    void authenticationFailureReachesTheCallerUnchanged() {
        LivyException kerberos = new LivyException(401, "SPNEGO: не удалось получить билет для HTTP/127.0.0.1");
        LivyClient client = new LivyClient(livy.baseUrl(), target -> { throw kerberos; },
                Duration.ofSeconds(1), Duration.ofSeconds(2));

        LivyException e = assertThrows(LivyException.class, () -> client.getSession(7));

        // Not reported as "Livy unavailable" (503): the request never left the service
        assertSame(kerberos, e);
        livy.verify(0, getRequestedFor(urlEqualTo("/sessions/7")));
    }

    @Test
    void credentialsInLivyUrlAreNotReported() {
        LivyClient client = new LivyClient("http://svc:topsecret@127.0.0.1:1", LivyAuthHeaderProvider.NONE,
                Duration.ofSeconds(1), Duration.ofSeconds(1));

        LivyException e = assertThrows(LivyException.class, () -> client.getSession(1));

        assertFalse(e.getMessage().contains("topsecret"), e.getMessage());
        assertTrue(e.getMessage().contains("http://127.0.0.1:1"), e.getMessage());
    }

    private static String causeChain(Throwable e) {
        StringBuilder chain = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
            chain.append(t.getClass().getName()).append(": ").append(t.getMessage()).append(" <- ");
        }
        return chain.toString();
    }
}
