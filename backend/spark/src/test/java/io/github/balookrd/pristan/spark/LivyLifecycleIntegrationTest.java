package io.github.balookrd.pristan.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.github.balookrd.pristan.common.auth.UserLoggedOutEvent;
import io.github.balookrd.pristan.spark.dto.CreateSessionRequest;
import io.github.balookrd.pristan.spark.model.SparkSessionRecord;
import io.github.balookrd.pristan.spark.repository.SparkSessionRepository;
import io.github.balookrd.pristan.spark.service.SparkSessionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

// WireMock.get/post/delete clash with the MockMvc builders: WireMock stubs are written fully qualified
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class LivyLifecycleIntegrationTest {

    // Started before the Spring context reads livy-url
    static final WireMockServer LIVY = new WireMockServer(wireMockConfig().dynamicPort());
    // Second cluster: the background sync of one cluster must not depend on the other
    static final WireMockServer LIVY2 = new WireMockServer(wireMockConfig().dynamicPort());

    static {
        LIVY.start();
        LIVY2.start();
    }

    // Spring Boot binds a list from one property source only: the cluster is described completely here
    @DynamicPropertySource
    static void livyCluster(DynamicPropertyRegistry registry) {
        registry.add("spark.clusters[0].id", () -> "livy-it");
        registry.add("spark.clusters[0].name", () -> "Livy IT");
        registry.add("spark.clusters[0].type", () -> "livy");
        registry.add("spark.clusters[0].livy-url", LIVY::baseUrl);
        registry.add("spark.clusters[0].mock-storage", () -> "false");
        registry.add("spark.clusters[0].livy.poll-interval-ms", () -> "20");
        registry.add("spark.clusters[0].resource-profiles[0].id", () -> "small");
        registry.add("spark.clusters[0].resource-profiles[0].name", () -> "Small");
        registry.add("spark.clusters[1].id", () -> "livy-it-2");
        registry.add("spark.clusters[1].name", () -> "Livy IT 2");
        registry.add("spark.clusters[1].type", () -> "livy");
        registry.add("spark.clusters[1].livy-url", LIVY2::baseUrl);
        registry.add("spark.clusters[1].mock-storage", () -> "false");
        registry.add("spark.clusters[1].livy.poll-interval-ms", () -> "20");
        registry.add("spark.clusters[1].resource-profiles[0].id", () -> "small");
        registry.add("spark.clusters[1].resource-profiles[0].name", () -> "Small");
        // The scheduled sync stays out of the way: tests run it explicitly with syncSessions()
        registry.add("spark.session-sync-interval-ms", () -> "3600000");
    }

    @AfterAll
    static void stopLivy() {
        LIVY.stop();
        LIVY2.stop();
    }

    @Autowired private MockMvc mockMvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private SparkSessionRepository sessionRepository;
    @Autowired private SparkSessionManager sessionManager;
    @Autowired private ApplicationEventPublisher eventPublisher;

    @BeforeEach
    void resetLivy() {
        LIVY.resetAll();
        LIVY2.resetAll();
        // A cluster paused by an earlier test (503) is synced again
        ((Map<?, ?>) ReflectionTestUtils.getField(sessionManager, "syncPausedUntil")).clear();
    }

    private String createSession(int livyId) throws Exception {
        return createSession(LIVY, "livy-it", livyId);
    }

    private String createSession(WireMockServer livy, String clusterId, int livyId) throws Exception {
        livy.stubFor(WireMock.post("/sessions").willReturn(okJson("{\"id\":" + livyId + ",\"appId\":null,\"state\":\"starting\"}")));
        CreateSessionRequest request = new CreateSessionRequest(clusterId, "spark-3.5.1", "hive-prod", "root.analytics",
                "small", "pyspark", null, null, null, List.of(), List.of(), List.of(), Map.of());
        String body = mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("starting")))
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).path("id").asText();
    }

    private static void stubLivySession(int livyId, String state, String user) {
        LIVY.stubFor(WireMock.get("/sessions/" + livyId).willReturn(okJson(livySession(livyId, state, user))));
    }

    /** GET /sessions of Livy: one JSON object per session. */
    private static void stubSessionList(WireMockServer livy, String... sessions) {
        livy.stubFor(WireMock.get(urlPathEqualTo("/sessions")).willReturn(okJson(
                "{\"from\":0,\"total\":" + sessions.length + ",\"sessions\":[" + String.join(",", sessions) + "]}")));
    }

    private static String livySession(int id, String state, String user) {
        return "{\"id\":" + id + ",\"name\":\"hexp-" + user + "-0a1b2c3d\",\"proxyUser\":\"" + user
                + "\",\"appId\":\"application_1_00" + id + "\",\"state\":\"" + state + "\"}";
    }

    private String storedStatus(String sessionId) {
        return sessionRepository.findById(sessionId).orElseThrow().getStatus();
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), "condition not reached in 5 s");
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void sessionStateAndAppIdComeFromLivy() throws Exception {
        String sessionId = createSession(11);
        stubSessionList(LIVY, livySession(11, "idle", "data_engineer"));

        sessionManager.syncSessions().get(5, TimeUnit.SECONDS);

        mockMvc.perform(get("/api/v1/sessions/" + sessionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("idle")))
                .andExpect(jsonPath("$.yarn_application_id", is("application_1_0011")));
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void stopDeletesLivySession() throws Exception {
        String sessionId = createSession(12);
        LIVY.stubFor(WireMock.delete("/sessions/12").willReturn(okJson("{\"msg\":\"deleted\"}")));

        mockMvc.perform(delete("/api/v1/sessions/" + sessionId)).andExpect(status().isOk());

        LIVY.verify(deleteRequestedFor(urlEqualTo("/sessions/12")));
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void idleTtlDeletesLivySession() throws Exception {
        String sessionId = createSession(13);
        LIVY.stubFor(WireMock.delete("/sessions/13").willReturn(okJson("{\"msg\":\"deleted\"}")));
        SparkSessionRecord record = sessionRepository.findById(sessionId).orElseThrow();
        record.setStatus("idle");
        record.setLastActivityAt(Instant.now().minus(Duration.ofHours(3)));
        sessionRepository.save(record);

        sessionManager.cleanupIdleSessions();

        assertEquals("killed", sessionRepository.findById(sessionId).orElseThrow().getStatus());
        await(() -> !LIVY.findAll(deleteRequestedFor(urlEqualTo("/sessions/13"))).isEmpty());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void idleTtlDoesNotWaitForSlowLivyDelete() throws Exception {
        String sessionId = createSession(27);
        LIVY.stubFor(WireMock.delete("/sessions/27").willReturn(okJson("{\"msg\":\"deleted\"}").withFixedDelay(3_000)));
        SparkSessionRecord record = sessionRepository.findById(sessionId).orElseThrow();
        record.setStatus("idle");
        record.setLastActivityAt(Instant.now().minus(Duration.ofHours(3)));
        sessionRepository.save(record);

        long start = System.nanoTime();
        sessionManager.cleanupIdleSessions();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // The scheduler thread is shared by all @Scheduled tasks: a slow Livy must not hold it
        assertTrue(elapsedMs < 2_000, "cleanup waited " + elapsedMs + " ms for DELETE");
        assertEquals("killed", storedStatus(sessionId));
        await(() -> !LIVY.findAll(deleteRequestedFor(urlEqualTo("/sessions/27"))).isEmpty());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void logoutClosesUserSessions() throws Exception {
        String sessionId = createSession(14);
        LIVY.stubFor(WireMock.delete("/sessions/14").willReturn(okJson("{\"msg\":\"deleted\"}")));

        eventPublisher.publishEvent(new UserLoggedOutEvent("data_engineer"));

        await(() -> "killed".equals(sessionRepository.findById(sessionId).orElseThrow().getStatus()));
        LIVY.verify(deleteRequestedFor(urlEqualTo("/sessions/14")));
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void deadLivySessionMarksRecordDead() throws Exception {
        String sessionId = createSession(16);
        // waitUntilReady sees a session that will never become idle and fails with LivyException(409)
        stubLivySession(16, "dead", "data_engineer");

        sessionManager.submitCode(sessionId, "print(1)", "pyspark", "data_engineer");

        await(() -> "dead".equals(sessionRepository.findById(sessionId).orElseThrow().getStatus()));
        // The record stays dead: a further statement is refused instead of another 409 from Livy
        assertTrue(sessionRepository.findById(sessionId).orElseThrow().getStoppedAt() != null);
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void stopDuringStatementIsNotRevertedToIdle() throws Exception {
        String sessionId = createSession(19);
        stubLivySession(19, "busy", "data_engineer");
        LIVY.stubFor(WireMock.post("/sessions/19/statements").willReturn(okJson("{\"id\":0,\"state\":\"running\",\"progress\":0.1}")));
        LIVY.stubFor(WireMock.get("/sessions/19/statements/0").willReturn(okJson("{\"id\":0,\"state\":\"running\",\"progress\":0.1}")));
        LIVY.stubFor(WireMock.delete("/sessions/19").willReturn(okJson("{\"msg\":\"deleted\"}")));

        String executionId = sessionManager.submitCode(sessionId, "spark.range(10).count()", "pyspark", "data_engineer");
        await(() -> !LIVY.findAll(postRequestedFor(urlEqualTo("/sessions/19/statements"))).isEmpty());

        mockMvc.perform(delete("/api/v1/sessions/" + sessionId)).andExpect(status().isOk());
        // The deleted session no longer knows the statement: the running task fails afterwards
        LIVY.stubFor(WireMock.get("/sessions/19/statements/0").willReturn(WireMock.notFound()));
        LIVY.stubFor(WireMock.get("/sessions/19").willReturn(WireMock.notFound()));

        await(() -> sessionManager.getResult(executionId, "data_engineer", false, 0, 10)
                .map(r -> "FAILED".equals(r.status())).orElse(false));
        assertEquals("killed", sessionRepository.findById(sessionId).orElseThrow().getStatus());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void sessionForgottenByLivyIsDead() throws Exception {
        String sessionId = createSession(17);
        // Livy restarted without recovery: the session is not in GET /sessions any more
        stubSessionList(LIVY);

        sessionManager.syncSessions().get(5, TimeUnit.SECONDS);

        mockMvc.perform(get("/api/v1/sessions/" + sessionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("dead")));
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void livyErrorKeepsStoredState() throws Exception {
        String sessionId = createSession(18);
        LIVY.stubFor(WireMock.get(urlPathEqualTo("/sessions")).willReturn(WireMock.serverError()));

        sessionManager.syncSessions().get(5, TimeUnit.SECONDS);

        mockMvc.perform(get("/api/v1/sessions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.id == '" + sessionId + "')].status", contains("starting")));
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void sessionApiReadsOnlyTheDatabase() throws Exception {
        String sessionId = createSession(20);
        LIVY.resetRequests();

        mockMvc.perform(get("/api/v1/sessions")).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/sessions/" + sessionId)).andExpect(status().isOk());

        assertEquals(0, LIVY.getAllServeEvents().size(), "the session API must not call Livy");
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void reusedLivyIdOfAnotherUserMarksSessionDeadWithoutDelete() throws Exception {
        String sessionId = createSession(21);
        // After a Livy restart id 21 belongs to somebody else's session
        stubSessionList(LIVY, livySession(21, "idle", "someone_else"));

        sessionManager.syncSessions().get(5, TimeUnit.SECONDS);

        assertEquals("dead", storedStatus(sessionId));
        assertTrue(LIVY.findAll(deleteRequestedFor(urlEqualTo("/sessions/21"))).isEmpty(),
                "another user's session must not be deleted");
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void statementInReusedLivySessionMarksSessionDead() throws Exception {
        String sessionId = createSession(22);
        LIVY.stubFor(WireMock.get("/sessions/22").willReturn(okJson(livySession(22, "idle", "someone_else"))));

        sessionManager.submitCode(sessionId, "print(1)", "pyspark", "data_engineer");

        await(() -> "dead".equals(storedStatus(sessionId)));
        assertTrue(LIVY.findAll(postRequestedFor(urlEqualTo("/sessions/22/statements"))).isEmpty(),
                "no code may run in another user's session");
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void sessionWithoutNameAndProxyUserIsForeign() throws Exception {
        String sessionId = createSession(27);
        // A session Livy created for someone else after a restart, without name and proxyUser
        LIVY.stubFor(WireMock.get("/sessions/27").willReturn(okJson("{\"id\":27,\"appId\":null,\"state\":\"idle\"}")));

        sessionManager.submitCode(sessionId, "print(1)", "pyspark", "data_engineer");

        await(() -> "dead".equals(storedStatus(sessionId)));
        assertTrue(LIVY.findAll(postRequestedFor(urlEqualTo("/sessions/27/statements"))).isEmpty());
        assertTrue(LIVY.findAll(deleteRequestedFor(urlEqualTo("/sessions/27"))).isEmpty());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void incompleteSessionListDoesNotKillMissingSessions() throws Exception {
        String sessionId = createSession(28);
        // Livy knows 1500 sessions, one page of 1000 does not contain id 28
        LIVY.stubFor(WireMock.get(urlPathEqualTo("/sessions")).willReturn(okJson(
                "{\"from\":0,\"total\":1500,\"sessions\":[" + livySession(900, "idle", "someone_else") + "]}")));

        sessionManager.syncSessions().get(5, TimeUnit.SECONDS);

        assertEquals("starting", storedStatus(sessionId));
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void unreachableClusterIsPausedAndOtherClusterStillSynced() throws Exception {
        String first = createSession(LIVY, "livy-it", 23);
        String second = createSession(LIVY2, "livy-it-2", 23);
        LIVY.stubFor(WireMock.get(urlPathEqualTo("/sessions")).willReturn(WireMock.aResponse().withStatus(503)));
        stubSessionList(LIVY2, livySession(23, "idle", "data_engineer"));

        sessionManager.syncSessions().get(5, TimeUnit.SECONDS);

        assertEquals("starting", storedStatus(first));
        assertEquals("idle", storedStatus(second));

        // Within spark.session-sync-pause-seconds the unreachable cluster is not asked again
        stubSessionList(LIVY2, livySession(23, "busy", "data_engineer"));
        sessionManager.syncSessions().get(5, TimeUnit.SECONDS);

        assertEquals(1, LIVY.findAll(WireMock.getRequestedFor(urlPathEqualTo("/sessions"))).size());
        assertEquals("busy", storedStatus(second));
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void stopOfFinishedSessionDoesNotCallLivy() throws Exception {
        String sessionId = createSession(24);
        SparkSessionRecord record = sessionRepository.findById(sessionId).orElseThrow();
        record.setStatus("dead");
        sessionRepository.save(record);

        mockMvc.perform(delete("/api/v1/sessions/" + sessionId)).andExpect(status().isOk());

        // The id may already belong to a new session after a Livy restart
        assertTrue(LIVY.findAll(deleteRequestedFor(urlEqualTo("/sessions/24"))).isEmpty());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void ttlSkipsSessionThatBecameActiveAgain() throws Exception {
        String sessionId = createSession(25);
        SparkSessionRecord record = sessionRepository.findById(sessionId).orElseThrow();
        record.setStatus("busy");
        record.setLastActivityAt(Instant.now().minus(Duration.ofHours(3)));
        sessionRepository.save(record);

        sessionManager.cleanupIdleSessions();

        assertEquals("busy", storedStatus(sessionId));
        assertTrue(LIVY.findAll(deleteRequestedFor(urlEqualTo("/sessions/25"))).isEmpty());
    }

    @Test
    @WithMockUser(username = "catalog_user", roles = {"USER"})
    void catalogRejectsInvalidIdentifiers() throws Exception {
        mockMvc.perform(get("/api/v1/catalog/livy-it/tables").param("database", "db;DROP TABLE x"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail", containsString("Недопустимое имя")));
        mockMvc.perform(get("/api/v1/catalog/livy-it/columns").param("database", "default").param("table", "t`x"))
                .andExpect(status().isBadRequest());
        assertTrue(LIVY.findAll(postRequestedFor(WireMock.urlMatching(".*/statements"))).isEmpty());
    }

    @Test
    @WithMockUser(username = "describe_user", roles = {"USER"})
    void describeStopsAtPartitionInformation() throws Exception {
        String sessionId = createSession(26);
        SparkSessionRecord record = sessionRepository.findById(sessionId).orElseThrow();
        record.setStatus("idle");
        sessionRepository.save(record);
        stubLivySession(26, "idle", "describe_user");
        LIVY.stubFor(WireMock.post("/sessions/26/statements").willReturn(okJson("{\"id\":0,\"state\":\"waiting\",\"progress\":0.0}")));
        LIVY.stubFor(WireMock.get("/sessions/26/statements/0").willReturn(okJson("""
                {"id":0,"state":"available","progress":1.0,"output":{"status":"ok","data":{"application/json":{
                  "schema":{"type":"struct","fields":[{"name":"col_name","type":"string","nullable":false,"metadata":{}},
                    {"name":"data_type","type":"string","nullable":true,"metadata":{}},
                    {"name":"comment","type":"string","nullable":true,"metadata":{}}]},
                  "data":[["id","bigint",null],["dt","string",null],["# Partition Information","",""],
                          ["# col_name","data_type","comment"],["dt","string",null]]}}}}""")));

        mockMvc.perform(get("/api/v1/catalog/livy-it/columns").param("database", "sales").param("table", "orders")
                        .param("refresh", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].name", contains("id", "dt")))
                .andExpect(jsonPath("$[*].type", contains("bigint", "string")));
        LIVY.verify(postRequestedFor(urlEqualTo("/sessions/26/statements"))
                .withRequestBody(matchingJsonPath("$.code", equalTo("DESCRIBE `sales`.`orders`"))));
    }

    @Test
    @WithMockUser(username = "catalog_user", roles = {"USER"})
    void catalogRunsShowDatabasesInUserSession() throws Exception {
        mockMvc.perform(get("/api/v1/catalog/livy-it/databases").param("refresh", "true"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.detail", containsString("запустите сессию Spark")));

        String sessionId = createSession(15);
        SparkSessionRecord record = sessionRepository.findById(sessionId).orElseThrow();
        record.setStatus("idle");
        sessionRepository.save(record);
        stubLivySession(15, "idle", "catalog_user");
        LIVY.stubFor(WireMock.post("/sessions/15/statements").willReturn(okJson("{\"id\":0,\"state\":\"waiting\",\"progress\":0.0}")));
        LIVY.stubFor(WireMock.get("/sessions/15/statements/0").willReturn(okJson("""
                {"id":0,"state":"available","progress":1.0,"output":{"status":"ok","data":{"application/json":{
                  "schema":{"type":"struct","fields":[{"name":"namespace","type":"string","nullable":false,"metadata":{}}]},
                  "data":[["default"],["analytics"]]}}}}""")));

        mockMvc.perform(get("/api/v1/catalog/livy-it/databases").param("refresh", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", contains("default", "analytics")));
        LIVY.verify(postRequestedFor(urlEqualTo("/sessions/15/statements"))
                .withRequestBody(matchingJsonPath("$.code", equalTo("SHOW DATABASES")))
                .withRequestBody(matchingJsonPath("$.kind", equalTo("sql"))));
    }
}
