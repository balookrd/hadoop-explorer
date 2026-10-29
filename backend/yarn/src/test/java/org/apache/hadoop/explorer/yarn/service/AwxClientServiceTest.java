package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.explorer.yarn.config.YarnProperties;
import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.ConfigPatchOp;
import org.apache.hadoop.explorer.yarn.model.DeployResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("AwxClientService: результат деплоя берется из AWX, а не выдумывается")
class AwxClientServiceTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private HttpServer server;
    private YarnProperties props;
    private final AtomicReference<String> finalStatus = new AtomicReference<>("successful");
    private final AtomicInteger statusPolls = new AtomicInteger();
    private final AtomicReference<String> launchBody = new AtomicReference<>();
    private final ClusterConfig cluster = ClusterConfig.builder().id("prod-yarn").name("Prod").build();

    @BeforeEach
    void startAwx() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v2/job_templates/101/launch/", ex -> {
            launchBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(ex, 201, "{\"job\": 42, \"id\": 42}");
        });
        server.createContext("/api/v2/jobs/42/", ex -> {
            if (ex.getRequestURI().getPath().endsWith("/stdout/")) {
                respond(ex, 200, "TASK [Execute yarn rmadmin -refreshQueues] fatal: refresh failed");
                return;
            }
            // Two polls in "running" before the final status
            String status = statusPolls.incrementAndGet() < 3 ? "running" : finalStatus.get();
            respond(ex, 200, "{\"id\": 42, \"status\": \"" + status + "\"}");
        });
        server.start();

        props = new YarnProperties();
        props.getAwx().setBaseUrl("http://127.0.0.1:" + server.getAddress().getPort());
        props.getAwx().setToken("t");
        props.getAwx().setPollIntervalSeconds(1);
        props.getAwx().setTimeoutSeconds(30);
    }

    @AfterEach
    void stopAwx() {
        server.stop(0);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }

    private DeployResponse deploy(boolean wait) {
        return new AwxClientService(props, mapper).deployPatch(cluster, 7L,
                List.of(ConfigPatchOp.set("yarn.scheduler.capacity.root.dev.capacity", "20")), "admin", "c", wait);
    }

    @Test
    @DisplayName("Успех только после status=successful в AWX; патч передается в extra_vars")
    void successAfterPolling() throws Exception {
        DeployResponse r = deploy(true);

        assertEquals("SUCCESS", r.getStatus());
        assertEquals(42, r.getAwxJobId());
        assertTrue(statusPolls.get() >= 3, "job status must be polled until it finishes");

        JsonNode vars = mapper.readTree(launchBody.get()).path("extra_vars");
        String patch = new String(Base64.getDecoder().decode(vars.path("capacity_scheduler_patch_b64").asText()), StandardCharsets.UTF_8);
        assertTrue(patch.contains("root.dev.capacity"), patch);
        assertEquals("7", vars.path("change_request_id").asText());
        assertEquals("admin", vars.path("applied_by").asText());
        assertTrue(vars.path("capacity_scheduler_xml").isMissingNode(), "plain XML must not be duplicated in extra_vars");
    }

    @Test
    @DisplayName("Упавший Ansible-плейбук дает FAILED с выводом задачи")
    void failedJobIsReported() {
        finalStatus.set("failed");

        DeployResponse r = deploy(true);

        assertEquals("FAILED", r.getStatus());
        assertTrue(r.getStdout().contains("refresh failed"), r.getStdout());
    }

    @Test
    @DisplayName("wait=false возвращает RUNNING, а не SUCCESS")
    void noWaitIsRunning() {
        assertEquals("RUNNING", deploy(false).getStatus());
    }

    @Test
    @DisplayName("Отключенный или не настроенный AWX — FAILED, а не фейковый успех")
    void disabledAwxFails() {
        props.getAwx().setEnabled(false);
        DeployResponse r = deploy(true);
        assertEquals("FAILED", r.getStatus());
        assertNull(r.getAwxJobId());

        props.getAwx().setEnabled(true);
        props.getAwx().setBaseUrl("");
        assertEquals("FAILED", deploy(true).getStatus());
    }

    @Test
    @DisplayName("Симуляция только при yarn.awx.simulate=true и явно помечена")
    void simulationIsExplicit() {
        props.getAwx().setSimulate(true);
        DeployResponse r = deploy(true);
        assertEquals("SUCCESS", r.getStatus());
        assertTrue(r.getMessage().contains("СИМУЛЯЦИЯ"), r.getMessage());
        assertNull(launchBody.get(), "AWX must not be called in simulation");
    }
}
