package org.apache.hadoop.explorer.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.spark.dto.CreateSessionRequest;
import org.apache.hadoop.explorer.spark.dto.ExecuteCodeRequest;
import org.apache.hadoop.explorer.spark.service.engine.SparkEngine;
import org.apache.hadoop.explorer.spark.service.engine.SparkEngineRegistry;
import org.apache.hadoop.explorer.spark.service.engine.livy.InvalidSessionRequestException;
import org.apache.hadoop.explorer.spark.service.engine.livy.LivyException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class SparkExecutionFailureTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @SpyBean
    private SparkEngineRegistry engines;

    /** Engine with a scripted outcome: either throws or returns the given result. */
    private record ScriptedEngine(SparkEngine.ExecutionResult result, Throwable failure) implements SparkEngine {
        @Override
        public SessionHandle createSession(String username, CreateSessionRequest request) {
            return new SessionHandle(1, null, "idle");
        }

        @Override
        public SessionHandle refreshSession(int livySessionId) {
            return new SessionHandle(livySessionId, null, "idle");
        }

        @Override
        public void deleteSession(int livySessionId) {
        }

        @Override
        public ExecutionResult execute(int livySessionId, String executionId, String code, String language, String username,
                                       Consumer<Map<String, Object>> eventConsumer, AtomicBoolean cancelSignal) {
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            return result;
        }
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void engineFailureMarksExecutionFailed() throws Exception {
        doReturn(new ScriptedEngine(null, new IllegalStateException("Livy недоступен"))).when(engines).forCluster(any());

        String executionId = execute(createSession());

        JsonNode result = awaitTerminalResult(executionId);
        assertEquals("FAILED", result.path("status").asText());
        assertEquals("Livy недоступен", result.path("error_message").asText());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void engineErrorMarksExecutionFailedAndSessionIdle() throws Exception {
        doReturn(new ScriptedEngine(null, new NoClassDefFoundError("org/springframework/web/client/RestClient")))
                .when(engines).forCluster(any());
        String sessionId = createSession();

        JsonNode result = awaitTerminalResult(execute(sessionId));

        assertEquals("FAILED", result.path("status").asText());
        assertEquals("NoClassDefFoundError: org/springframework/web/client/RestClient", result.path("error_message").asText());
        String session = mockMvc.perform(get("/api/v1/sessions/" + sessionId)).andReturn().getResponse().getContentAsString();
        assertEquals("idle", objectMapper.readTree(session).path("status").asText());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void livyFailureOnSessionCreateIsBadGatewayWithDetail() throws Exception {
        SparkEngine engine = mock(SparkEngine.class);
        when(engine.createSession(any(), any())).thenThrow(
                new LivyException(503, "Livy http://livy.example:8998 недоступен (POST /sessions): ConnectException"));
        doReturn(engine).when(engines).forCluster(any());

        var response = mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sessionRequest(Map.of()))))
                .andExpect(status().isBadGateway())
                .andReturn().getResponse();

        JsonNode body = objectMapper.readTree(response.getContentAsString(StandardCharsets.UTF_8));
        assertEquals("Livy http://livy.example:8998 недоступен (POST /sessions): ConnectException", body.path("detail").asText());
        assertEquals(502, body.path("status").asInt());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void livyTimeoutOnSessionCreateIsGatewayTimeout() throws Exception {
        SparkEngine engine = mock(SparkEngine.class);
        when(engine.createSession(any(), any())).thenThrow(
                new LivyException(504, "Livy http://livy.example:8998 не ответил за 60000 мс (POST /sessions, livy.read-timeout-seconds)"));
        doReturn(engine).when(engines).forCluster(any());

        var response = mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sessionRequest(Map.of()))))
                .andExpect(status().isGatewayTimeout())
                .andReturn().getResponse();

        assertEquals(504, objectMapper.readTree(response.getContentAsString(StandardCharsets.UTF_8)).path("status").asInt());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void livyAuthFailureIsStillBadGatewayNotUnauthorized() throws Exception {
        // A 401 from Livy is the service's credential problem; passing it through would log the user out in the UI
        SparkEngine engine = mock(SparkEngine.class);
        when(engine.createSession(any(), any())).thenThrow(new LivyException(401, "Livy http://livy.example:8998 POST /sessions: HTTP 401"));
        doReturn(engine).when(engines).forCluster(any());

        mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sessionRequest(Map.of()))))
                .andExpect(status().isBadGateway());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void invalidSessionRequestIsBadRequestWithDetail() throws Exception {
        SparkEngine engine = mock(SparkEngine.class);
        when(engine.createSession(any(), any())).thenThrow(
                new InvalidSessionRequestException("Параметр spark_conf запрещен администратором: spark.master"));
        doReturn(engine).when(engines).forCluster(any());

        var response = mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sessionRequest(Map.of("spark.master", "local[*]")))))
                .andExpect(status().isBadRequest())
                .andReturn().getResponse();

        assertEquals("Параметр spark_conf запрещен администратором: spark.master",
                objectMapper.readTree(response.getContentAsString(StandardCharsets.UTF_8)).path("detail").asText());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void cancelledResultIsNotReportedAsFinished() throws Exception {
        SparkEngine.ExecutionResult cancelled = new SparkEngine.ExecutionResult(List.of(), List.of(), 0, "", 1.0, "CANCELLED", "Отменено");
        doReturn(new ScriptedEngine(cancelled, null)).when(engines).forCluster(any());

        String executionId = execute(createSession());

        assertEquals("CANCELLED", awaitTerminalResult(executionId).path("status").asText());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void executeInStoppedSessionIsRejectedWithoutReasonInBody() throws Exception {
        String sessionId = createSession();
        mockMvc.perform(delete("/api/v1/sessions/" + sessionId)).andExpect(status().isOk());

        var response = mockMvc.perform(post("/api/v1/statements/execute")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new ExecuteCodeRequest(sessionId, "spark.range(1)", "pyspark"))))
                .andExpect(status().isNotFound())
                .andReturn().getResponse();
        // ResponseStatusException goes to sendError: the reason is the servlet error message, not the body
        assertEquals("", response.getContentAsString(StandardCharsets.UTF_8));
        assertEquals("Сессия " + sessionId + " завершена (killed), запустите новую", response.getErrorMessage());
    }

    private static CreateSessionRequest sessionRequest(Map<String, String> sparkConf) {
        return new CreateSessionRequest("prod-analytics", "spark-3.5.1", "hive-prod",
                "root.analytics", "small", "pyspark", "py310", null, null, List.of(), List.of(), List.of(), sparkConf);
    }

    private String createSession() throws Exception {
        CreateSessionRequest request = sessionRequest(Map.of());
        String body = mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).path("id").asText();
    }

    private String execute(String sessionId) throws Exception {
        String body = mockMvc.perform(post("/api/v1/statements/execute")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new ExecuteCodeRequest(sessionId, "spark.range(1)", "pyspark"))))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).path("execution_id").asText();
    }

    private JsonNode awaitTerminalResult(String executionId) throws Exception {
        JsonNode result = null;
        for (int attempt = 0; attempt < 50; attempt++) {
            // The JSON response carries no charset, so the default ISO-8859-1 would garble Cyrillic text
            String body = mockMvc.perform(get("/api/v1/statements/" + executionId + "/result"))
                    .andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8);
            result = objectMapper.readTree(body);
            String state = result.path("status").asText();
            if (!state.equals("QUEUED") && !state.equals("RUNNING")) {
                return result;
            }
            Thread.sleep(100);
        }
        return result;
    }
}
