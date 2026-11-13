package io.github.balookrd.pristan.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balookrd.pristan.spark.dto.*;
import io.github.balookrd.pristan.spark.service.SparkMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class SparkIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private SparkMetrics sparkMetrics;

    @Test
    void probeEndpointsAreServedWithoutAuth() throws Exception {
        // Liveness probe -> /healthz, readiness probe -> /actuator/health/readiness
        mockMvc.perform(get("/healthz"))
                .andExpect(status().isOk());
        mockMvc.perform(get("/actuator/health/readiness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("UP")));
        // Prometheus scrapes without credentials
        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk());
    }

    @Test
    void testHealthEndpoint() throws Exception {
        mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("healthy")))
                .andExpect(jsonPath("$.service", is("spark-explorer")));
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void testClustersEndpoints() throws Exception {
        mockMvc.perform(get("/api/v1/clusters"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(1))))
                .andExpect(jsonPath("$[0].id", is("prod-analytics")));

        mockMvc.perform(get("/api/v1/clusters/prod-analytics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is("prod-analytics")))
                .andExpect(jsonPath("$.spark_versions").isArray())
                .andExpect(jsonPath("$.resource_profiles").isArray());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void testSessionAndStatementExecutionFlow() throws Exception {
        CreateSessionRequest createReq = new CreateSessionRequest(
                "prod-analytics",
                "spark-3.5.1",
                "hive-prod",
                "root.analytics",
                "small",
                "pyspark",
                "py310",
                null,
                null,
                List.of(),
                List.of(),
                List.of(),
                Map.of()
        );

        MvcResult sessResult = mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(createReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").isNotEmpty())
                .andExpect(jsonPath("$.status", is("idle")))
                .andReturn();

        SessionResponse sessResp = objectMapper.readValue(sessResult.getResponse().getContentAsString(), SessionResponse.class);
        String sessionId = sessResp.id();

        // Запуск выполнения кода
        ExecuteCodeRequest codeReq = new ExecuteCodeRequest(sessionId, "df = spark.table('customers')\ndf.show()", "pyspark");
        MvcResult codeResult = mockMvc.perform(post("/api/v1/statements/execute")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(codeReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.execution_id").isNotEmpty())
                .andExpect(jsonPath("$.status", is("QUEUED")))
                .andReturn();

        ExecuteCodeResponse execResp = objectMapper.readValue(codeResult.getResponse().getContentAsString(), ExecuteCodeResponse.class);
        String executionId = execResp.executionId();

        // Ожидаем завершения в Mock Spark Engine
        Thread.sleep(400);

        mockMvc.perform(get("/api/v1/statements/" + executionId + "/result"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.execution_id", is(executionId)))
                .andExpect(jsonPath("$.columns").isArray())
                .andExpect(jsonPath("$.rows").isArray());

        mockMvc.perform(get("/api/v1/history"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].id", hasItem(executionId)));
    }

    private String executeAs(String username) throws Exception {
        CreateSessionRequest createReq = new CreateSessionRequest("prod-analytics", "spark-3.5.1", "hive-prod",
                "root.analytics", "small", "pyspark", "py310", null, null, List.of(), List.of(), List.of(), Map.of());
        MvcResult sessResult = mockMvc.perform(post("/api/v1/sessions").with(user(username).roles("USER"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(createReq)))
                .andExpect(status().isOk())
                .andReturn();
        String sessionId = objectMapper.readValue(sessResult.getResponse().getContentAsString(), SessionResponse.class).id();
        ExecuteCodeRequest codeReq = new ExecuteCodeRequest(sessionId, "df = spark.table('customers')\ndf.show()", "pyspark");
        MvcResult codeResult = mockMvc.perform(post("/api/v1/statements/execute").with(user(username).roles("USER"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(codeReq)))
                .andExpect(status().isOk())
                .andReturn();
        return objectMapper.readValue(codeResult.getResponse().getContentAsString(), ExecuteCodeResponse.class).executionId();
    }

    @Test
    void resultAndStreamOfAnotherUsersStatementAreNotFound() throws Exception {
        String executionId = executeAs("data_engineer");
        Thread.sleep(400);

        // 404, not 403: the response must not confirm that the execution id exists
        mockMvc.perform(get("/api/v1/statements/" + executionId + "/result").with(user("intruder").roles("USER")))
                .andExpect(status().isNotFound());
        mockMvc.perform(get("/api/v1/statements/" + executionId + "/stream").with(user("intruder").roles("USER")))
                .andExpect(status().isNotFound());
    }

    @Test
    void ownerAndAdminGetResultAndStream() throws Exception {
        String executionId = executeAs("data_engineer");
        Thread.sleep(400);

        mockMvc.perform(get("/api/v1/statements/" + executionId + "/result").with(user("data_engineer").roles("USER")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.execution_id", is(executionId)));
        mockMvc.perform(get("/api/v1/statements/" + executionId + "/stream").with(user("data_engineer").roles("USER")))
                .andExpect(status().isOk())
                .andExpect(request().asyncStarted());
        mockMvc.perform(get("/api/v1/statements/" + executionId + "/result").with(user("root").roles("ADMIN")))
                .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void testPipelineDagValidationAndExecution() throws Exception {
        // 1. Корректный DAG (A -> B)
        PipelineNode nodeA = new PipelineNode("n1", "Extract", "pyspark", "code", 300, Map.of(), new NodePosition(0, 0));
        PipelineNode nodeB = new PipelineNode("n2", "Transform", "pyspark", "code", 300, Map.of(), new NodePosition(100, 100));
        PipelineEdge edge = new PipelineEdge("n1", "n2");

        PipelineDefinition validDef = new PipelineDefinition(
                "pipe-1", "ETL Pipeline", "Valid DAG", "prod-analytics",
                List.of(nodeA, nodeB), List.of(edge), 0, 0
        );

        mockMvc.perform(post("/api/v1/pipelines")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validDef)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is("pipe-1")));

        // Запуск пайплайна
        mockMvc.perform(post("/api/v1/pipelines/pipe-1/run"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.pipeline_id", is("pipe-1")))
                .andExpect(jsonPath("$.status", is("success")));

        // 2. Циклический граф (A -> B, B -> A) => должен вернуть 422 Unprocessable Entity
        PipelineEdge cycleEdge = new PipelineEdge("n2", "n1");
        PipelineDefinition cyclicDef = new PipelineDefinition(
                "pipe-cycle", "Cyclic Pipeline", "Invalid DAG", "prod-analytics",
                List.of(nodeA, nodeB), List.of(edge, cycleEdge), 0, 0
        );

        mockMvc.perform(post("/api/v1/pipelines")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(cyclicDef)))
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void testWorkspaceOperations() throws Exception {
        Map<String, Object> state = Map.of("editorMode", "pyspark", "notebookContent", "# Hello Spark");
        WorkspacePayload payload = new WorkspacePayload(state);

        mockMvc.perform(put("/api/v1/workspace")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(payload)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username", is("data_engineer")))
                .andExpect(jsonPath("$.state.editorMode", is("pyspark")));

        mockMvc.perform(get("/api/v1/workspace"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username", is("data_engineer")))
                .andExpect(jsonPath("$.state.notebookContent", is("# Hello Spark")));
    }

    @Test
    @WithMockUser(username = "data_engineer", roles = {"USER"})
    void prometheusEndpointExposesSessionAndStatementMetrics() throws Exception {
        CreateSessionRequest createReq = new CreateSessionRequest(
                "prod-analytics", "spark-3.5.1", "hive-prod", "root.analytics", "small", "pyspark", "py310",
                null, null, List.of(), List.of(), List.of(), Map.of()
        );
        MvcResult sessResult = mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(createReq)))
                .andExpect(status().isOk())
                .andReturn();
        String sessionId = objectMapper.readValue(sessResult.getResponse().getContentAsString(), SessionResponse.class).id();

        ExecuteCodeRequest codeReq = new ExecuteCodeRequest(sessionId, "df = spark.table('customers')\ndf.show()", "pyspark");
        mockMvc.perform(post("/api/v1/statements/execute")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(codeReq)))
                .andExpect(status().isOk());

        // The statement runs on an executor thread; wait until its terminal status is recorded
        scrapeUntilContains(
                "spark_statement_duration_seconds_count{app=\"spark\",cluster=\"prod-analytics\",language=\"pyspark\",status=\"FINISHED\"}");

        // The gauge is refreshed every 30 s by the scheduler; refresh now instead of waiting
        sparkMetrics.refreshActiveSessions();
        String body = scrapeUntilContains("spark_sessions_active{app=\"spark\",cluster=\"prod-analytics\",kind=\"pyspark\"}");

        assertTrue(body.contains("http_server_requests_seconds_count{app=\"spark\""),
                "HTTP server metrics must carry app=spark");
    }

    private String scrapeUntilContains(String series) throws Exception {
        String body = "";
        for (int attempt = 0; attempt < 50; attempt++) {
            body = mockMvc.perform(get("/actuator/prometheus"))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            if (body.contains(series)) {
                return body;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Series not found in /actuator/prometheus after 5 s: " + series + "\n" + body);
    }
}
