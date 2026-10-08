package org.apache.hadoop.explorer.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.spark.dto.*;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class SparkIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

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
}
