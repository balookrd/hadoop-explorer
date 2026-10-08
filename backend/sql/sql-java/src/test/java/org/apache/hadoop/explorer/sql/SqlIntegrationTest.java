package org.apache.hadoop.explorer.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.sql.dto.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class SqlIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void testHealthEndpoint() throws Exception {
        mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("healthy")))
                .andExpect(jsonPath("$.service", is("sql-explorer")));
    }

    @Test
    @WithMockUser(username = "analyst", roles = {"USER"})
    void testClustersEndpoint() throws Exception {
        mockMvc.perform(get("/api/v1/clusters"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(2))))
                .andExpect(jsonPath("$[0].id", is("trino-prod")))
                .andExpect(jsonPath("$[0].type", is("trino")));
    }

    @Test
    @WithMockUser(username = "analyst", roles = {"USER"})
    void testCatalogMetadata() throws Exception {
        mockMvc.perform(get("/api/v1/catalog/trino-prod/catalogs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasItem("tpch")));

        mockMvc.perform(get("/api/v1/catalog/trino-prod/schemas").param("catalog", "tpch"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasItem("sf1")));

        mockMvc.perform(get("/api/v1/catalog/trino-prod/tables").param("catalog", "tpch").param("schema", "sf1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasItem("customer")));

        mockMvc.perform(get("/api/v1/catalog/trino-prod/columns")
                        .param("catalog", "tpch")
                        .param("schema", "sf1")
                        .param("table", "customer"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].name", hasItem("custkey")));
    }

    @Test
    @WithMockUser(username = "analyst", roles = {"USER"})
    void testQueryExecutionFlow() throws Exception {
        ExecuteQueryRequest req = new ExecuteQueryRequest("trino-prod", "SELECT * FROM tpch.sf1.customer LIMIT 10");
        MvcResult res = mockMvc.perform(post("/api/v1/queries/execute")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.query_id").isNotEmpty())
                .andExpect(jsonPath("$.status", is("QUEUED")))
                .andReturn();

        ExecuteQueryResponse resp = objectMapper.readValue(res.getResponse().getContentAsString(), ExecuteQueryResponse.class);
        String queryId = resp.queryId();

        // Ожидаем выполнение запроса
        Thread.sleep(600);

        mockMvc.perform(get("/api/v1/queries/" + queryId + "/result"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.query_id", is(queryId)))
                .andExpect(jsonPath("$.columns").isArray())
                .andExpect(jsonPath("$.rows").isArray());

        mockMvc.perform(get("/api/v1/queries/history"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].id", hasItem(queryId)));
    }

    @Test
    @WithMockUser(username = "analyst", roles = {"USER"})
    void testWorkspacePersistence() throws Exception {
        Map<String, Object> state = Map.of("activeTab", "tab-1", "tabs", Map.of("tab-1", "SELECT 1;"));
        WorkspacePayload payload = new WorkspacePayload(state);

        mockMvc.perform(put("/api/v1/workspace")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(payload)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username", is("analyst")))
                .andExpect(jsonPath("$.state.activeTab", is("tab-1")));

        mockMvc.perform(get("/api/v1/workspace"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username", is("analyst")))
                .andExpect(jsonPath("$.state.activeTab", is("tab-1")));
    }

    @Test
    @WithMockUser(username = "analyst", roles = {"USER"})
    void testAiOperations() throws Exception {
        mockMvc.perform(get("/api/v1/ai/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available", is(true)));

        CheckSqlRequest checkReq = new CheckSqlRequest("SELECT * FROM orders", "trino", "trino-prod", Map.of());
        mockMvc.perform(post("/api/v1/ai/check")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(checkReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.issues").isArray());

        FormatSqlRequest formatReq = new FormatSqlRequest("select a, b from c where d=1", "trino", "trino-prod");
        mockMvc.perform(post("/api/v1/ai/format")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(formatReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.formatted_sql").isNotEmpty());

        GenerateSqlRequest genReq = new GenerateSqlRequest("покажи топ пользователей", "trino", "trino-prod", Map.of());
        mockMvc.perform(post("/api/v1/ai/generate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(genReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.generated_sql").isNotEmpty());
    }
}
