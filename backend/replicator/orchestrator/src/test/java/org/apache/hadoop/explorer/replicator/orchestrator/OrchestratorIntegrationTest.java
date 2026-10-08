package org.apache.hadoop.explorer.replicator.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.Cookie;
import org.apache.hadoop.explorer.common.model.LoginRequest;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.model.TokenRequest;
import org.apache.hadoop.explorer.replicator.model.UpdateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(classes = ReplicatorOrchestratorApplication.class)
@AutoConfigureMockMvc
class OrchestratorIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Сквозной интеграционный сценарий Оркестратора: Login -> Topology -> Agents -> Jobs Lifecycle -> Token Bucket")
    void fullOrchestratorWorkflowTest() throws Exception {
        // 1. Health Check
        mockMvc.perform(get("/health"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("UP"));

        // 2. Аутентификация администратора через common-security-starter
        LoginRequest loginRequest = new LoginRequest("admin", "secret");
        MvcResult loginResult = mockMvc.perform(post("/api/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(loginRequest)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true))
            .andReturn();

        Cookie authCookie = loginResult.getResponse().getCookie("access_token");
        assertNotNull(authCookie);

        // 3. Получение топологии
        mockMvc.perform(get("/api/v1/topology").cookie(authCookie))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.datacenters").isArray())
            .andExpect(jsonPath("$.clusters").isArray());

        // 4. Регистрация агента с заголовком X-Agent-Secret
        AgentRegisterRequest agentReg = new AgentRegisterRequest("agent-dc1-node1", "dc1", "all", "node1:50051", "node1", 100.0);
        mockMvc.perform(post("/api/v1/agents/register")
                .header("X-Agent-Secret", "replicator-secure-agent-secret-key-12345")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(agentReg)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("registered"));

        // 5. Запрос квоты полосы пропускания (Token Bucket)
        TokenRequest tokenReq = new TokenRequest("agent-dc1-node1", 1048576, "dc1", "dc2");
        mockMvc.perform(post("/api/v1/tokens/request")
                .header("X-Agent-Secret", "replicator-secure-agent-secret-key-12345")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(tokenReq)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.wait_seconds").value(0.0));

        // 6. Создание задачи репликации
        CreateJobRequest createJob = new CreateJobRequest(
            "/data/warehouse/tables",
            "/backup/warehouse/tables",
            "dc1",
            "dc2",
            10485760L,
            "hdfs@EXAMPLE.COM",
            true,
            false,
            null
        );

        MvcResult jobResult = mockMvc.perform(post("/api/v1/jobs")
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createJob)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.id").isNotEmpty())
            .andExpect(jsonPath("$.status").value("QUEUED"))
            .andReturn();

        JobResponse job = objectMapper.readValue(jobResult.getResponse().getContentAsString(), JobResponse.class);
        String jobId = job.id();

        // 7. Запуск задачи (постановка в очередь QUEUED)
        mockMvc.perform(post("/api/v1/jobs/" + jobId + "/start")
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("QUEUED"));

        // 8. Обновление прогресса агентом
        UpdateJobRequest updateReq = new UpdateJobRequest();
        updateReq.setCopiedBytes(5242880L);
        updateReq.setStatus("RUNNING");
        updateReq.setMessage("Replicated 5 MB");

        mockMvc.perform(patch("/api/v1/jobs/" + jobId)
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(updateReq)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.copied_bytes").value(5242880L))
            .andExpect(jsonPath("$.progress_percent").value(50.0));

        // 9. Проверка истории запусков (Job Runs)
        mockMvc.perform(get("/api/v1/jobs/" + jobId + "/runs").cookie(authCookie))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$").isArray())
            .andExpect(jsonPath("$[0].job_id").value(jobId));

        // 10. Удаление задачи
        mockMvc.perform(delete("/api/v1/jobs/" + jobId)
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true));

        mockMvc.perform(get("/api/v1/jobs/" + jobId).cookie(authCookie))
            .andExpect(status().isNotFound());
    }
}
