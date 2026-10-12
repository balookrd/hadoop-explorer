package org.apache.hadoop.explorer.replicator.orchestrator.metrics;

import org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
public class ReplicatorMetricsTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AgentRegistry agentRegistry;

    @Autowired
    private JobService jobService;

    @Test
    @DisplayName("Проверка экспорта метрик Prometheus на эндпоинте /metrics")
    void testScrapeMetricsEndpoint() throws Exception {
        // Регистрируем тестовый агент с телеметрией
        AgentHeartbeatRequest hb = new AgentHeartbeatRequest("agent-metrics-node1", "dc1", "node1:50051", 2, "all");
        hb.setTransferredBytes(104857600L); // 100 MB
        hb.setTransferErrors(1L);
        hb.setBandwidthLimitMbS(50.0);
        agentRegistry.heartbeat(hb, "replicator-secure-agent-secret-key-12345");

        // Создаем тестовую джобу
        CreateJobRequest req = new CreateJobRequest(
                "/data/metrics/src", "/backup/metrics/dst", "dc1", "dc2",
                10485760L, "hdfs@REALM.LOCAL", true, false,
                null, 10, "STANDARD", null
        );
        jobService.createJob(req, "metrics-tester");

        mockMvc.perform(get("/metrics"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("text/plain; version=0.0.4; charset=utf-8"))
                // Проверяем наличие ключевых метрик HDFS репликации
                .andExpect(content().string(containsString("replication_bytes_total")))
                .andExpect(content().string(containsString("replication_jobs_total")))
                .andExpect(content().string(containsString("active_workers")))
                .andExpect(content().string(containsString("throttling_delay_seconds_total")))
                // Проверяем наличие метрик HMS и Disaster Recovery
                .andExpect(content().string(containsString("hms_replication_jobs_total")))
                .andExpect(content().string(containsString("dr_cluster_fenced")))
                // Проверяем наличие детальных метрик агентов
                .andExpect(content().string(containsString("agent_status{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("agent_active_transfers{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("agent_transferred_bytes_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("agent_errors_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("agent_max_bandwidth_mb_s{agent_id=\"agent-metrics-node1\"")));
    }
}
