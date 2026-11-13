package io.github.balookrd.pristan.parom.orchestrator.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.Cookie;
import io.github.balookrd.pristan.zastava.model.LoginRequest;
import io.github.balookrd.pristan.parom.model.AgentHeartbeatRequest;
import io.github.balookrd.pristan.parom.orchestrator.dto.CreateJobRequest;
import io.github.balookrd.pristan.parom.orchestrator.registry.AgentRegistry;
import io.github.balookrd.pristan.parom.orchestrator.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
public class ParomMetricsTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AgentRegistry agentRegistry;

    @Autowired
    private JobService jobService;

    @Autowired
    private io.github.balookrd.pristan.parom.orchestrator.repository.KafkaReplicationJobRepository kafkaReplicationJobRepository;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Метрики раскрывают имена кластеров, задач и путей, а скрейп стоит запросов к БД,
     * поэтому эндпоинт требует аутентификации.
     */
    private Cookie adminCookie() throws Exception {
        var result = mockMvc.perform(post("/api/v1/auth/login")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new LoginRequest("admin", "secret"))))
                .andExpect(status().isOk())
                .andReturn();
        Cookie cookie = result.getResponse().getCookie("access_token");
        assertNotNull(cookie);
        return cookie;
    }

    @Test
    @DisplayName("Эндпоинт метрик недоступен без аутентификации")
    void testMetricsEndpointRequiresAuthentication() throws Exception {
        mockMvc.perform(get("/metrics"))
                .andExpect(status().isUnauthorized());
    }

    private static final String SCRAPE_TOKEN = "test-metrics-scrape-token-0123456789";

    private static String basic(String username, String password) {
        return "Basic " + Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Prometheus читает /metrics по Basic-учетке prometheus:<scrape-token>")
    void testMetricsEndpointAcceptsScrapeToken() throws Exception {
        mockMvc.perform(get("/metrics").header("Authorization", basic("prometheus", SCRAPE_TOKEN)))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("# TYPE parom_jobs_total gauge")))
                // Micrometer meters used by the "Orchestrator process" panel of parom_overview.json
                .andExpect(content().string(containsString("process_cpu_usage")))
                .andExpect(content().string(containsString("jvm_memory_used_bytes")));
    }

    @Test
    @DisplayName("Неверный scrape-токен или имя пользователя не дают доступа к /metrics")
    void testMetricsEndpointRejectsWrongScrapeCredentials() throws Exception {
        mockMvc.perform(get("/metrics").header("Authorization", basic("prometheus", "wrong-token")))
                .andExpect(status().isUnauthorized());
        mockMvc.perform(get("/metrics").header("Authorization", basic("admin", SCRAPE_TOKEN)))
                .andExpect(status().isUnauthorized());
        mockMvc.perform(get("/metrics").header("Authorization", "Basic not-base64!"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("Scrape-токен действует только на /metrics, а не на REST API")
    void testScrapeTokenDoesNotOpenApi() throws Exception {
        mockMvc.perform(get("/api/v1/agents").header("Authorization", basic("prometheus", SCRAPE_TOKEN)))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("Scrape-токен не открывает ADMIN-эндпоинты Actuator")
    void testScrapeTokenDoesNotOpenAdminActuator() throws Exception {
        mockMvc.perform(get("/actuator/env").header("Authorization", basic("prometheus", SCRAPE_TOKEN)))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("Scrape-токен принимается только на GET /metrics, POST отклоняется")
    void testScrapeTokenAcceptedOnGetOnly() throws Exception {
        mockMvc.perform(post("/metrics").header("Authorization", basic("prometheus", SCRAPE_TOKEN)))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("Проверка экспорта метрик Prometheus на эндпоинте /metrics")
    void testScrapeMetricsEndpoint() throws Exception {
        // Метрики Kafka и staging — глобальные суммы по всем задачам и агентам, а проверяются
        // точными числами. База в памяти общая для тестов одной JVM, поэтому состояние,
        // оставленное другими классами, сделало бы результат зависящим от порядка запуска.
        kafkaReplicationJobRepository.deleteAll();
        agentRegistry.clear();

        // Регистрируем тестовый агент с телеметрией
        AgentHeartbeatRequest hb = new AgentHeartbeatRequest("agent-metrics-node1", "dc1", "node1:50051", 2, "all");
        hb.setTransferredBytes(104857600L); // 100 MB
        hb.setTransferErrors(1L);
        hb.setBandwidthLimitMbS(50.0);
        hb.setCompressionRawBytes(104857600L);
        hb.setCompressionCompressedBytes(52428800L);
        hb.setCompressionRatio(2.0);
        hb.setChunkLatencyAvgMs(12.5);
        hb.setChunkLatencyMaxMs(45.0);
        hb.setChunkTransferCount(100L);
        hb.setStagingActiveFiles(3);
        hb.setStagingActiveBytes(3145728L);
        hb.setStagingOrphansCleaned(5L);
        agentRegistry.heartbeat(hb, "test-orchestrator-agent-secret-0123456789");

        // Создаем тестовую джобу HDFS
        CreateJobRequest req = new CreateJobRequest(
                "/data/metrics/src", "/backup/metrics/dst", "dc1", "dc2",
                10485760L, "hdfs@REALM.LOCAL", true, false,
                null, 10, "STANDARD", null
        );
        jobService.createJob(req, "metrics-tester");

        // Создаем тестовую джобу Kafka
        io.github.balookrd.pristan.parom.orchestrator.entity.KafkaReplicationJobEntity kJob =
                new io.github.balookrd.pristan.parom.orchestrator.entity.KafkaReplicationJobEntity();
        kJob.setId("kafka-job-metrics-1");
        kJob.setName("Orders & Payments Stream");
        kJob.setSourceDc("DC1");
        kJob.setTargetDc("DC2");
        kJob.setStatus("ACTIVE");
        kJob.setMessagesReplicated(150000L);
        kJob.setBytesReplicated(75000000L);
        kJob.setCurrentLagMs(12L);
        kJob.setBandwidthLimitMb(80.0);
        kJob.setTopicsJson("[]");
        kafkaReplicationJobRepository.save(kJob);

        mockMvc.perform(get("/metrics").cookie(adminCookie()))
                .andExpect(status().isOk())
                .andExpect(content().contentType("text/plain; version=0.0.4; charset=utf-8"))
                // Проверяем наличие канонических метрик Parom (гармонизированный стандарт)
                .andExpect(content().string(containsString("parom_bytes_total")))
                .andExpect(content().string(containsString("parom_jobs_total")))
                .andExpect(content().string(containsString("parom_active_workers")))
                .andExpect(content().string(containsString("parom_active_agents")))
                .andExpect(content().string(containsString("parom_throttling_delay_seconds_total")))
                .andExpect(content().string(containsString("parom_hms_jobs_total")))
                .andExpect(content().string(containsString("parom_dr_cluster_fenced")))
                .andExpect(content().string(containsString("parom_file_size_bytes_bucket")))
                .andExpect(content().string(containsString("parom_agent_status{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_agent_active_transfers{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_agent_transferred_bytes_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_agent_errors_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_agent_max_bandwidth_mb_s{agent_id=\"agent-metrics-node1\"")))
                // Новые enterprise метрики Micrometer / Prometheus
                .andExpect(content().string(containsString("parom_compression_raw_bytes_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_compression_compressed_bytes_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_compression_ratio{cluster_id=\"all\"} 2.00")))
                .andExpect(content().string(containsString("parom_compression_ratio{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_grpc_chunk_transfer_latency_avg_ms{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_grpc_chunk_transfer_latency_max_ms{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_grpc_chunk_transfer_latency_seconds_bucket")))
                .andExpect(content().string(containsString("parom_grpc_chunk_transfer_latency_seconds_count 100")))
                .andExpect(content().string(containsString("parom_staging_queue_size 3")))
                .andExpect(content().string(containsString("parom_staging_active_files{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("parom_staging_active_bytes 3145728")))
                .andExpect(content().string(containsString("parom_staging_orphans_cleaned_total 5")))
                // Метрики потоковой Kafka-репликации
                .andExpect(content().string(containsString("parom_kafka_jobs_total{status=\"ACTIVE\"} 1")))
                .andExpect(content().string(containsString("kafka_replicated_messages_total 150000")))
                .andExpect(content().string(containsString("kafka_replicated_bytes_total 75000000")))
                .andExpect(content().string(containsString("kafka_current_lag_ms 12")))
                .andExpect(content().string(containsString("parom_kafka_job_messages_total{job_id=\"kafka-job-metrics-1\"")))
                .andExpect(content().string(containsString("parom_kafka_job_bytes_total{job_id=\"kafka-job-metrics-1\"")))
                .andExpect(content().string(containsString("parom_kafka_job_lag_ms{job_id=\"kafka-job-metrics-1\"")))
                .andExpect(content().string(containsString("parom_kafka_bandwidth_limit_mb{job_id=\"kafka-job-metrics-1\"")))
                // Убеждаемся в отсутствии устаревших legacy-алиасов
                .andExpect(content().string(not(containsString("# HELP replication_bytes_total"))))
                .andExpect(content().string(not(containsString("# HELP replication_jobs_total"))))
                .andExpect(content().string(not(containsString("# HELP replication_tasks_total"))))
                .andExpect(content().string(not(containsString("# HELP active_workers"))))
                .andExpect(content().string(not(containsString("# HELP active_agents"))))
                .andExpect(content().string(not(containsString("# HELP throttling_delay_seconds_total"))))
                .andExpect(content().string(not(containsString("# HELP hms_replication_jobs_total"))))
                .andExpect(content().string(not(containsString("# HELP dr_cluster_fenced"))))
                .andExpect(content().string(not(containsString("# HELP agent_status"))))
                .andExpect(content().string(not(containsString("# HELP agent_active_transfers"))))
                .andExpect(content().string(not(containsString("# HELP agent_transferred_bytes_total"))))
                .andExpect(content().string(not(containsString("# HELP agent_errors_total"))));
    }
}
