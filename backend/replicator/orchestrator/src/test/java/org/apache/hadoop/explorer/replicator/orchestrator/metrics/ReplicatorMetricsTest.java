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
import static org.hamcrest.Matchers.not;
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

    @Autowired
    private org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository kafkaReplicationJobRepository;

    @Test
    @DisplayName("Проверка экспорта метрик Prometheus на эндпоинте /metrics")
    void testScrapeMetricsEndpoint() throws Exception {
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
        agentRegistry.heartbeat(hb, "replicator-secure-agent-secret-key-12345");

        // Создаем тестовую джобу HDFS
        CreateJobRequest req = new CreateJobRequest(
                "/data/metrics/src", "/backup/metrics/dst", "dc1", "dc2",
                10485760L, "hdfs@REALM.LOCAL", true, false,
                null, 10, "STANDARD", null
        );
        jobService.createJob(req, "metrics-tester");

        // Создаем тестовую джобу Kafka
        org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity kJob =
                new org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity();
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

        mockMvc.perform(get("/metrics"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("text/plain; version=0.0.4; charset=utf-8"))
                // Проверяем наличие канонических метрик Replicator (гармонизированный стандарт)
                .andExpect(content().string(containsString("replicator_bytes_total")))
                .andExpect(content().string(containsString("replicator_jobs_total")))
                .andExpect(content().string(containsString("replicator_active_workers")))
                .andExpect(content().string(containsString("replicator_active_agents")))
                .andExpect(content().string(containsString("replicator_throttling_delay_seconds_total")))
                .andExpect(content().string(containsString("replicator_hms_jobs_total")))
                .andExpect(content().string(containsString("replicator_dr_cluster_fenced")))
                .andExpect(content().string(containsString("replicator_file_size_bytes_bucket")))
                .andExpect(content().string(containsString("replicator_agent_status{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_agent_active_transfers{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_agent_transferred_bytes_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_agent_errors_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_agent_max_bandwidth_mb_s{agent_id=\"agent-metrics-node1\"")))
                // Новые enterprise метрики Micrometer / Prometheus
                .andExpect(content().string(containsString("replicator_compression_raw_bytes_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_compression_compressed_bytes_total{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_compression_ratio{cluster_id=\"all\"} 2.00")))
                .andExpect(content().string(containsString("replicator_compression_ratio{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_grpc_chunk_transfer_latency_avg_ms{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_grpc_chunk_transfer_latency_max_ms{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_grpc_chunk_transfer_latency_seconds_bucket")))
                .andExpect(content().string(containsString("replicator_grpc_chunk_transfer_latency_seconds_count 100")))
                .andExpect(content().string(containsString("replicator_staging_queue_size 3")))
                .andExpect(content().string(containsString("replicator_staging_active_files{agent_id=\"agent-metrics-node1\"")))
                .andExpect(content().string(containsString("replicator_staging_active_bytes 3145728")))
                .andExpect(content().string(containsString("replicator_staging_orphans_cleaned_total 5")))
                // Метрики потоковой Kafka-репликации
                .andExpect(content().string(containsString("replicator_kafka_jobs_total{status=\"ACTIVE\"} 1")))
                .andExpect(content().string(containsString("kafka_replicated_messages_total 150000")))
                .andExpect(content().string(containsString("kafka_replicated_bytes_total 75000000")))
                .andExpect(content().string(containsString("kafka_current_lag_ms 12")))
                .andExpect(content().string(containsString("replicator_kafka_job_messages_total{job_id=\"kafka-job-metrics-1\"")))
                .andExpect(content().string(containsString("replicator_kafka_job_bytes_total{job_id=\"kafka-job-metrics-1\"")))
                .andExpect(content().string(containsString("replicator_kafka_job_lag_ms{job_id=\"kafka-job-metrics-1\"")))
                .andExpect(content().string(containsString("replicator_kafka_bandwidth_limit_mb{job_id=\"kafka-job-metrics-1\"")))
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
