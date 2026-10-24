package org.apache.hadoop.explorer.replicator.orchestrator.metrics;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.AgentResponseDto;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrStatusResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.KafkaReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.DisasterRecoveryService;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Централизованный сервис сбора и экспорта метрик Prometheus для Hadoop gRPC Replicator:
 * - Оркестратор: задачи HDFS, пулы пофайловых задач, шейпинг полосы Token Bucket
 * - Агенты: статус доступности флота, активные трансферы, переданный объем байт, ошибки
 * - Репликация Hive Metastore (HMS): прогресс Bootstrap, скорость, ETA, CDC стриминг
 * - Disaster Recovery (DR): статус изоляции (fencing), аварийный рубильник (kill-switch), обратная репликация
 * - Репликация Kafka: сквозной лаг (lag ms), счетчики сообщений и байт, лимиты полосы
 */
@Service
public class ReplicatorMetricsService {

    private static final Logger log = LoggerFactory.getLogger(ReplicatorMetricsService.class);

    private final JobRepository jobRepository;
    private final TaskRepository taskRepository;
    private final HmsReplicationJobRepository hmsReplicationJobRepository;
    private final KafkaReplicationJobRepository kafkaReplicationJobRepository;
    private final AgentRegistry agentRegistry;
    private final TokenBucketThrottler throttler;
    private final DisasterRecoveryService disasterRecoveryService;

    // Внутренний счетчик накопленной задержки троттлинга
    private final AtomicLong accumulatedThrottlingNanos = new AtomicLong(0);

    @Autowired
    public ReplicatorMetricsService(
            JobRepository jobRepository,
            TaskRepository taskRepository,
            HmsReplicationJobRepository hmsReplicationJobRepository,
            KafkaReplicationJobRepository kafkaReplicationJobRepository,
            AgentRegistry agentRegistry,
            TokenBucketThrottler throttler,
            DisasterRecoveryService disasterRecoveryService
    ) {
        this.jobRepository = jobRepository;
        this.taskRepository = taskRepository;
        this.hmsReplicationJobRepository = hmsReplicationJobRepository;
        this.kafkaReplicationJobRepository = kafkaReplicationJobRepository;
        this.agentRegistry = agentRegistry;
        this.throttler = throttler;
        this.disasterRecoveryService = disasterRecoveryService;
    }

    public ReplicatorMetricsService(
            JobRepository jobRepository,
            TaskRepository taskRepository,
            HmsReplicationJobRepository hmsReplicationJobRepository,
            AgentRegistry agentRegistry,
            TokenBucketThrottler throttler,
            DisasterRecoveryService disasterRecoveryService
    ) {
        this(jobRepository, taskRepository, hmsReplicationJobRepository, null, agentRegistry, throttler, disasterRecoveryService);
    }

    public void recordThrottlingWait(double waitSeconds) {
        if (waitSeconds > 0) {
            accumulatedThrottlingNanos.addAndGet((long) (waitSeconds * 1_000_000_000L));
        }
    }

    /**
     * Генерация полного набора текстовых метрик Prometheus для Hadoop gRPC Replicator:
     * HDFS задачи, пофайловые таски, шейпинг, воркеры, HMS и DR.
     */
    public String renderReplicatorPrometheusMetrics() {
        StringBuilder sb = new StringBuilder();

        try {
            // 1. replicator_bytes_total
            double bytesCompleted = calculateBytesByStatus("COMPLETED");
            double bytesRunning = calculateBytesByStatus("RUNNING");
            double bytesFailed = calculateBytesByStatus("FAILED");

            sb.append("# HELP replicator_bytes_total Суммарный объем переданных байт данных репликации\n");
            sb.append("# TYPE replicator_bytes_total counter\n");
            sb.append(String.format(Locale.US, "replicator_bytes_total{status=\"COMPLETED\"} %.0f\n", bytesCompleted));
            sb.append(String.format(Locale.US, "replicator_bytes_total{status=\"IN_PROGRESS\"} %.0f\n", bytesRunning));
            sb.append(String.format(Locale.US, "replicator_bytes_total{status=\"FAILED\"} %.0f\n", bytesFailed));

            // 2. replicator_active_workers & replicator_active_agents
            double onlineAgents = countAgentsByStatus("online");
            double totalAgents = countTotalAgents();

            sb.append("# HELP replicator_active_workers Количество активных онлайн-воркеров репликации\n");
            sb.append("# TYPE replicator_active_workers gauge\n");
            sb.append(String.format(Locale.US, "replicator_active_workers %.0f\n", onlineAgents));

            sb.append("# HELP replicator_active_agents Количество доступных агентов репликации\n");
            sb.append("# TYPE replicator_active_agents gauge\n");
            sb.append(String.format(Locale.US, "replicator_active_agents{status=\"online\"} %.0f\n", onlineAgents));
            sb.append(String.format(Locale.US, "replicator_active_agents{status=\"total\"} %.0f\n", totalAgents));

            // 3. Throttling metrics: replicator_throttling_*
            double waitSec = accumulatedThrottlingNanos.get() / 1_000_000_000.0;
            double globalLimit = throttler.getGlobalLimit();

            sb.append("# HELP replicator_throttling_delay_seconds_total Суммарная накопленная задержка из-за ограничения полосы Token Bucket Throttler\n");
            sb.append("# TYPE replicator_throttling_delay_seconds_total counter\n");
            sb.append(String.format(Locale.US, "replicator_throttling_delay_seconds_total %.4f\n", waitSec));

            sb.append("# HELP replicator_throttling_delay_seconds Накопленная задержка из-за ограничения полосы Token Bucket\n");
            sb.append("# TYPE replicator_throttling_delay_seconds gauge\n");
            sb.append(String.format(Locale.US, "replicator_throttling_delay_seconds %.4f\n", waitSec));

            sb.append("# HELP replicator_throttling_global_limit_bytes_per_sec Текущий глобальный лимит пропускной способности репликации\n");
            sb.append("# TYPE replicator_throttling_global_limit_bytes_per_sec gauge\n");
            sb.append(String.format(Locale.US, "replicator_throttling_global_limit_bytes_per_sec %.0f\n", globalLimit));

            // 4. replicator_jobs_total
            double runningJobs = countJobsByStatus("RUNNING");
            double queuedJobs = countJobsByStatus("QUEUED");
            double completedJobs = countJobsByStatus("COMPLETED");
            double failedJobs = countJobsByStatus("FAILED");
            double scheduledJobs = countJobsByScheduled(true);
            double manualJobs = countJobsByScheduled(false);

            sb.append("# HELP replicator_jobs_total Количество задач репликации HDFS по статусам и режимам\n");
            sb.append("# TYPE replicator_jobs_total gauge\n");
            sb.append(String.format(Locale.US, "replicator_jobs_total{status=\"RUNNING\"} %.0f\n", runningJobs));
            sb.append(String.format(Locale.US, "replicator_jobs_total{status=\"QUEUED\"} %.0f\n", queuedJobs));
            sb.append(String.format(Locale.US, "replicator_jobs_total{status=\"COMPLETED\"} %.0f\n", completedJobs));
            sb.append(String.format(Locale.US, "replicator_jobs_total{status=\"FAILED\"} %.0f\n", failedJobs));
            sb.append(String.format(Locale.US, "replicator_jobs_total{status=\"SCHEDULED\",mode=\"scheduled\"} %.0f\n", scheduledJobs));
            sb.append(String.format(Locale.US, "replicator_jobs_total{status=\"MANUAL\",mode=\"manual\"} %.0f\n", manualJobs));

            // 5. replicator_tasks_total
            double queuedTasks = countTasksByStatus("QUEUED");
            double runningTasks = countTasksByStatus("RUNNING");
            double completedTasks = countTasksByStatus("COMPLETED");
            double failedTasks = countTasksByStatus("FAILED");

            sb.append("# HELP replicator_tasks_total Количество пофайловых задач в распределенном пуле\n");
            sb.append("# TYPE replicator_tasks_total gauge\n");
            sb.append(String.format(Locale.US, "replicator_tasks_total{status=\"QUEUED\"} %.0f\n", queuedTasks));
            sb.append(String.format(Locale.US, "replicator_tasks_total{status=\"RUNNING\"} %.0f\n", runningTasks));
            sb.append(String.format(Locale.US, "replicator_tasks_total{status=\"COMPLETED\"} %.0f\n", completedTasks));
            sb.append(String.format(Locale.US, "replicator_tasks_total{status=\"FAILED\"} %.0f\n", failedTasks));

            // 6. Гистограмма распределения размеров файлов: replicator_file_size_bytes & replication_file_size_bytes
            renderFileSizeMetrics(sb);

            // 7. HMS Replication metrics: replicator_hms_* & hms_replication_*
            renderHmsMetrics(sb);

            // 8. Disaster Recovery metrics: replicator_dr_* & dr_*
            renderDrMetrics(sb);

            // 9. Детальные метрики агентов: replicator_agent_* & agent_*
            renderAgentMetrics(sb);

            // 10. Kafka Replication metrics: replicator_kafka_* & kafka_*
            renderKafkaMetrics(sb);

        } catch (Exception e) {
            log.error("Ошибка при генерации метрик Prometheus: {}", e.getMessage(), e);
        }

        return sb.toString();
    }

    private void renderFileSizeMetrics(StringBuilder sb) {
        try {
            // Один агрегат вместо чтения всех подзадач в heap
            List<Object[]> rows = taskRepository.aggregateFileSizeHistogram();
            long b1M = 0, b10M = 0, b100M = 0, b1G = 0, b10G = 0, count = 0, sum = 0;
            if (rows != null && !rows.isEmpty() && rows.get(0) != null) {
                Object[] row = rows.get(0);
                count = toLong(row[0]);
                sum = toLong(row[1]);
                b1M = toLong(row[2]);
                b10M = toLong(row[3]);
                b100M = toLong(row[4]);
                b1G = toLong(row[5]);
                b10G = toLong(row[6]);
            }

            sb.append("# HELP replicator_file_size_bytes Распределение размеров файлов репликации\n");
            sb.append("# TYPE replicator_file_size_bytes histogram\n");
            sb.append(String.format(Locale.US, "replicator_file_size_bytes_bucket{le=\"1048576\"} %d\n", b1M));
            sb.append(String.format(Locale.US, "replicator_file_size_bytes_bucket{le=\"10485760\"} %d\n", b10M));
            sb.append(String.format(Locale.US, "replicator_file_size_bytes_bucket{le=\"104857600\"} %d\n", b100M));
            sb.append(String.format(Locale.US, "replicator_file_size_bytes_bucket{le=\"1073741824\"} %d\n", b1G));
            sb.append(String.format(Locale.US, "replicator_file_size_bytes_bucket{le=\"10737418240\"} %d\n", b10G));
            sb.append(String.format(Locale.US, "replicator_file_size_bytes_bucket{le=\"+Inf\"} %d\n", count));
            sb.append(String.format(Locale.US, "replicator_file_size_bytes_sum %d\n", sum));
            sb.append(String.format(Locale.US, "replicator_file_size_bytes_count %d\n", count));
        } catch (Exception e) {
            log.error("Ошибка при генерации метрик размеров файлов: {}", e.getMessage());
        }
    }

    private void renderHmsMetrics(StringBuilder sb) {
        double bootstrapping = countHmsJobsByStatus("BOOTSTRAPPING");
        double streaming = countHmsJobsByStatus("STREAMING");
        double waiting = countHmsJobsByStatus("WAITING_FOR_AGENTS");
        double completed = countHmsJobsByStatus("COMPLETED");
        double failed = countHmsJobsByStatus("FAILED");

        sb.append("# HELP replicator_hms_jobs_total Количество задач репликации Hive Metastore по статусам\n");
        sb.append("# TYPE replicator_hms_jobs_total gauge\n");
        sb.append(String.format(Locale.US, "replicator_hms_jobs_total{status=\"BOOTSTRAPPING\"} %.0f\n", bootstrapping));
        sb.append(String.format(Locale.US, "replicator_hms_jobs_total{status=\"STREAMING\"} %.0f\n", streaming));
        sb.append(String.format(Locale.US, "replicator_hms_jobs_total{status=\"WAITING_FOR_AGENTS\"} %.0f\n", waiting));
        sb.append(String.format(Locale.US, "replicator_hms_jobs_total{status=\"COMPLETED\"} %.0f\n", completed));
        sb.append(String.format(Locale.US, "replicator_hms_jobs_total{status=\"FAILED\"} %.0f\n", failed));

        double totalTables = calculateHmsTables(false);
        double repTables = calculateHmsTables(true);
        sb.append("# HELP replicator_hms_tables_total Количество таблиц в схемах HMS\n");
        sb.append("# TYPE replicator_hms_tables_total gauge\n");
        sb.append(String.format(Locale.US, "replicator_hms_tables_total{type=\"total\"} %.0f\n", totalTables));
        sb.append(String.format(Locale.US, "replicator_hms_tables_total{type=\"replicated\"} %.0f\n", repTables));

        double totalPartitions = calculateHmsPartitions(false);
        double repPartitions = calculateHmsPartitions(true);
        sb.append("# HELP replicator_hms_partitions_total Количество партиций таблиц HMS\n");
        sb.append("# TYPE replicator_hms_partitions_total gauge\n");
        sb.append(String.format(Locale.US, "replicator_hms_partitions_total{type=\"total\"} %.0f\n", totalPartitions));
        sb.append(String.format(Locale.US, "replicator_hms_partitions_total{type=\"replicated\"} %.0f\n", repPartitions));

        double speed = calculateHmsBootstrapSpeedBytesPerSec();
        sb.append("# HELP replicator_hms_speed_bytes_per_second Текущая средняя скорость первичной синхронизации HMS\n");
        sb.append("# TYPE replicator_hms_speed_bytes_per_second gauge\n");
        sb.append(String.format(Locale.US, "replicator_hms_speed_bytes_per_second %.2f\n", speed));
    }

    private void renderDrMetrics(StringBuilder sb) {
        sb.append("# HELP replicator_dr_cluster_fenced Статус изоляции кластера (1 = Fenced / Kill-Switch активирован, 0 = Active)\n");
        sb.append("# TYPE replicator_dr_cluster_fenced gauge\n");
        try {
            DrStatusResponse drStatus = disasterRecoveryService.getDrStatus();
            if (drStatus != null && drStatus.clusters() != null && !drStatus.clusters().isEmpty()) {
                for (var cluster : drStatus.clusters()) {
                    double fenced = (Boolean.TRUE.equals(cluster.isFenced()) || isClusterFenced(cluster.id())) ? 1.0 : 0.0;
                    sb.append(String.format(Locale.US, "replicator_dr_cluster_fenced{cluster_id=\"%s\"} %.1f\n", escape(cluster.id()), fenced));
                }
            } else if (drStatus != null && drStatus.datacenters() != null && !drStatus.datacenters().isEmpty()) {
                for (var dc : drStatus.datacenters()) {
                    double fenced = (dc.isFenced() || dc.bandwidthLimitMbS() == 0.0) ? 1.0 : 0.0;
                    sb.append(String.format(Locale.US, "replicator_dr_cluster_fenced{cluster_id=\"%s\"} %.1f\n", escape(dc.id()), fenced));
                }
            }
        } catch (Exception ignored) {}

        double revRunning = countReverseJobs("RUNNING");
        double revCompleted = countReverseJobs("COMPLETED");

        sb.append("# HELP replicator_dr_reverse_jobs_total Количество задач обратной репликации при Failover\n");
        sb.append("# TYPE replicator_dr_reverse_jobs_total gauge\n");
        sb.append(String.format(Locale.US, "replicator_dr_reverse_jobs_total{status=\"RUNNING\"} %.0f\n", revRunning));
        sb.append(String.format(Locale.US, "replicator_dr_reverse_jobs_total{status=\"COMPLETED\"} %.0f\n", revCompleted));
    }

    private void renderAgentMetrics(StringBuilder sb) {
        List<AgentResponseDto> agents = agentRegistry.getAgentDtos();
        if (agents.isEmpty()) {
            return;
        }

        // 1. replicator_agent_status
        sb.append("# HELP replicator_agent_status Статус доступности агента (1 = online, 0 = offline/stale)\n");
        sb.append("# TYPE replicator_agent_status gauge\n");
        for (var a : agents) {
            double val = "online".equalsIgnoreCase(a.status()) ? 1.0 : 0.0;
            sb.append(String.format(Locale.US, "replicator_agent_status{agent_id=\"%s\",cluster_id=\"%s\",mode=\"%s\",status=\"%s\"} %.1f\n",
                    escape(a.agentId()), escape(a.clusterId()), escape(a.mode()), escape(a.status()), val));
        }

        // 2. replicator_agent_active_transfers
        sb.append("# HELP replicator_agent_active_transfers Текущее количество параллельных передач на агенте\n");
        sb.append("# TYPE replicator_agent_active_transfers gauge\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_agent_active_transfers{agent_id=\"%s\",cluster_id=\"%s\",mode=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), escape(a.mode()), a.activeTransfers()));
        }

        // 3. replicator_agent_heartbeat_age_seconds
        sb.append("# HELP replicator_agent_heartbeat_age_seconds Время с момента последнего keepalive от агента\n");
        sb.append("# TYPE replicator_agent_heartbeat_age_seconds gauge\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_agent_heartbeat_age_seconds{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.heartbeatAgeSeconds()));
        }

        // 4. replicator_agent_max_bandwidth_mb_s
        sb.append("# HELP replicator_agent_max_bandwidth_mb_s Выделенный локальный лимит полосы пропускания HTB агента (МБ/с)\n");
        sb.append("# TYPE replicator_agent_max_bandwidth_mb_s gauge\n");
        for (var a : agents) {
            double bw = a.maxBandwidthMbS() != null ? a.maxBandwidthMbS() : 0.0;
            sb.append(String.format(Locale.US, "replicator_agent_max_bandwidth_mb_s{agent_id=\"%s\",cluster_id=\"%s\"} %.2f\n",
                    escape(a.agentId()), escape(a.clusterId()), bw));
        }

        // 5. replicator_agent_transferred_bytes_total
        sb.append("# HELP replicator_agent_transferred_bytes_total Суммарный объем байт данных переданных агентом\n");
        sb.append("# TYPE replicator_agent_transferred_bytes_total counter\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_agent_transferred_bytes_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.transferredBytes()));
        }

        // 6. replicator_agent_errors_total
        sb.append("# HELP replicator_agent_errors_total Количество ошибок передачи данных агента\n");
        sb.append("# TYPE replicator_agent_errors_total counter\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_agent_errors_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.transferErrors()));
        }

        // 7. Сжатие трафика (Wire Compression): replicator_compression_*
        long totalRaw = 0;
        long totalComp = 0;
        for (var a : agents) {
            totalRaw += a.compressionRawBytes();
            totalComp += a.compressionCompressedBytes();
        }
        double globalRatio = (totalRaw > 0 && totalComp > 0) ? ((double) totalRaw / Math.max(1, totalComp)) : 1.0;

        sb.append("# HELP replicator_compression_raw_bytes_total Суммарный объем исходных данных до сжатия в байтах\n");
        sb.append("# TYPE replicator_compression_raw_bytes_total counter\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_compression_raw_bytes_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.compressionRawBytes()));
        }

        sb.append("# HELP replicator_compression_compressed_bytes_total Суммарный объем сжатых данных в канале передачи в байтах\n");
        sb.append("# TYPE replicator_compression_compressed_bytes_total counter\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_compression_compressed_bytes_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.compressionCompressedBytes()));
        }

        sb.append("# HELP replicator_compression_ratio Эффективный коэффициент сжатия трафика репликации (raw_bytes / compressed_bytes)\n");
        sb.append("# TYPE replicator_compression_ratio gauge\n");
        sb.append(String.format(Locale.US, "replicator_compression_ratio{cluster_id=\"all\"} %.2f\n", globalRatio));
        for (var a : agents) {
            double ratio = a.compressionRatio() > 0 ? a.compressionRatio() : 1.0;
            sb.append(String.format(Locale.US, "replicator_compression_ratio{agent_id=\"%s\",cluster_id=\"%s\"} %.2f\n",
                    escape(a.agentId()), escape(a.clusterId()), ratio));
        }

        // 8. Задержка передачи gRPC чанков: replicator_grpc_chunk_transfer_latency_*
        long totalChunks = 0;
        double sumLatencySec = 0.0;
        for (var a : agents) {
            totalChunks += a.chunkTransferCount();
            sumLatencySec += (a.chunkLatencyAvgMs() * a.chunkTransferCount()) / 1000.0;
        }

        sb.append("# HELP replicator_grpc_chunk_transfer_latency_avg_ms Средняя задержка передачи чанка данных через gRPC (мс)\n");
        sb.append("# TYPE replicator_grpc_chunk_transfer_latency_avg_ms gauge\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_grpc_chunk_transfer_latency_avg_ms{agent_id=\"%s\",cluster_id=\"%s\"} %.3f\n",
                    escape(a.agentId()), escape(a.clusterId()), a.chunkLatencyAvgMs()));
        }

        sb.append("# HELP replicator_grpc_chunk_transfer_latency_max_ms Максимальная задержка передачи чанка данных через gRPC (мс)\n");
        sb.append("# TYPE replicator_grpc_chunk_transfer_latency_max_ms gauge\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_grpc_chunk_transfer_latency_max_ms{agent_id=\"%s\",cluster_id=\"%s\"} %.3f\n",
                    escape(a.agentId()), escape(a.clusterId()), a.chunkLatencyMaxMs()));
        }

        sb.append("# HELP replicator_grpc_chunk_transfer_latency_seconds Гистограмма задержки передачи чанка данных через gRPC канал\n");
        sb.append("# TYPE replicator_grpc_chunk_transfer_latency_seconds histogram\n");
        double[] slaBuckets = {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0};
        for (double b : slaBuckets) {
            long bucketCount = 0;
            for (var a : agents) {
                if (a.chunkTransferCount() > 0) {
                    double avgSec = a.chunkLatencyAvgMs() / 1000.0;
                    if (avgSec <= b) {
                        bucketCount += a.chunkTransferCount();
                    }
                }
            }
            sb.append(String.format(Locale.US, "replicator_grpc_chunk_transfer_latency_seconds_bucket{le=\"%s\"} %d\n",
                    formatBucket(b), bucketCount));
        }
        sb.append(String.format(Locale.US, "replicator_grpc_chunk_transfer_latency_seconds_bucket{le=\"+Inf\"} %d\n", totalChunks));
        sb.append(String.format(Locale.US, "replicator_grpc_chunk_transfer_latency_seconds_sum %.4f\n", sumLatencySec));
        sb.append(String.format(Locale.US, "replicator_grpc_chunk_transfer_latency_seconds_count %d\n", totalChunks));

        // 9. Очередь и файлы Staging: replicator_staging_*
        int totalStagingFiles = 0;
        long totalStagingBytes = 0;
        long totalOrphans = 0;
        for (var a : agents) {
            totalStagingFiles += a.stagingActiveFiles();
            totalStagingBytes += a.stagingActiveBytes();
            totalOrphans += a.stagingOrphansCleaned();
        }

        sb.append("# HELP replicator_staging_queue_size Совокупное количество активных staging-файлов в процессе репликации\n");
        sb.append("# TYPE replicator_staging_queue_size gauge\n");
        sb.append(String.format(Locale.US, "replicator_staging_queue_size %d\n", totalStagingFiles));

        sb.append("# HELP replicator_staging_active_files Количество активных файлов в процессе staging по агентам\n");
        sb.append("# TYPE replicator_staging_active_files gauge\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_staging_active_files{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.stagingActiveFiles()));
        }

        sb.append("# HELP replicator_staging_active_bytes Текущий совокупный объем данных в staging-буфере в байтах\n");
        sb.append("# TYPE replicator_staging_active_bytes gauge\n");
        sb.append(String.format(Locale.US, "replicator_staging_active_bytes %d\n", totalStagingBytes));
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_staging_active_bytes{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.stagingActiveBytes()));
        }

        sb.append("# HELP replicator_staging_orphans_cleaned_total Количество удаленных осиротевших staging-файлов сборщиком мусора\n");
        sb.append("# TYPE replicator_staging_orphans_cleaned_total counter\n");
        sb.append(String.format(Locale.US, "replicator_staging_orphans_cleaned_total %d\n", totalOrphans));
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_staging_orphans_cleaned_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.stagingOrphansCleaned()));
        }
    }

    private String formatBucket(double b) {
        if (b == (long) b) {
            return String.format(Locale.US, "%.1f", b);
        }
        return String.valueOf(b);
    }

    private double calculateBytesByStatus(String status) {
        try {
            List<JobEntity> jobs = jobRepository.findAll();
            return jobs.stream()
                    .filter(j -> j.getParentJobId() == null)
                    .filter(j -> status.equalsIgnoreCase(j.getStatus()))
                    .mapToLong(j -> "COMPLETED".equalsIgnoreCase(status) ? j.getTotalBytes() : j.getCopiedBytes())
                    .sum();
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double countAgentsByStatus(String status) {
        try {
            List<AgentResponseDto> agents = agentRegistry.getAgentDtos();
            return agents.stream().filter(a -> status.equalsIgnoreCase(a.status())).count();
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double countTotalAgents() {
        try {
            return agentRegistry.getAgentDtos().size();
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double countJobsByStatus(String status) {
        try {
            return jobRepository.countByStatusAndParentJobIdIsNull(status);
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double countJobsByScheduled(boolean scheduled) {
        try {
            List<JobEntity> jobs = jobRepository.findAll();
            return jobs.stream()
                    .filter(j -> j.getParentJobId() == null)
                    .filter(j -> j.isScheduled() == scheduled)
                    .count();
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double countTasksByStatus(String status) {
        try {
            return taskRepository.countByStatus(status);
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double countHmsJobsByStatus(String status) {
        try {
            return hmsReplicationJobRepository.countByStatus(status);
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double calculateHmsTables(boolean replicated) {
        try {
            List<HmsReplicationJobEntity> jobs = hmsReplicationJobRepository.findAll();
            return jobs.stream()
                    .mapToInt(j -> replicated ? j.getReplicatedTables() : j.getTotalTables())
                    .sum();
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double calculateHmsPartitions(boolean replicated) {
        try {
            List<HmsReplicationJobEntity> jobs = hmsReplicationJobRepository.findAll();
            return jobs.stream()
                    .mapToInt(j -> replicated ? j.getReplicatedPartitions() : j.getTotalPartitions())
                    .sum();
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double calculateHmsBootstrapSpeedBytesPerSec() {
        try {
            List<HmsReplicationJobEntity> jobs = hmsReplicationJobRepository.findByStatus("BOOTSTRAPPING");
            double totalMbS = 0.0;
            int count = 0;
            for (var job : jobs) {
                if (job.getAverageSpeedMbS() != null && job.getAverageSpeedMbS() > 0) {
                    totalMbS += job.getAverageSpeedMbS();
                    count++;
                }
            }
            return count > 0 ? (totalMbS * 1024.0 * 1024.0) : 0.0;
        } catch (Exception e) {
            return 0.0;
        }
    }

    private boolean isClusterFenced(String clusterId) {
        try {
            DrStatusResponse drStatus = disasterRecoveryService.getDrStatus();
            if (drStatus != null && drStatus.datacenters() != null) {
                for (var dc : drStatus.datacenters()) {
                    if (clusterId.equalsIgnoreCase(dc.id()) && (dc.isFenced() || dc.bandwidthLimitMbS() == 0.0)) {
                        return true;
                    }
                }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    private double countReverseJobs(String status) {
        try {
            List<JobEntity> jobs = jobRepository.findAll();
            return jobs.stream()
                    .filter(j -> j.getSourcePath() != null && j.getSourcePath().startsWith("/backup/"))
                    .filter(j -> status.equalsIgnoreCase(j.getStatus()))
                    .count();
        } catch (Exception e) {
            return 0.0;
        }
    }

    private void renderKafkaMetrics(StringBuilder sb) {
        if (kafkaReplicationJobRepository == null) {
            return;
        }
        try {
            List<KafkaReplicationJobEntity> jobs = kafkaReplicationJobRepository.findAll();

            long activeCount = 0;
            long pausedCount = 0;
            long totalMessages = 0;
            long totalBytes = 0;
            long maxLagMs = 0;

            for (KafkaReplicationJobEntity j : jobs) {
                if ("ACTIVE".equalsIgnoreCase(j.getStatus())) {
                    activeCount++;
                } else if ("PAUSED".equalsIgnoreCase(j.getStatus())) {
                    pausedCount++;
                }
                long msgs = j.getMessagesReplicated() != null ? j.getMessagesReplicated() : 0L;
                long bytes = j.getBytesReplicated() != null ? j.getBytesReplicated() : 0L;
                long lag = j.getCurrentLagMs() != null ? j.getCurrentLagMs() : 0L;

                totalMessages += msgs;
                totalBytes += bytes;
                if (lag > maxLagMs) {
                    maxLagMs = lag;
                }
            }

            // 1. Статусы задач
            sb.append("# HELP replicator_kafka_jobs_total Количество задач репликации Kafka по статусам\n");
            sb.append("# TYPE replicator_kafka_jobs_total gauge\n");
            sb.append(String.format(Locale.US, "replicator_kafka_jobs_total{status=\"ACTIVE\"} %d\n", activeCount));
            sb.append(String.format(Locale.US, "replicator_kafka_jobs_total{status=\"PAUSED\"} %d\n", pausedCount));
            sb.append(String.format(Locale.US, "replicator_kafka_jobs_total{status=\"TOTAL\"} %d\n", jobs.size()));

            // 2. Агрегированные счетчики (гармонизированный стандарт Prometheus & Grafana)
            sb.append("# HELP kafka_replicated_messages_total Суммарное количество сообщений Kafka, переданных через gRPC\n");
            sb.append("# TYPE kafka_replicated_messages_total counter\n");
            sb.append(String.format(Locale.US, "kafka_replicated_messages_total %d\n", totalMessages));

            sb.append("# HELP kafka_replicated_bytes_total Суммарный объем байт данных Kafka, переданных через gRPC\n");
            sb.append("# TYPE kafka_replicated_bytes_total counter\n");
            sb.append(String.format(Locale.US, "kafka_replicated_bytes_total %d\n", totalBytes));

            sb.append("# HELP kafka_current_lag_ms Максимальный текущий сквозной лаг репликации Kafka (мс)\n");
            sb.append("# TYPE kafka_current_lag_ms gauge\n");
            sb.append(String.format(Locale.US, "kafka_current_lag_ms %d\n", maxLagMs));

            // 3. Детальные метрики по каждой задаче репликации Kafka
            if (!jobs.isEmpty()) {
                sb.append("# HELP replicator_kafka_job_messages_total Передано сообщений по задаче репликации Kafka\n");
                sb.append("# TYPE replicator_kafka_job_messages_total counter\n");
                for (KafkaReplicationJobEntity j : jobs) {
                    long msgs = j.getMessagesReplicated() != null ? j.getMessagesReplicated() : 0L;
                    sb.append(String.format(Locale.US, "replicator_kafka_job_messages_total{job_id=\"%s\",job_name=\"%s\",source_dc=\"%s\",target_dc=\"%s\",status=\"%s\"} %d\n",
                            escape(j.getId()), escape(j.getName()), escape(j.getSourceDc()), escape(j.getTargetDc()), escape(j.getStatus()), msgs));
                }

                sb.append("# HELP replicator_kafka_job_bytes_total Передано байт по задаче репликации Kafka\n");
                sb.append("# TYPE replicator_kafka_job_bytes_total counter\n");
                for (KafkaReplicationJobEntity j : jobs) {
                    long bytes = j.getBytesReplicated() != null ? j.getBytesReplicated() : 0L;
                    sb.append(String.format(Locale.US, "replicator_kafka_job_bytes_total{job_id=\"%s\",job_name=\"%s\",source_dc=\"%s\",target_dc=\"%s\",status=\"%s\"} %d\n",
                            escape(j.getId()), escape(j.getName()), escape(j.getSourceDc()), escape(j.getTargetDc()), escape(j.getStatus()), bytes));
                }

                sb.append("# HELP replicator_kafka_job_lag_ms Текущий сквозной лаг репликации по задаче Kafka (мс)\n");
                sb.append("# TYPE replicator_kafka_job_lag_ms gauge\n");
                for (KafkaReplicationJobEntity j : jobs) {
                    long lag = j.getCurrentLagMs() != null ? j.getCurrentLagMs() : 0L;
                    sb.append(String.format(Locale.US, "replicator_kafka_job_lag_ms{job_id=\"%s\",job_name=\"%s\",source_dc=\"%s\",target_dc=\"%s\",status=\"%s\"} %d\n",
                            escape(j.getId()), escape(j.getName()), escape(j.getSourceDc()), escape(j.getTargetDc()), escape(j.getStatus()), lag));
                }

                sb.append("# HELP replicator_kafka_bandwidth_limit_mb Выделенный лимит полосы пропускания WAN для задачи Kafka (МБ/с)\n");
                sb.append("# TYPE replicator_kafka_bandwidth_limit_mb gauge\n");
                for (KafkaReplicationJobEntity j : jobs) {
                    double bw = j.getBandwidthLimitMb() != null ? j.getBandwidthLimitMb() : 0.0;
                    sb.append(String.format(Locale.US, "replicator_kafka_bandwidth_limit_mb{job_id=\"%s\",job_name=\"%s\",source_dc=\"%s\",target_dc=\"%s\"} %.2f\n",
                            escape(j.getId()), escape(j.getName()), escape(j.getSourceDc()), escape(j.getTargetDc()), bw));
                }
            }
        } catch (Exception e) {
            log.error("Ошибка при генерации метрик Kafka репликации: {}", e.getMessage(), e);
        }
    }

    private String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "");
    }

    /** Агрегаты JPA приходят как Number или null. */
    private static long toLong(Object value) {
        return (value instanceof Number number) ? number.longValue() : 0L;
    }
}
