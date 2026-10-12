package org.apache.hadoop.explorer.replicator.orchestrator.metrics;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.AgentResponseDto;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrStatusResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.TaskRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.DisasterRecoveryService;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 */
@Service
public class ReplicatorMetricsService {

    private static final Logger log = LoggerFactory.getLogger(ReplicatorMetricsService.class);

    private final JobRepository jobRepository;
    private final TaskRepository taskRepository;
    private final HmsReplicationJobRepository hmsReplicationJobRepository;
    private final AgentRegistry agentRegistry;
    private final TokenBucketThrottler throttler;
    private final DisasterRecoveryService disasterRecoveryService;

    // Внутренний счетчик накопленной задержки троттлинга
    private final AtomicLong accumulatedThrottlingNanos = new AtomicLong(0);

    public ReplicatorMetricsService(
            JobRepository jobRepository,
            TaskRepository taskRepository,
            HmsReplicationJobRepository hmsReplicationJobRepository,
            AgentRegistry agentRegistry,
            TokenBucketThrottler throttler,
            DisasterRecoveryService disasterRecoveryService
    ) {
        this.jobRepository = jobRepository;
        this.taskRepository = taskRepository;
        this.hmsReplicationJobRepository = hmsReplicationJobRepository;
        this.agentRegistry = agentRegistry;
        this.throttler = throttler;
        this.disasterRecoveryService = disasterRecoveryService;
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
            // 1. replicator_bytes_total (и legacy replication_bytes_total)
            double bytesCompleted = calculateBytesByStatus("COMPLETED");
            double bytesRunning = calculateBytesByStatus("RUNNING");
            double bytesFailed = calculateBytesByStatus("FAILED");

            sb.append("# HELP replicator_bytes_total Суммарный объем переданных байт данных репликации\n");
            sb.append("# TYPE replicator_bytes_total counter\n");
            sb.append(String.format(Locale.US, "replicator_bytes_total{status=\"COMPLETED\"} %.0f\n", bytesCompleted));
            sb.append(String.format(Locale.US, "replicator_bytes_total{status=\"IN_PROGRESS\"} %.0f\n", bytesRunning));
            sb.append(String.format(Locale.US, "replicator_bytes_total{status=\"FAILED\"} %.0f\n", bytesFailed));

            sb.append("# HELP replication_bytes_total Суммарный объем переданных байт данных репликации (legacy alias)\n");
            sb.append("# TYPE replication_bytes_total counter\n");
            sb.append(String.format(Locale.US, "replication_bytes_total{status=\"COMPLETED\"} %.0f\n", bytesCompleted));
            sb.append(String.format(Locale.US, "replication_bytes_total{status=\"IN_PROGRESS\"} %.0f\n", bytesRunning));
            sb.append(String.format(Locale.US, "replication_bytes_total{status=\"FAILED\"} %.0f\n", bytesFailed));

            // 2. replicator_active_workers & replicator_active_agents (и legacy active_workers / active_agents)
            double onlineAgents = countAgentsByStatus("online");
            double totalAgents = countTotalAgents();

            sb.append("# HELP replicator_active_workers Количество активных онлайн-воркеров репликации\n");
            sb.append("# TYPE replicator_active_workers gauge\n");
            sb.append(String.format(Locale.US, "replicator_active_workers %.0f\n", onlineAgents));

            sb.append("# HELP active_workers Количество активных онлайн-воркеров репликации (legacy alias)\n");
            sb.append("# TYPE active_workers gauge\n");
            sb.append(String.format(Locale.US, "active_workers %.0f\n", onlineAgents));

            sb.append("# HELP replicator_active_agents Количество доступных агентов репликации\n");
            sb.append("# TYPE replicator_active_agents gauge\n");
            sb.append(String.format(Locale.US, "replicator_active_agents{status=\"online\"} %.0f\n", onlineAgents));
            sb.append(String.format(Locale.US, "replicator_active_agents{status=\"total\"} %.0f\n", totalAgents));

            sb.append("# HELP active_agents Количество доступных агентов репликации (legacy alias)\n");
            sb.append("# TYPE active_agents gauge\n");
            sb.append(String.format(Locale.US, "active_agents{status=\"online\"} %.0f\n", onlineAgents));
            sb.append(String.format(Locale.US, "active_agents{status=\"total\"} %.0f\n", totalAgents));

            // 3. Throttling metrics: replicator_throttling_* (и legacy throttling_*)
            double waitSec = accumulatedThrottlingNanos.get() / 1_000_000_000.0;
            double globalLimit = throttler.getGlobalLimit();

            sb.append("# HELP replicator_throttling_delay_seconds_total Суммарная накопленная задержка из-за ограничения полосы Token Bucket Throttler\n");
            sb.append("# TYPE replicator_throttling_delay_seconds_total counter\n");
            sb.append(String.format(Locale.US, "replicator_throttling_delay_seconds_total %.4f\n", waitSec));

            sb.append("# HELP throttling_delay_seconds_total Суммарная накопленная задержка из-за ограничения полосы Token Bucket Throttler (legacy alias)\n");
            sb.append("# TYPE throttling_delay_seconds_total counter\n");
            sb.append(String.format(Locale.US, "throttling_delay_seconds_total %.4f\n", waitSec));

            sb.append("# HELP replicator_throttling_delay_seconds Накопленная задержка из-за ограничения полосы Token Bucket\n");
            sb.append("# TYPE replicator_throttling_delay_seconds gauge\n");
            sb.append(String.format(Locale.US, "replicator_throttling_delay_seconds %.4f\n", waitSec));

            sb.append("# HELP throttling_delay_seconds Накопленная задержка из-за ограничения полосы Token Bucket (legacy alias)\n");
            sb.append("# TYPE throttling_delay_seconds gauge\n");
            sb.append(String.format(Locale.US, "throttling_delay_seconds %.4f\n", waitSec));

            sb.append("# HELP replicator_throttling_global_limit_bytes_per_sec Текущий глобальный лимит пропускной способности репликации\n");
            sb.append("# TYPE replicator_throttling_global_limit_bytes_per_sec gauge\n");
            sb.append(String.format(Locale.US, "replicator_throttling_global_limit_bytes_per_sec %.0f\n", globalLimit));

            sb.append("# HELP throttling_global_limit_bytes_per_sec Текущий глобальный лимит пропускной способности репликации (legacy alias)\n");
            sb.append("# TYPE throttling_global_limit_bytes_per_sec gauge\n");
            sb.append(String.format(Locale.US, "throttling_global_limit_bytes_per_sec %.0f\n", globalLimit));

            // 4. replicator_jobs_total (и legacy replication_jobs_total)
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

            sb.append("# HELP replication_jobs_total Количество задач репликации HDFS по статусам и режимам (legacy alias)\n");
            sb.append("# TYPE replication_jobs_total gauge\n");
            sb.append(String.format(Locale.US, "replication_jobs_total{status=\"RUNNING\"} %.0f\n", runningJobs));
            sb.append(String.format(Locale.US, "replication_jobs_total{status=\"QUEUED\"} %.0f\n", queuedJobs));
            sb.append(String.format(Locale.US, "replication_jobs_total{status=\"COMPLETED\"} %.0f\n", completedJobs));
            sb.append(String.format(Locale.US, "replication_jobs_total{status=\"FAILED\"} %.0f\n", failedJobs));
            sb.append(String.format(Locale.US, "replication_jobs_total{status=\"SCHEDULED\",mode=\"scheduled\"} %.0f\n", scheduledJobs));
            sb.append(String.format(Locale.US, "replication_jobs_total{status=\"MANUAL\",mode=\"manual\"} %.0f\n", manualJobs));

            // 5. replicator_tasks_total (и legacy replication_tasks_total)
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

            sb.append("# HELP replication_tasks_total Количество пофайловых задач в распределенном пуле (legacy alias)\n");
            sb.append("# TYPE replication_tasks_total gauge\n");
            sb.append(String.format(Locale.US, "replication_tasks_total{status=\"QUEUED\"} %.0f\n", queuedTasks));
            sb.append(String.format(Locale.US, "replication_tasks_total{status=\"RUNNING\"} %.0f\n", runningTasks));
            sb.append(String.format(Locale.US, "replication_tasks_total{status=\"COMPLETED\"} %.0f\n", completedTasks));
            sb.append(String.format(Locale.US, "replication_tasks_total{status=\"FAILED\"} %.0f\n", failedTasks));

            // 6. Гистограмма распределения размеров файлов: replicator_file_size_bytes & replication_file_size_bytes
            renderFileSizeMetrics(sb);

            // 7. HMS Replication metrics: replicator_hms_* & hms_replication_*
            renderHmsMetrics(sb);

            // 8. Disaster Recovery metrics: replicator_dr_* & dr_*
            renderDrMetrics(sb);

            // 9. Детальные метрики агентов: replicator_agent_* & agent_*
            renderAgentMetrics(sb);

        } catch (Exception e) {
            log.error("Ошибка при генерации метрик Prometheus: {}", e.getMessage(), e);
        }

        return sb.toString();
    }

    private void renderFileSizeMetrics(StringBuilder sb) {
        try {
            List<org.apache.hadoop.explorer.replicator.orchestrator.entity.TaskEntity> tasks = taskRepository.findAll();
            long b1M = 0, b10M = 0, b100M = 0, b1G = 0, b10G = 0, count = 0, sum = 0;
            for (var t : tasks) {
                long size = t.getFileSize();
                sum += size;
                count++;
                if (size <= 1048576L) b1M++;
                if (size <= 10485760L) b10M++;
                if (size <= 104857600L) b100M++;
                if (size <= 1073741824L) b1G++;
                if (size <= 10737418240L) b10G++;
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

            sb.append("# HELP replication_file_size_bytes Распределение размеров файлов репликации (legacy alias)\n");
            sb.append("# TYPE replication_file_size_bytes histogram\n");
            sb.append(String.format(Locale.US, "replication_file_size_bytes_bucket{le=\"1048576\"} %d\n", b1M));
            sb.append(String.format(Locale.US, "replication_file_size_bytes_bucket{le=\"10485760\"} %d\n", b10M));
            sb.append(String.format(Locale.US, "replication_file_size_bytes_bucket{le=\"104857600\"} %d\n", b100M));
            sb.append(String.format(Locale.US, "replication_file_size_bytes_bucket{le=\"1073741824\"} %d\n", b1G));
            sb.append(String.format(Locale.US, "replication_file_size_bytes_bucket{le=\"10737418240\"} %d\n", b10G));
            sb.append(String.format(Locale.US, "replication_file_size_bytes_bucket{le=\"+Inf\"} %d\n", count));
            sb.append(String.format(Locale.US, "replication_file_size_bytes_sum %d\n", sum));
            sb.append(String.format(Locale.US, "replication_file_size_bytes_count %d\n", count));
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

        sb.append("# HELP hms_replication_jobs_total Количество задач репликации Hive Metastore по статусам (legacy alias)\n");
        sb.append("# TYPE hms_replication_jobs_total gauge\n");
        sb.append(String.format(Locale.US, "hms_replication_jobs_total{status=\"BOOTSTRAPPING\"} %.0f\n", bootstrapping));
        sb.append(String.format(Locale.US, "hms_replication_jobs_total{status=\"STREAMING\"} %.0f\n", streaming));
        sb.append(String.format(Locale.US, "hms_replication_jobs_total{status=\"WAITING_FOR_AGENTS\"} %.0f\n", waiting));
        sb.append(String.format(Locale.US, "hms_replication_jobs_total{status=\"COMPLETED\"} %.0f\n", completed));
        sb.append(String.format(Locale.US, "hms_replication_jobs_total{status=\"FAILED\"} %.0f\n", failed));

        double totalTables = calculateHmsTables(false);
        double repTables = calculateHmsTables(true);
        sb.append("# HELP replicator_hms_tables_total Количество таблиц в схемах HMS\n");
        sb.append("# TYPE replicator_hms_tables_total gauge\n");
        sb.append(String.format(Locale.US, "replicator_hms_tables_total{type=\"total\"} %.0f\n", totalTables));
        sb.append(String.format(Locale.US, "replicator_hms_tables_total{type=\"replicated\"} %.0f\n", repTables));

        sb.append("# HELP hms_replication_tables_total Количество таблиц в схемах HMS (legacy alias)\n");
        sb.append("# TYPE hms_replication_tables_total gauge\n");
        sb.append(String.format(Locale.US, "hms_replication_tables_total{type=\"total\"} %.0f\n", totalTables));
        sb.append(String.format(Locale.US, "hms_replication_tables_total{type=\"replicated\"} %.0f\n", repTables));

        double totalPartitions = calculateHmsPartitions(false);
        double repPartitions = calculateHmsPartitions(true);
        sb.append("# HELP replicator_hms_partitions_total Количество партиций таблиц HMS\n");
        sb.append("# TYPE replicator_hms_partitions_total gauge\n");
        sb.append(String.format(Locale.US, "replicator_hms_partitions_total{type=\"total\"} %.0f\n", totalPartitions));
        sb.append(String.format(Locale.US, "replicator_hms_partitions_total{type=\"replicated\"} %.0f\n", repPartitions));

        sb.append("# HELP hms_replication_partitions_total Количество партиций таблиц HMS (legacy alias)\n");
        sb.append("# TYPE hms_replication_partitions_total gauge\n");
        sb.append(String.format(Locale.US, "hms_replication_partitions_total{type=\"total\"} %.0f\n", totalPartitions));
        sb.append(String.format(Locale.US, "hms_replication_partitions_total{type=\"replicated\"} %.0f\n", repPartitions));

        double speed = calculateHmsBootstrapSpeedBytesPerSec();
        sb.append("# HELP replicator_hms_speed_bytes_per_second Текущая средняя скорость первичной синхронизации HMS\n");
        sb.append("# TYPE replicator_hms_speed_bytes_per_second gauge\n");
        sb.append(String.format(Locale.US, "replicator_hms_speed_bytes_per_second %.2f\n", speed));

        sb.append("# HELP hms_replication_speed_bytes_per_second Текущая средняя скорость первичной синхронизации HMS (legacy alias)\n");
        sb.append("# TYPE hms_replication_speed_bytes_per_second gauge\n");
        sb.append(String.format(Locale.US, "hms_replication_speed_bytes_per_second %.2f\n", speed));
    }

    private void renderDrMetrics(StringBuilder sb) {
        double fencedDc1 = isClusterFenced("dc1") ? 1.0 : 0.0;
        double fencedDc2 = isClusterFenced("dc2") ? 1.0 : 0.0;

        sb.append("# HELP replicator_dr_cluster_fenced Статус изоляции кластера (1 = Fenced / Kill-Switch активирован, 0 = Active)\n");
        sb.append("# TYPE replicator_dr_cluster_fenced gauge\n");
        sb.append(String.format(Locale.US, "replicator_dr_cluster_fenced{cluster_id=\"dc1\"} %.1f\n", fencedDc1));
        sb.append(String.format(Locale.US, "replicator_dr_cluster_fenced{cluster_id=\"dc2\"} %.1f\n", fencedDc2));

        sb.append("# HELP dr_cluster_fenced Статус изоляции кластера (1 = Fenced / Kill-Switch активирован, 0 = Active) (legacy alias)\n");
        sb.append("# TYPE dr_cluster_fenced gauge\n");
        sb.append(String.format(Locale.US, "dr_cluster_fenced{cluster_id=\"dc1\"} %.1f\n", fencedDc1));
        sb.append(String.format(Locale.US, "dr_cluster_fenced{cluster_id=\"dc2\"} %.1f\n", fencedDc2));

        double revRunning = countReverseJobs("RUNNING");
        double revCompleted = countReverseJobs("COMPLETED");

        sb.append("# HELP replicator_dr_reverse_jobs_total Количество задач обратной репликации при Failover\n");
        sb.append("# TYPE replicator_dr_reverse_jobs_total gauge\n");
        sb.append(String.format(Locale.US, "replicator_dr_reverse_jobs_total{status=\"RUNNING\"} %.0f\n", revRunning));
        sb.append(String.format(Locale.US, "replicator_dr_reverse_jobs_total{status=\"COMPLETED\"} %.0f\n", revCompleted));

        sb.append("# HELP dr_reverse_jobs_total Количество задач обратной репликации при Failover (legacy alias)\n");
        sb.append("# TYPE dr_reverse_jobs_total gauge\n");
        sb.append(String.format(Locale.US, "dr_reverse_jobs_total{status=\"RUNNING\"} %.0f\n", revRunning));
        sb.append(String.format(Locale.US, "dr_reverse_jobs_total{status=\"COMPLETED\"} %.0f\n", revCompleted));
    }

    private void renderAgentMetrics(StringBuilder sb) {
        List<AgentResponseDto> agents = agentRegistry.getAgentDtos();
        if (agents.isEmpty()) {
            return;
        }

        // 1. replicator_agent_status & agent_status
        sb.append("# HELP replicator_agent_status Статус доступности агента (1 = online, 0 = offline/stale)\n");
        sb.append("# TYPE replicator_agent_status gauge\n");
        for (var a : agents) {
            double val = "online".equalsIgnoreCase(a.status()) ? 1.0 : 0.0;
            sb.append(String.format(Locale.US, "replicator_agent_status{agent_id=\"%s\",cluster_id=\"%s\",mode=\"%s\",status=\"%s\"} %.1f\n",
                    escape(a.agentId()), escape(a.clusterId()), escape(a.mode()), escape(a.status()), val));
        }

        sb.append("# HELP agent_status Статус доступности агента (1 = online, 0 = offline/stale) (legacy alias)\n");
        sb.append("# TYPE agent_status gauge\n");
        for (var a : agents) {
            double val = "online".equalsIgnoreCase(a.status()) ? 1.0 : 0.0;
            sb.append(String.format(Locale.US, "agent_status{agent_id=\"%s\",cluster_id=\"%s\",mode=\"%s\",status=\"%s\"} %.1f\n",
                    escape(a.agentId()), escape(a.clusterId()), escape(a.mode()), escape(a.status()), val));
        }

        // 2. replicator_agent_active_transfers & agent_active_transfers
        sb.append("# HELP replicator_agent_active_transfers Текущее количество параллельных передач на агенте\n");
        sb.append("# TYPE replicator_agent_active_transfers gauge\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_agent_active_transfers{agent_id=\"%s\",cluster_id=\"%s\",mode=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), escape(a.mode()), a.activeTransfers()));
        }

        sb.append("# HELP agent_active_transfers Текущее количество параллельных передач на агенте (legacy alias)\n");
        sb.append("# TYPE agent_active_transfers gauge\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "agent_active_transfers{agent_id=\"%s\",cluster_id=\"%s\",mode=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), escape(a.mode()), a.activeTransfers()));
        }

        // 3. replicator_agent_heartbeat_age_seconds & agent_heartbeat_age_seconds
        sb.append("# HELP replicator_agent_heartbeat_age_seconds Время с момента последнего keepalive от агента\n");
        sb.append("# TYPE replicator_agent_heartbeat_age_seconds gauge\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_agent_heartbeat_age_seconds{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.heartbeatAgeSeconds()));
        }

        sb.append("# HELP agent_heartbeat_age_seconds Время с момента последнего keepalive от агента (legacy alias)\n");
        sb.append("# TYPE agent_heartbeat_age_seconds gauge\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "agent_heartbeat_age_seconds{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.heartbeatAgeSeconds()));
        }

        // 4. replicator_agent_max_bandwidth_mb_s & agent_max_bandwidth_mb_s
        sb.append("# HELP replicator_agent_max_bandwidth_mb_s Выделенный локальный лимит полосы пропускания HTB агента (МБ/с)\n");
        sb.append("# TYPE replicator_agent_max_bandwidth_mb_s gauge\n");
        for (var a : agents) {
            double bw = a.maxBandwidthMbS() != null ? a.maxBandwidthMbS() : 0.0;
            sb.append(String.format(Locale.US, "replicator_agent_max_bandwidth_mb_s{agent_id=\"%s\",cluster_id=\"%s\"} %.2f\n",
                    escape(a.agentId()), escape(a.clusterId()), bw));
        }

        sb.append("# HELP agent_max_bandwidth_mb_s Выделенный локальный лимит полосы пропускания HTB агента (МБ/с) (legacy alias)\n");
        sb.append("# TYPE agent_max_bandwidth_mb_s gauge\n");
        for (var a : agents) {
            double bw = a.maxBandwidthMbS() != null ? a.maxBandwidthMbS() : 0.0;
            sb.append(String.format(Locale.US, "agent_max_bandwidth_mb_s{agent_id=\"%s\",cluster_id=\"%s\"} %.2f\n",
                    escape(a.agentId()), escape(a.clusterId()), bw));
        }

        // 5. replicator_agent_transferred_bytes_total & agent_transferred_bytes_total
        sb.append("# HELP replicator_agent_transferred_bytes_total Суммарный объем байт данных переданных агентом\n");
        sb.append("# TYPE replicator_agent_transferred_bytes_total counter\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_agent_transferred_bytes_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.transferredBytes()));
        }

        sb.append("# HELP agent_transferred_bytes_total Суммарный объем байт данных переданных агентом (legacy alias)\n");
        sb.append("# TYPE agent_transferred_bytes_total counter\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "agent_transferred_bytes_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.transferredBytes()));
        }

        // 6. replicator_agent_errors_total & agent_errors_total (& Grafana replicator_agent_transfer_errors_total)
        sb.append("# HELP replicator_agent_errors_total Количество ошибок передачи данных агента\n");
        sb.append("# TYPE replicator_agent_errors_total counter\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_agent_errors_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.transferErrors()));
        }

        sb.append("# HELP replicator_agent_transfer_errors_total Количество ошибок передачи данных агента (Grafana alias)\n");
        sb.append("# TYPE replicator_agent_transfer_errors_total counter\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "replicator_agent_transfer_errors_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.transferErrors()));
        }

        sb.append("# HELP agent_errors_total Количество ошибок передачи данных агента (legacy alias)\n");
        sb.append("# TYPE agent_errors_total counter\n");
        for (var a : agents) {
            sb.append(String.format(Locale.US, "agent_errors_total{agent_id=\"%s\",cluster_id=\"%s\"} %d\n",
                    escape(a.agentId()), escape(a.clusterId()), a.transferErrors()));
        }
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

    private String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "");
    }
}
