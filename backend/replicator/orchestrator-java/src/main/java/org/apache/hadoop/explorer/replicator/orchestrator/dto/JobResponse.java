package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;

import java.time.Instant;

public record JobResponse(
    @JsonProperty("id") String id,
    @JsonProperty("source_path") String sourcePath,
    @JsonProperty("target_path") String targetPath,
    @JsonProperty("source_cluster_id") String sourceClusterId,
    @JsonProperty("target_cluster_id") String targetClusterId,
    @JsonProperty("status") String status,
    @JsonProperty("total_bytes") long totalBytes,
    @JsonProperty("copied_bytes") long copiedBytes,
    @JsonProperty("progress_percent") double progressPercent,
    @JsonProperty("average_speed_mb_s") Double averageSpeedMbS,
    @JsonProperty("run_as_service_account") boolean runAsServiceAccount,
    @JsonProperty("execution_principal") String executionPrincipal,
    @JsonProperty("created_by") String createdBy,
    @JsonProperty("is_scheduled") boolean isScheduled,
    @JsonProperty("cron_expression") String cronExpression,
    @JsonProperty("next_run_at") Instant nextRunAt,
    @JsonProperty("last_run_at") Instant lastRunAt,
    @JsonProperty("message") String message,
    @JsonProperty("created_at") Instant createdAt,
    @JsonProperty("updated_at") Instant updatedAt,
    @JsonProperty("started_at") Instant startedAt,
    @JsonProperty("completed_at") Instant completedAt,
    @JsonProperty("active_run_id") String activeRunId,
    @JsonProperty("history_retention_runs") int historyRetentionRuns,
    @JsonProperty("runs_count") int runsCount
) {
    public static JobResponse fromEntity(JobEntity entity) {
        return fromEntity(entity, 0);
    }

    public static JobResponse fromEntity(JobEntity entity, int runsCount) {
        return new JobResponse(
            entity.getId(),
            entity.getSourcePath(),
            entity.getTargetPath(),
            entity.getSourceClusterId(),
            entity.getTargetClusterId(),
            entity.getStatus(),
            entity.getTotalBytes(),
            entity.getCopiedBytes(),
            entity.getProgressPercent(),
            entity.getAverageSpeedMbS(),
            entity.isRunAsServiceAccount(),
            entity.getExecutionPrincipal(),
            entity.getCreatedBy(),
            entity.isScheduled(),
            entity.getCronExpression(),
            entity.getNextRunAt(),
            entity.getLastRunAt(),
            entity.getMessage(),
            entity.getCreatedAt(),
            entity.getUpdatedAt(),
            entity.getStartedAt(),
            entity.getCompletedAt(),
            entity.getActiveRunId(),
            entity.getHistoryRetentionRuns(),
            runsCount
        );
    }
}
