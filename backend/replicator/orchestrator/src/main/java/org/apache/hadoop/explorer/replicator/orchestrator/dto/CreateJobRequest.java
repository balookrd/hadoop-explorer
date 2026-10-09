package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record CreateJobRequest(
    @NotBlank(message = "source_path cannot be blank")
    @JsonProperty("source_path")
    String sourcePath,

    @NotBlank(message = "target_path cannot be blank")
    @JsonProperty("target_path")
    String targetPath,

    @JsonProperty("source_cluster_id")
    String sourceClusterId,

    @JsonProperty("target_cluster_id")
    String targetClusterId,

    @JsonProperty("total_bytes")
    Long totalBytes,

    @JsonProperty("execution_principal")
    String executionPrincipal,

    @JsonProperty("run_as_service_account")
    Boolean runAsServiceAccount,

    @JsonProperty("is_scheduled")
    Boolean isScheduled,

    @JsonProperty("cron_expression")
    String cronExpression,

    @JsonProperty("history_retention_runs")
    Integer historyRetentionRuns,

    @JsonProperty("job_type")
    String jobType,

    @JsonProperty("parent_job_id")
    String parentJobId
) {
    public CreateJobRequest {
        if (sourceClusterId == null) sourceClusterId = "dc1";
        if (targetClusterId == null) targetClusterId = "dc2";
        if (totalBytes == null) totalBytes = 0L;
        if (executionPrincipal == null) executionPrincipal = "hdfs@EXAMPLE.COM";
        if (runAsServiceAccount == null) runAsServiceAccount = true;
        if (isScheduled == null) isScheduled = false;
        if (historyRetentionRuns == null) historyRetentionRuns = 20;
        if (jobType == null) jobType = "STANDARD";
    }

    public CreateJobRequest(
        String sourcePath,
        String targetPath,
        String sourceClusterId,
        String targetClusterId,
        Long totalBytes,
        String executionPrincipal,
        Boolean runAsServiceAccount,
        Boolean isScheduled,
        String cronExpression
    ) {
        this(sourcePath, targetPath, sourceClusterId, targetClusterId, totalBytes, executionPrincipal, runAsServiceAccount, isScheduled, cronExpression, 20, "STANDARD", null);
    }
}
