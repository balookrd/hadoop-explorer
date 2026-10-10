package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record DrStatusResponse(
    @JsonProperty("datacenters") List<DrDcStatus> datacenters,
    @JsonProperty("clusters") List<DrClusterStatus> clusters,
    @JsonProperty("summary") DrSummary summary,
    @JsonProperty("hdfs_routes") List<DrRouteItem> hdfsRoutes,
    @JsonProperty("hms_routes") List<DrRouteItem> hmsRoutes
) {
    public record DrDcStatus(
        @JsonProperty("id") String id,
        @JsonProperty("name") String name,
        @JsonProperty("status") String status, // "ONLINE", "DEGRADED", "OFFLINE"
        @JsonProperty("role") String role, // "PRIMARY", "STANDBY", "PROMOTED_PRIMARY"
        @JsonProperty("online_agents") int onlineAgents,
        @JsonProperty("total_agents") int totalAgents,
        @JsonProperty("bandwidth_limit_mb_s") double bandwidthLimitMbS,
        @JsonProperty("is_fenced") boolean isFenced
    ) {}

    public record DrClusterStatus(
        @JsonProperty("id") String id,
        @JsonProperty("name") String name,
        @JsonProperty("dc_id") String dcId,
        @JsonProperty("status") String status, // "ONLINE", "OFFLINE"
        @JsonProperty("active_jobs_count") int activeJobsCount,
        @JsonProperty("queued_jobs_count") int queuedJobsCount,
        @JsonProperty("failed_jobs_count") int failedJobsCount,
        @JsonProperty("completed_jobs_count") int completedJobsCount
    ) {}

    public record DrSummary(
        @JsonProperty("active_source_dc") String activeSourceDc,
        @JsonProperty("active_target_dc") String activeTargetDc,
        @JsonProperty("primary_dc_online") boolean primaryDcOnline,
        @JsonProperty("standby_dc_online") boolean standbyDcOnline,
        @JsonProperty("total_active_jobs") int totalActiveJobs,
        @JsonProperty("total_frozen_jobs") int totalFrozenJobs,
        @JsonProperty("total_failed_jobs") int totalFailedJobs,
        @JsonProperty("unreplicated_bytes") long unreplicatedBytes,
        @JsonProperty("unreplicated_events") long unreplicatedEvents
    ) {}

    public record DrRouteItem(
        @JsonProperty("id") String id,
        @JsonProperty("type") String type, // "HDFS" или "HMS"
        @JsonProperty("name") String name,
        @JsonProperty("source_cluster_id") String sourceClusterId,
        @JsonProperty("target_cluster_id") String targetClusterId,
        @JsonProperty("source_path") String sourcePath,
        @JsonProperty("target_path") String targetPath,
        @JsonProperty("status") String status,
        @JsonProperty("is_scheduled") boolean isScheduled,
        @JsonProperty("cron_expression") String cronExpression,
        @JsonProperty("lag_bytes_or_events") long lagBytesOrEvents,
        @JsonProperty("has_reverse_job") boolean hasReverseJob,
        @JsonProperty("reverse_job_id") String reverseJobId
    ) {}
}
