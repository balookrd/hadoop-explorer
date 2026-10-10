package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record DrActionResponse(
    @JsonProperty("success") boolean success,
    @JsonProperty("message") String message,
    @JsonProperty("stopped_hdfs_jobs") int stoppedHdfsJobs,
    @JsonProperty("stopped_hms_jobs") int stoppedHmsJobs,
    @JsonProperty("reversed_hdfs_jobs") int reversedHdfsJobs,
    @JsonProperty("reversed_hms_jobs") int reversedHmsJobs,
    @JsonProperty("created_job_ids") List<String> createdJobIds
) {
    public static DrActionResponse error(String message) {
        return new DrActionResponse(false, message, 0, 0, 0, 0, List.of());
    }

    public static DrActionResponse stopSuccess(String message, int stoppedHdfs, int stoppedHms) {
        return new DrActionResponse(true, message, stoppedHdfs, stoppedHms, 0, 0, List.of());
    }

    public static DrActionResponse reverseSuccess(String message, int reversedHdfs, int reversedHms, List<String> createdIds) {
        return new DrActionResponse(true, message, 0, 0, reversedHdfs, reversedHms, createdIds);
    }
}
