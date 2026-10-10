package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record DrReverseRequest(
    @NotBlank(message = "from_cluster_id cannot be blank")
    @JsonProperty("from_cluster_id")
    String fromClusterId,

    @NotBlank(message = "to_cluster_id cannot be blank")
    @JsonProperty("to_cluster_id")
    String toClusterId,

    @JsonProperty("include_hdfs")
    Boolean includeHdfs,

    @JsonProperty("include_hms")
    Boolean includeHms,

    @JsonProperty("auto_start")
    Boolean autoStart
) {
    public DrReverseRequest {
        if (includeHdfs == null) includeHdfs = true;
        if (includeHms == null) includeHms = true;
        if (autoStart == null) autoStart = true;
    }
}
