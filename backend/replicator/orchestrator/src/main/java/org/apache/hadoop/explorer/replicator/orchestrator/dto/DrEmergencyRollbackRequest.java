package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DrEmergencyRollbackRequest(
    @JsonProperty("cluster_id")
    String clusterId,

    @JsonProperty("restore_network")
    Boolean restoreNetwork,

    @JsonProperty("resume_hms")
    Boolean resumeHms,

    @JsonProperty("resume_hdfs")
    Boolean resumeHdfs
) {
    public DrEmergencyRollbackRequest {
        if (clusterId == null || clusterId.isBlank()) {
            clusterId = "dc1";
        }
        if (restoreNetwork == null) {
            restoreNetwork = true;
        }
        if (resumeHms == null) {
            resumeHms = true;
        }
        if (resumeHdfs == null) {
            resumeHdfs = true;
        }
    }
}
