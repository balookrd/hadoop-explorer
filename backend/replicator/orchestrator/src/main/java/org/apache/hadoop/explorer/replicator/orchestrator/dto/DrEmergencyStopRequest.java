package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record DrEmergencyStopRequest(
    @NotBlank(message = "cluster_id cannot be blank")
    @JsonProperty("cluster_id")
    String clusterId,

    @JsonProperty("reason")
    String reason,

    @JsonProperty("fence_network")
    Boolean fenceNetwork
) {
    public DrEmergencyStopRequest {
        if (fenceNetwork == null) {
            fenceNetwork = true;
        }
    }
}
