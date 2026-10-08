package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

public class DraftValidateRequest {
    @NotBlank
    @JsonProperty("cluster_id")
    private String clusterId;

    @JsonProperty("selected_partition")
    private String selectedPartition = "DEFAULT";

    @NotNull
    private List<QueueDraftItem> queues = new ArrayList<>();

    @JsonProperty("queue_mappings")
    private String queueMappings;

    @JsonProperty("queue_mappings_override")
    private Boolean queueMappingsOverride;

    public DraftValidateRequest() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final DraftValidateRequest obj = new DraftValidateRequest();

        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder selectedPartition(String selectedPartition) { obj.selectedPartition = selectedPartition; return this; }
        public Builder queues(List<QueueDraftItem> queues) { obj.queues = queues; return this; }
        public Builder queueMappings(String queueMappings) { obj.queueMappings = queueMappings; return this; }
        public Builder queueMappingsOverride(Boolean queueMappingsOverride) { obj.queueMappingsOverride = queueMappingsOverride; return this; }

        public DraftValidateRequest build() { return obj; }
    }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public String getSelectedPartition() { return selectedPartition; }
    public void setSelectedPartition(String selectedPartition) { this.selectedPartition = selectedPartition; }
    public List<QueueDraftItem> getQueues() { return queues; }
    public void setQueues(List<QueueDraftItem> queues) { this.queues = queues; }
    public String getQueueMappings() { return queueMappings; }
    public void setQueueMappings(String queueMappings) { this.queueMappings = queueMappings; }
    public Boolean getQueueMappingsOverride() { return queueMappingsOverride; }
    public void setQueueMappingsOverride(Boolean queueMappingsOverride) { this.queueMappingsOverride = queueMappingsOverride; }
}
