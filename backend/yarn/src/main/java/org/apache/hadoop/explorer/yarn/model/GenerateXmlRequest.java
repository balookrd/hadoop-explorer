package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

public class GenerateXmlRequest {
    @NotBlank
    @JsonProperty("cluster_id")
    private String clusterId;

    @NotNull
    private List<QueueDraftItem> queues = new ArrayList<>();

    @JsonProperty("proposal_comment")
    private String proposalComment;

    @JsonProperty("resource_mode_override")
    private String resourceModeOverride;

    @JsonProperty("queue_mappings")
    private String queueMappings;

    @JsonProperty("queue_mappings_override")
    private Boolean queueMappingsOverride;

    public GenerateXmlRequest() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final GenerateXmlRequest obj = new GenerateXmlRequest();

        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder queues(List<QueueDraftItem> queues) { obj.queues = queues; return this; }
        public Builder proposalComment(String proposalComment) { obj.proposalComment = proposalComment; return this; }
        public Builder resourceModeOverride(String resourceModeOverride) { obj.resourceModeOverride = resourceModeOverride; return this; }
        public Builder queueMappings(String queueMappings) { obj.queueMappings = queueMappings; return this; }
        public Builder queueMappingsOverride(Boolean queueMappingsOverride) { obj.queueMappingsOverride = queueMappingsOverride; return this; }

        public GenerateXmlRequest build() { return obj; }
    }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public List<QueueDraftItem> getQueues() { return queues; }
    public void setQueues(List<QueueDraftItem> queues) { this.queues = queues; }
    public String getProposalComment() { return proposalComment; }
    public void setProposalComment(String proposalComment) { this.proposalComment = proposalComment; }
    public String getResourceModeOverride() { return resourceModeOverride; }
    public void setResourceModeOverride(String resourceModeOverride) { this.resourceModeOverride = resourceModeOverride; }
    public String getQueueMappings() { return queueMappings; }
    public void setQueueMappings(String queueMappings) { this.queueMappings = queueMappings; }
    public Boolean getQueueMappingsOverride() { return queueMappingsOverride; }
    public void setQueueMappingsOverride(Boolean queueMappingsOverride) { this.queueMappingsOverride = queueMappingsOverride; }
}
