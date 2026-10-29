package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;

import java.util.ArrayList;
import java.util.List;

public class ChangeRequestCreate {
    @NotBlank
    @JsonProperty("cluster_id")
    private String clusterId;

    @NotBlank
    @Size(min = 3, max = 150)
    private String title;

    @Size(max = 1000)
    private String description = "";

    @Valid
    private List<QueueDraftItem> changes = new ArrayList<>();

    @JsonProperty("queue_mappings")
    @Size(max = 4000)
    private String queueMappings;

    @JsonProperty("queue_mappings_override")
    private Boolean queueMappingsOverride;

    public ChangeRequestCreate() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final ChangeRequestCreate obj = new ChangeRequestCreate();

        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder title(String title) { obj.title = title; return this; }
        public Builder description(String description) { obj.description = description; return this; }
        public Builder changes(List<QueueDraftItem> changes) { obj.changes = changes; return this; }
        public Builder queueMappings(String queueMappings) { obj.queueMappings = queueMappings; return this; }
        public Builder queueMappingsOverride(Boolean queueMappingsOverride) { obj.queueMappingsOverride = queueMappingsOverride; return this; }

        public ChangeRequestCreate build() { return obj; }
    }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public List<QueueDraftItem> getChanges() { return changes; }
    public String getQueueMappings() { return queueMappings; }
    public void setQueueMappings(String queueMappings) { this.queueMappings = queueMappings; }
    public Boolean getQueueMappingsOverride() { return queueMappingsOverride; }
    public void setQueueMappingsOverride(Boolean queueMappingsOverride) { this.queueMappingsOverride = queueMappingsOverride; }
    public void setChanges(List<QueueDraftItem> changes) { this.changes = changes; }
}
