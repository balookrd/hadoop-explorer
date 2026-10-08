package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DraftDiffResponse {
    @JsonProperty("cluster_id")
    private String clusterId;

    @JsonProperty("has_changes")
    private boolean hasChanges;

    private List<DiffItem> diffs = new ArrayList<>();

    @JsonProperty("queue_mappings_diff")
    private Map<String, Object> queueMappingsDiff;

    public DraftDiffResponse() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final DraftDiffResponse obj = new DraftDiffResponse();

        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder hasChanges(boolean hasChanges) { obj.hasChanges = hasChanges; return this; }
        public Builder diffs(List<DiffItem> diffs) { obj.diffs = diffs; return this; }
        public Builder queueMappingsDiff(Map<String, Object> queueMappingsDiff) { obj.queueMappingsDiff = queueMappingsDiff; return this; }

        public DraftDiffResponse build() { return obj; }
    }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public boolean isHasChanges() { return hasChanges; }
    public void setHasChanges(boolean hasChanges) { this.hasChanges = hasChanges; }
    public List<DiffItem> getDiffs() { return diffs; }
    public void setDiffs(List<DiffItem> diffs) { this.diffs = diffs; }
    public Map<String, Object> getQueueMappingsDiff() { return queueMappingsDiff; }
    public void setQueueMappingsDiff(Map<String, Object> queueMappingsDiff) { this.queueMappingsDiff = queueMappingsDiff; }
}
