package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DiffItem {
    private String path;
    private String name;

    @JsonProperty("parent_path")
    private String parentPath;

    private String partition;
    private String action;

    @JsonProperty("live_capacity")
    private Double liveCapacity;

    @JsonProperty("draft_capacity")
    private Double draftCapacity;

    @JsonProperty("delta_capacity")
    private Double deltaCapacity;

    @JsonProperty("live_max_capacity")
    private Double liveMaxCapacity;

    @JsonProperty("draft_max_capacity")
    private Double draftMaxCapacity;

    @JsonProperty("delta_max_capacity")
    private Double deltaMaxCapacity;

    @JsonProperty("live_memory_mb")
    private Integer liveMemoryMb;

    @JsonProperty("draft_memory_mb")
    private Integer draftMemoryMb;

    @JsonProperty("delta_memory_mb")
    private Integer deltaMemoryMb;

    @JsonProperty("live_vcores")
    private Integer liveVcores;

    @JsonProperty("draft_vcores")
    private Integer draftVcores;

    @JsonProperty("delta_vcores")
    private Integer deltaVcores;

    @JsonProperty("live_type")
    private QueueType liveType;

    @JsonProperty("draft_type")
    private QueueType draftType;

    @JsonProperty("live_state")
    private QueueState liveState;

    @JsonProperty("draft_state")
    private QueueState draftState;

    @JsonProperty("live_resource_mode")
    private String liveResourceMode;

    @JsonProperty("draft_resource_mode")
    private String draftResourceMode;

    @JsonProperty("live_user_limit_factor")
    private Double liveUserLimitFactor;

    @JsonProperty("draft_user_limit_factor")
    private Double draftUserLimitFactor;

    @JsonProperty("live_ordering_policy")
    private String liveOrderingPolicy;

    @JsonProperty("draft_ordering_policy")
    private String draftOrderingPolicy;

    @JsonProperty("live_max_applications")
    private Integer liveMaxApplications;

    @JsonProperty("draft_max_applications")
    private Integer draftMaxApplications;

    @JsonProperty("live_max_am_resource_percent")
    private Double liveMaxAmResourcePercent;

    @JsonProperty("draft_max_am_resource_percent")
    private Double draftMaxAmResourcePercent;

    @JsonProperty("live_max_parallel_apps")
    private Integer liveMaxParallelApps;

    @JsonProperty("draft_max_parallel_apps")
    private Integer draftMaxParallelApps;

    @JsonProperty("live_max_application_lifetime")
    private Integer liveMaxApplicationLifetime;

    @JsonProperty("draft_max_application_lifetime")
    private Integer draftMaxApplicationLifetime;

    @JsonProperty("live_accessible_node_labels")
    private List<String> liveAccessibleNodeLabels;

    @JsonProperty("draft_accessible_node_labels")
    private List<String> draftAccessibleNodeLabels;

    @JsonProperty("live_default_node_label_expression")
    private String liveDefaultNodeLabelExpression;

    @JsonProperty("draft_default_node_label_expression")
    private String draftDefaultNodeLabelExpression;

    public DiffItem() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final DiffItem obj = new DiffItem();

        public Builder path(String path) { obj.path = path; return this; }
        public Builder name(String name) { obj.name = name; return this; }
        public Builder parentPath(String parentPath) { obj.parentPath = parentPath; return this; }
        public Builder partition(String partition) { obj.partition = partition; return this; }
        public Builder action(String action) { obj.action = action; return this; }
        public Builder liveCapacity(Double liveCapacity) { obj.liveCapacity = liveCapacity; return this; }
        public Builder draftCapacity(Double draftCapacity) { obj.draftCapacity = draftCapacity; return this; }
        public Builder deltaCapacity(Double deltaCapacity) { obj.deltaCapacity = deltaCapacity; return this; }
        public Builder liveMaxCapacity(Double liveMaxCapacity) { obj.liveMaxCapacity = liveMaxCapacity; return this; }
        public Builder draftMaxCapacity(Double draftMaxCapacity) { obj.draftMaxCapacity = draftMaxCapacity; return this; }
        public Builder deltaMaxCapacity(Double deltaMaxCapacity) { obj.deltaMaxCapacity = deltaMaxCapacity; return this; }
        public Builder liveMemoryMb(Integer liveMemoryMb) { obj.liveMemoryMb = liveMemoryMb; return this; }
        public Builder draftMemoryMb(Integer draftMemoryMb) { obj.draftMemoryMb = draftMemoryMb; return this; }
        public Builder deltaMemoryMb(Integer deltaMemoryMb) { obj.deltaMemoryMb = deltaMemoryMb; return this; }
        public Builder liveVcores(Integer liveVcores) { obj.liveVcores = liveVcores; return this; }
        public Builder draftVcores(Integer draftVcores) { obj.draftVcores = draftVcores; return this; }
        public Builder deltaVcores(Integer deltaVcores) { obj.deltaVcores = deltaVcores; return this; }
        public Builder liveType(QueueType liveType) { obj.liveType = liveType; return this; }
        public Builder draftType(QueueType draftType) { obj.draftType = draftType; return this; }
        public Builder liveState(QueueState liveState) { obj.liveState = liveState; return this; }
        public Builder draftState(QueueState draftState) { obj.draftState = draftState; return this; }
        public Builder liveResourceMode(String liveResourceMode) { obj.liveResourceMode = liveResourceMode; return this; }
        public Builder draftResourceMode(String draftResourceMode) { obj.draftResourceMode = draftResourceMode; return this; }
        public Builder liveUserLimitFactor(Double liveUserLimitFactor) { obj.liveUserLimitFactor = liveUserLimitFactor; return this; }
        public Builder draftUserLimitFactor(Double draftUserLimitFactor) { obj.draftUserLimitFactor = draftUserLimitFactor; return this; }
        public Builder liveOrderingPolicy(String liveOrderingPolicy) { obj.liveOrderingPolicy = liveOrderingPolicy; return this; }
        public Builder draftOrderingPolicy(String draftOrderingPolicy) { obj.draftOrderingPolicy = draftOrderingPolicy; return this; }
        public Builder liveMaxApplications(Integer liveMaxApplications) { obj.liveMaxApplications = liveMaxApplications; return this; }
        public Builder draftMaxApplications(Integer draftMaxApplications) { obj.draftMaxApplications = draftMaxApplications; return this; }
        public Builder liveMaxAmResourcePercent(Double liveMaxAmResourcePercent) { obj.liveMaxAmResourcePercent = liveMaxAmResourcePercent; return this; }
        public Builder draftMaxAmResourcePercent(Double draftMaxAmResourcePercent) { obj.draftMaxAmResourcePercent = draftMaxAmResourcePercent; return this; }
        public Builder liveMaxParallelApps(Integer liveMaxParallelApps) { obj.liveMaxParallelApps = liveMaxParallelApps; return this; }
        public Builder draftMaxParallelApps(Integer draftMaxParallelApps) { obj.draftMaxParallelApps = draftMaxParallelApps; return this; }
        public Builder liveMaxApplicationLifetime(Integer liveMaxApplicationLifetime) { obj.liveMaxApplicationLifetime = liveMaxApplicationLifetime; return this; }
        public Builder draftMaxApplicationLifetime(Integer draftMaxApplicationLifetime) { obj.draftMaxApplicationLifetime = draftMaxApplicationLifetime; return this; }
        public Builder liveAccessibleNodeLabels(List<String> liveAccessibleNodeLabels) { obj.liveAccessibleNodeLabels = liveAccessibleNodeLabels; return this; }
        public Builder draftAccessibleNodeLabels(List<String> draftAccessibleNodeLabels) { obj.draftAccessibleNodeLabels = draftAccessibleNodeLabels; return this; }
        public Builder liveDefaultNodeLabelExpression(String liveDefaultNodeLabelExpression) { obj.liveDefaultNodeLabelExpression = liveDefaultNodeLabelExpression; return this; }
        public Builder draftDefaultNodeLabelExpression(String draftDefaultNodeLabelExpression) { obj.draftDefaultNodeLabelExpression = draftDefaultNodeLabelExpression; return this; }

        public DiffItem build() { return obj; }
    }

    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getParentPath() { return parentPath; }
    public void setParentPath(String parentPath) { this.parentPath = parentPath; }
    public String getPartition() { return partition; }
    public void setPartition(String partition) { this.partition = partition; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public Double getLiveCapacity() { return liveCapacity; }
    public void setLiveCapacity(Double liveCapacity) { this.liveCapacity = liveCapacity; }
    public Double getDraftCapacity() { return draftCapacity; }
    public void setDraftCapacity(Double draftCapacity) { this.draftCapacity = draftCapacity; }
    public Double getDeltaCapacity() { return deltaCapacity; }
    public void setDeltaCapacity(Double deltaCapacity) { this.deltaCapacity = deltaCapacity; }
    public Double getLiveMaxCapacity() { return liveMaxCapacity; }
    public void setLiveMaxCapacity(Double liveMaxCapacity) { this.liveMaxCapacity = liveMaxCapacity; }
    public Double getDraftMaxCapacity() { return draftMaxCapacity; }
    public void setDraftMaxCapacity(Double draftMaxCapacity) { this.draftMaxCapacity = draftMaxCapacity; }
    public Double getDeltaMaxCapacity() { return deltaMaxCapacity; }
    public void setDeltaMaxCapacity(Double deltaMaxCapacity) { this.deltaMaxCapacity = deltaMaxCapacity; }
    public Integer getLiveMemoryMb() { return liveMemoryMb; }
    public void setLiveMemoryMb(Integer liveMemoryMb) { this.liveMemoryMb = liveMemoryMb; }
    public Integer getDraftMemoryMb() { return draftMemoryMb; }
    public void setDraftMemoryMb(Integer draftMemoryMb) { this.draftMemoryMb = draftMemoryMb; }
    public Integer getDeltaMemoryMb() { return deltaMemoryMb; }
    public void setDeltaMemoryMb(Integer deltaMemoryMb) { this.deltaMemoryMb = deltaMemoryMb; }
    public Integer getLiveVcores() { return liveVcores; }
    public void setLiveVcores(Integer liveVcores) { this.liveVcores = liveVcores; }
    public Integer getDraftVcores() { return draftVcores; }
    public void setDraftVcores(Integer draftVcores) { this.draftVcores = draftVcores; }
    public Integer getDeltaVcores() { return deltaVcores; }
    public void setDeltaVcores(Integer deltaVcores) { this.deltaVcores = deltaVcores; }
    public QueueType getLiveType() { return liveType; }
    public void setLiveType(QueueType liveType) { this.liveType = liveType; }
    public QueueType getDraftType() { return draftType; }
    public void setDraftType(QueueType draftType) { this.draftType = draftType; }
    public QueueState getLiveState() { return liveState; }
    public void setLiveState(QueueState liveState) { this.liveState = liveState; }
    public QueueState getDraftState() { return draftState; }
    public void setDraftState(QueueState draftState) { this.draftState = draftState; }
    public String getLiveResourceMode() { return liveResourceMode; }
    public void setLiveResourceMode(String liveResourceMode) { this.liveResourceMode = liveResourceMode; }
    public String getDraftResourceMode() { return draftResourceMode; }
    public void setDraftResourceMode(String draftResourceMode) { this.draftResourceMode = draftResourceMode; }
    public Double getLiveUserLimitFactor() { return liveUserLimitFactor; }
    public void setLiveUserLimitFactor(Double liveUserLimitFactor) { this.liveUserLimitFactor = liveUserLimitFactor; }
    public Double getDraftUserLimitFactor() { return draftUserLimitFactor; }
    public void setDraftUserLimitFactor(Double draftUserLimitFactor) { this.draftUserLimitFactor = draftUserLimitFactor; }
    public String getLiveOrderingPolicy() { return liveOrderingPolicy; }
    public void setLiveOrderingPolicy(String liveOrderingPolicy) { this.liveOrderingPolicy = liveOrderingPolicy; }
    public String getDraftOrderingPolicy() { return draftOrderingPolicy; }
    public void setDraftOrderingPolicy(String draftOrderingPolicy) { this.draftOrderingPolicy = draftOrderingPolicy; }
    public Integer getLiveMaxApplications() { return liveMaxApplications; }
    public void setLiveMaxApplications(Integer liveMaxApplications) { this.liveMaxApplications = liveMaxApplications; }
    public Integer getDraftMaxApplications() { return draftMaxApplications; }
    public void setDraftMaxApplications(Integer draftMaxApplications) { this.draftMaxApplications = draftMaxApplications; }
    public Double getLiveMaxAmResourcePercent() { return liveMaxAmResourcePercent; }
    public void setLiveMaxAmResourcePercent(Double liveMaxAmResourcePercent) { this.liveMaxAmResourcePercent = liveMaxAmResourcePercent; }
    public Double getDraftMaxAmResourcePercent() { return draftMaxAmResourcePercent; }
    public void setDraftMaxAmResourcePercent(Double draftMaxAmResourcePercent) { this.draftMaxAmResourcePercent = draftMaxAmResourcePercent; }
    public Integer getLiveMaxParallelApps() { return liveMaxParallelApps; }
    public void setLiveMaxParallelApps(Integer liveMaxParallelApps) { this.liveMaxParallelApps = liveMaxParallelApps; }
    public Integer getDraftMaxParallelApps() { return draftMaxParallelApps; }
    public void setDraftMaxParallelApps(Integer draftMaxParallelApps) { this.draftMaxParallelApps = draftMaxParallelApps; }
    public Integer getLiveMaxApplicationLifetime() { return liveMaxApplicationLifetime; }
    public void setLiveMaxApplicationLifetime(Integer liveMaxApplicationLifetime) { this.liveMaxApplicationLifetime = liveMaxApplicationLifetime; }
    public Integer getDraftMaxApplicationLifetime() { return draftMaxApplicationLifetime; }
    public void setDraftMaxApplicationLifetime(Integer draftMaxApplicationLifetime) { this.draftMaxApplicationLifetime = draftMaxApplicationLifetime; }
    public List<String> getLiveAccessibleNodeLabels() { return liveAccessibleNodeLabels; }
    public void setLiveAccessibleNodeLabels(List<String> liveAccessibleNodeLabels) { this.liveAccessibleNodeLabels = liveAccessibleNodeLabels; }
    public List<String> getDraftAccessibleNodeLabels() { return draftAccessibleNodeLabels; }
    public void setDraftAccessibleNodeLabels(List<String> draftAccessibleNodeLabels) { this.draftAccessibleNodeLabels = draftAccessibleNodeLabels; }
    public String getLiveDefaultNodeLabelExpression() { return liveDefaultNodeLabelExpression; }
    public void setLiveDefaultNodeLabelExpression(String liveDefaultNodeLabelExpression) { this.liveDefaultNodeLabelExpression = liveDefaultNodeLabelExpression; }
    public String getDraftDefaultNodeLabelExpression() { return draftDefaultNodeLabelExpression; }
    public void setDraftDefaultNodeLabelExpression(String draftDefaultNodeLabelExpression) { this.draftDefaultNodeLabelExpression = draftDefaultNodeLabelExpression; }
}
