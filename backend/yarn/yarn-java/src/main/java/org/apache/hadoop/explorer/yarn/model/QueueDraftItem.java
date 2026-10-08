package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueueDraftItem {
    @NotBlank
    @Pattern(regexp = "^root(\\.[a-zA-Z0-9_\\-]+)*$", message = "Полный путь очереди должен начинаться с root")
    private String path;

    @NotBlank
    @Pattern(regexp = "^[a-zA-Z0-9_\\-]+$", message = "Имя очереди должно содержать буквы, цифры, дефис или подчеркивание")
    private String name;

    @JsonProperty("parent_path")
    @Pattern(regexp = "^root(\\.[a-zA-Z0-9_\\-]+)*$", message = "Путь родительской очереди должен начинаться с root")
    private String parentPath;

    private String action = "modify";

    @JsonProperty("is_leaf")
    private boolean leaf = true;

    private QueueState state = QueueState.RUNNING;

    @JsonProperty("resource_mode")
    private String resourceMode;

    @JsonProperty("user_limit_factor")
    private Double userLimitFactor;

    @JsonProperty("ordering_policy")
    private String orderingPolicy;

    @JsonProperty("max_applications")
    private Integer maxApplications;

    @JsonProperty("max_am_resource_percent")
    private Double maxAmResourcePercent;

    @JsonProperty("max_parallel_apps")
    private Integer maxParallelApps;

    @JsonProperty("max_application_lifetime")
    private Integer maxApplicationLifetime;

    @JsonProperty("accessible_node_labels")
    private List<String> accessibleNodeLabels;

    @JsonProperty("default_node_label_expression")
    private String defaultNodeLabelExpression;

    private Map<String, PartitionResourceConfig> partitions = new HashMap<>();

    public QueueDraftItem() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final QueueDraftItem obj = new QueueDraftItem();

        public Builder path(String path) { obj.path = path; return this; }
        public Builder name(String name) { obj.name = name; return this; }
        public Builder parentPath(String parentPath) { obj.parentPath = parentPath; return this; }
        public Builder action(String action) { obj.action = action; return this; }
        public Builder leaf(boolean leaf) { obj.leaf = leaf; return this; }
        public Builder state(QueueState state) { obj.state = state; return this; }
        public Builder resourceMode(String resourceMode) { obj.resourceMode = resourceMode; return this; }
        public Builder userLimitFactor(Double userLimitFactor) { obj.userLimitFactor = userLimitFactor; return this; }
        public Builder orderingPolicy(String orderingPolicy) { obj.orderingPolicy = orderingPolicy; return this; }
        public Builder maxApplications(Integer maxApplications) { obj.maxApplications = maxApplications; return this; }
        public Builder maxAmResourcePercent(Double maxAmResourcePercent) { obj.maxAmResourcePercent = maxAmResourcePercent; return this; }
        public Builder maxParallelApps(Integer maxParallelApps) { obj.maxParallelApps = maxParallelApps; return this; }
        public Builder maxApplicationLifetime(Integer maxApplicationLifetime) { obj.maxApplicationLifetime = maxApplicationLifetime; return this; }
        public Builder accessibleNodeLabels(List<String> accessibleNodeLabels) { obj.accessibleNodeLabels = accessibleNodeLabels; return this; }
        public Builder defaultNodeLabelExpression(String defaultNodeLabelExpression) { obj.defaultNodeLabelExpression = defaultNodeLabelExpression; return this; }
        public Builder partitions(Map<String, PartitionResourceConfig> partitions) { obj.partitions = partitions; return this; }

        public QueueDraftItem build() { return obj; }
    }

    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getParentPath() { return parentPath; }
    public void setParentPath(String parentPath) { this.parentPath = parentPath; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public boolean isLeaf() { return leaf; }
    public void setLeaf(boolean leaf) { this.leaf = leaf; }
    public QueueState getState() { return state; }
    public void setState(QueueState state) { this.state = state; }
    public String getResourceMode() { return resourceMode; }
    public void setResourceMode(String resourceMode) { this.resourceMode = resourceMode; }
    public Double getUserLimitFactor() { return userLimitFactor; }
    public void setUserLimitFactor(Double userLimitFactor) { this.userLimitFactor = userLimitFactor; }
    public String getOrderingPolicy() { return orderingPolicy; }
    public void setOrderingPolicy(String orderingPolicy) { this.orderingPolicy = orderingPolicy; }
    public Integer getMaxApplications() { return maxApplications; }
    public void setMaxApplications(Integer maxApplications) { this.maxApplications = maxApplications; }
    public Double getMaxAmResourcePercent() { return maxAmResourcePercent; }
    public void setMaxAmResourcePercent(Double maxAmResourcePercent) { this.maxAmResourcePercent = maxAmResourcePercent; }
    public Integer getMaxParallelApps() { return maxParallelApps; }
    public void setMaxParallelApps(Integer maxParallelApps) { this.maxParallelApps = maxParallelApps; }
    public Integer getMaxApplicationLifetime() { return maxApplicationLifetime; }
    public void setMaxApplicationLifetime(Integer maxApplicationLifetime) { this.maxApplicationLifetime = maxApplicationLifetime; }
    public List<String> getAccessibleNodeLabels() { return accessibleNodeLabels; }
    public void setAccessibleNodeLabels(List<String> accessibleNodeLabels) { this.accessibleNodeLabels = accessibleNodeLabels; }
    public String getDefaultNodeLabelExpression() { return defaultNodeLabelExpression; }
    public void setDefaultNodeLabelExpression(String defaultNodeLabelExpression) { this.defaultNodeLabelExpression = defaultNodeLabelExpression; }
    public Map<String, PartitionResourceConfig> getPartitions() { return partitions; }
    public void setPartitions(Map<String, PartitionResourceConfig> partitions) { this.partitions = partitions; }
}
