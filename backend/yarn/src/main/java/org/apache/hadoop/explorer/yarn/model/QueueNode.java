package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueueNode {
    private String name;
    private String path;

    @JsonProperty("parent_path")
    private String parentPath;

    @JsonProperty("is_leaf")
    private boolean leaf = true;

    private QueueState state = QueueState.RUNNING;

    @JsonProperty("resource_mode")
    private String resourceMode = "percentage";

    @JsonProperty("user_limit_factor")
    private Double userLimitFactor = 1.0;

    @JsonProperty("ordering_policy")
    private String orderingPolicy = "fifo";

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

    @JsonProperty("current_used_resources")
    private ResourceAllocation currentUsedResources = new ResourceAllocation();

    @JsonProperty("allocated_resources")
    private ResourceAllocation allocatedResources = new ResourceAllocation();

    @JsonProperty("current_used_percent")
    private double currentUsedPercent = 0.0;

    @JsonProperty("num_applications")
    private int numApplications = 0;

    @JsonProperty("num_active_applications")
    private int numActiveApplications = 0;

    @JsonProperty("num_pending_applications")
    private int numPendingApplications = 0;

    private List<QueueNode> children = new ArrayList<>();

    public QueueNode() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final QueueNode obj = new QueueNode();

        public Builder name(String name) { obj.name = name; return this; }
        public Builder path(String path) { obj.path = path; return this; }
        public Builder parentPath(String parentPath) { obj.parentPath = parentPath; return this; }
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
        public Builder currentUsedResources(ResourceAllocation currentUsedResources) { obj.currentUsedResources = currentUsedResources; return this; }
        public Builder allocatedResources(ResourceAllocation allocatedResources) { obj.allocatedResources = allocatedResources; return this; }
        public Builder currentUsedPercent(double currentUsedPercent) { obj.currentUsedPercent = currentUsedPercent; return this; }
        public Builder numApplications(int numApplications) { obj.numApplications = numApplications; return this; }
        public Builder numActiveApplications(int numActiveApplications) { obj.numActiveApplications = numActiveApplications; return this; }
        public Builder numPendingApplications(int numPendingApplications) { obj.numPendingApplications = numPendingApplications; return this; }
        public Builder children(List<QueueNode> children) { obj.children = children; return this; }

        public QueueNode build() { return obj; }
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }
    public String getParentPath() { return parentPath; }
    public void setParentPath(String parentPath) { this.parentPath = parentPath; }
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
    public ResourceAllocation getCurrentUsedResources() { return currentUsedResources; }
    public void setCurrentUsedResources(ResourceAllocation currentUsedResources) { this.currentUsedResources = currentUsedResources; }
    public ResourceAllocation getAllocatedResources() { return allocatedResources; }
    public void setAllocatedResources(ResourceAllocation allocatedResources) { this.allocatedResources = allocatedResources; }
    public double getCurrentUsedPercent() { return currentUsedPercent; }
    public void setCurrentUsedPercent(double currentUsedPercent) { this.currentUsedPercent = currentUsedPercent; }
    public int getNumApplications() { return numApplications; }
    public void setNumApplications(int numApplications) { this.numApplications = numApplications; }
    public int getNumActiveApplications() { return numActiveApplications; }
    public void setNumActiveApplications(int numActiveApplications) { this.numActiveApplications = numActiveApplications; }
    public int getNumPendingApplications() { return numPendingApplications; }
    public void setNumPendingApplications(int numPendingApplications) { this.numPendingApplications = numPendingApplications; }
    public List<QueueNode> getChildren() { return children; }
    public void setChildren(List<QueueNode> children) { this.children = children; }
}
