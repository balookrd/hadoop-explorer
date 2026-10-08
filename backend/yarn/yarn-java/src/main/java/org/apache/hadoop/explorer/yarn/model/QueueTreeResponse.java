package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class QueueTreeResponse {
    @JsonProperty("cluster_id")
    private String clusterId;

    @JsonProperty("cluster_name")
    private String clusterName;

    @JsonProperty("resource_mode")
    private String resourceMode;

    @JsonProperty("default_partition")
    private String defaultPartition;

    private List<String> partitions = new ArrayList<>();

    @JsonProperty("root_queue")
    private QueueNode rootQueue;

    @JsonProperty("cluster_metrics")
    private ClusterMetrics clusterMetrics;

    private List<BranchBalance> balances = new ArrayList<>();

    @JsonProperty("queue_mappings")
    private String queueMappings;

    @JsonProperty("queue_mappings_override")
    private boolean queueMappingsOverride;

    public QueueTreeResponse() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final QueueTreeResponse obj = new QueueTreeResponse();

        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder clusterName(String clusterName) { obj.clusterName = clusterName; return this; }
        public Builder resourceMode(String resourceMode) { obj.resourceMode = resourceMode; return this; }
        public Builder defaultPartition(String defaultPartition) { obj.defaultPartition = defaultPartition; return this; }
        public Builder partitions(List<String> partitions) { obj.partitions = partitions; return this; }
        public Builder rootQueue(QueueNode rootQueue) { obj.rootQueue = rootQueue; return this; }
        public Builder clusterMetrics(ClusterMetrics clusterMetrics) { obj.clusterMetrics = clusterMetrics; return this; }
        public Builder balances(List<BranchBalance> balances) { obj.balances = balances; return this; }
        public Builder queueMappings(String queueMappings) { obj.queueMappings = queueMappings; return this; }
        public Builder queueMappingsOverride(boolean queueMappingsOverride) { obj.queueMappingsOverride = queueMappingsOverride; return this; }

        public QueueTreeResponse build() { return obj; }
    }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public String getClusterName() { return clusterName; }
    public void setClusterName(String clusterName) { this.clusterName = clusterName; }
    public String getResourceMode() { return resourceMode; }
    public void setResourceMode(String resourceMode) { this.resourceMode = resourceMode; }
    public String getDefaultPartition() { return defaultPartition; }
    public void setDefaultPartition(String defaultPartition) { this.defaultPartition = defaultPartition; }
    public List<String> getPartitions() { return partitions; }
    public void setPartitions(List<String> partitions) { this.partitions = partitions; }
    public QueueNode getRootQueue() { return rootQueue; }
    public void setRootQueue(QueueNode rootQueue) { this.rootQueue = rootQueue; }
    public ClusterMetrics getClusterMetrics() { return clusterMetrics; }
    public void setClusterMetrics(ClusterMetrics clusterMetrics) { this.clusterMetrics = clusterMetrics; }
    public List<BranchBalance> getBalances() { return balances; }
    public void setBalances(List<BranchBalance> balances) { this.balances = balances; }
    public String getQueueMappings() { return queueMappings; }
    public void setQueueMappings(String queueMappings) { this.queueMappings = queueMappings; }
    public boolean isQueueMappingsOverride() { return queueMappingsOverride; }
    public void setQueueMappingsOverride(boolean queueMappingsOverride) { this.queueMappingsOverride = queueMappingsOverride; }
}
