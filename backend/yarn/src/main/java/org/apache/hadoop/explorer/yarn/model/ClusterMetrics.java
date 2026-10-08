package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class ClusterMetrics {
    @JsonProperty("total_memory_mb")
    private int totalMemoryMb;

    @JsonProperty("total_vcores")
    private int totalVcores;

    @JsonProperty("allocated_memory_mb")
    private int allocatedMemoryMb;

    @JsonProperty("allocated_vcores")
    private int allocatedVcores;

    @JsonProperty("available_memory_mb")
    private int availableMemoryMb;

    @JsonProperty("available_vcores")
    private int availableVcores;

    @JsonProperty("active_nodes")
    private int activeNodes;

    @JsonProperty("unhealthy_nodes")
    private int unhealthyNodes;

    @JsonProperty("total_containers")
    private int totalContainers;

    @JsonProperty("running_apps")
    private int runningApps;

    private List<String> partitions = new ArrayList<>();

    public ClusterMetrics() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ClusterMetrics obj = new ClusterMetrics();

        public Builder totalMemoryMb(int totalMemoryMb) { obj.totalMemoryMb = totalMemoryMb; return this; }
        public Builder totalVcores(int totalVcores) { obj.totalVcores = totalVcores; return this; }
        public Builder allocatedMemoryMb(int allocatedMemoryMb) { obj.allocatedMemoryMb = allocatedMemoryMb; return this; }
        public Builder allocatedVcores(int allocatedVcores) { obj.allocatedVcores = allocatedVcores; return this; }
        public Builder availableMemoryMb(int availableMemoryMb) { obj.availableMemoryMb = availableMemoryMb; return this; }
        public Builder availableVcores(int availableVcores) { obj.availableVcores = availableVcores; return this; }
        public Builder activeNodes(int activeNodes) { obj.activeNodes = activeNodes; return this; }
        public Builder unhealthyNodes(int unhealthyNodes) { obj.unhealthyNodes = unhealthyNodes; return this; }
        public Builder totalContainers(int totalContainers) { obj.totalContainers = totalContainers; return this; }
        public Builder runningApps(int runningApps) { obj.runningApps = runningApps; return this; }
        public Builder partitions(List<String> partitions) { obj.partitions = partitions; return this; }

        public ClusterMetrics build() { return obj; }
    }

    public int getTotalMemoryMb() { return totalMemoryMb; }
    public void setTotalMemoryMb(int totalMemoryMb) { this.totalMemoryMb = totalMemoryMb; }
    public int getTotalVcores() { return totalVcores; }
    public void setTotalVcores(int totalVcores) { this.totalVcores = totalVcores; }
    public int getAllocatedMemoryMb() { return allocatedMemoryMb; }
    public void setAllocatedMemoryMb(int allocatedMemoryMb) { this.allocatedMemoryMb = allocatedMemoryMb; }
    public int getAllocatedVcores() { return allocatedVcores; }
    public void setAllocatedVcores(int allocatedVcores) { this.allocatedVcores = allocatedVcores; }
    public int getAvailableMemoryMb() { return availableMemoryMb; }
    public void setAvailableMemoryMb(int availableMemoryMb) { this.availableMemoryMb = availableMemoryMb; }
    public int getAvailableVcores() { return availableVcores; }
    public void setAvailableVcores(int availableVcores) { this.availableVcores = availableVcores; }
    public int getActiveNodes() { return activeNodes; }
    public void setActiveNodes(int activeNodes) { this.activeNodes = activeNodes; }
    public int getUnhealthyNodes() { return unhealthyNodes; }
    public void setUnhealthyNodes(int unhealthyNodes) { this.unhealthyNodes = unhealthyNodes; }
    public int getTotalContainers() { return totalContainers; }
    public void setTotalContainers(int totalContainers) { this.totalContainers = totalContainers; }
    public int getRunningApps() { return runningApps; }
    public void setRunningApps(int runningApps) { this.runningApps = runningApps; }
    public List<String> getPartitions() { return partitions; }
    public void setPartitions(List<String> partitions) { this.partitions = partitions; }
}
