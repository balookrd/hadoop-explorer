package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PartitionResourceConfig {
    @JsonProperty("partition_name")
    private String partitionName = "DEFAULT";

    private double capacity;

    @JsonProperty("max_capacity")
    private double maxCapacity;

    @JsonProperty("is_elastic")
    private boolean elastic = false;

    @JsonProperty("elasticity_ratio")
    private double elasticityRatio = 1.0;

    @JsonProperty("memory_mb")
    private Integer memoryMb;

    private Integer vcores;

    @JsonProperty("max_memory_mb")
    private Integer maxMemoryMb;

    @JsonProperty("max_vcores")
    private Integer maxVcores;

    @JsonProperty("memory_percent")
    private Double memoryPercent;

    @JsonProperty("vcore_percent")
    private Double vcorePercent;

    @JsonProperty("max_memory_percent")
    private Double maxMemoryPercent;

    @JsonProperty("max_vcore_percent")
    private Double maxVcorePercent;

    @JsonProperty("absolute_resources")
    private ResourceAllocation absoluteResources;

    @JsonProperty("absolute_max_resources")
    private ResourceAllocation absoluteMaxResources;

    public PartitionResourceConfig() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final PartitionResourceConfig obj = new PartitionResourceConfig();

        public Builder partitionName(String partitionName) {
            obj.partitionName = partitionName;
            return this;
        }

        public Builder capacity(double capacity) {
            obj.capacity = capacity;
            return this;
        }

        public Builder maxCapacity(double maxCapacity) {
            obj.maxCapacity = maxCapacity;
            return this;
        }

        public Builder elastic(boolean elastic) {
            obj.elastic = elastic;
            return this;
        }

        public Builder elasticityRatio(double elasticityRatio) {
            obj.elasticityRatio = elasticityRatio;
            return this;
        }

        public Builder memoryMb(Integer memoryMb) {
            obj.memoryMb = memoryMb;
            return this;
        }

        public Builder vcores(Integer vcores) {
            obj.vcores = vcores;
            return this;
        }

        public Builder maxMemoryMb(Integer maxMemoryMb) {
            obj.maxMemoryMb = maxMemoryMb;
            return this;
        }

        public Builder maxVcores(Integer maxVcores) {
            obj.maxVcores = maxVcores;
            return this;
        }

        public Builder memoryPercent(Double memoryPercent) {
            obj.memoryPercent = memoryPercent;
            return this;
        }

        public Builder vcorePercent(Double vcorePercent) {
            obj.vcorePercent = vcorePercent;
            return this;
        }

        public Builder maxMemoryPercent(Double maxMemoryPercent) {
            obj.maxMemoryPercent = maxMemoryPercent;
            return this;
        }

        public Builder maxVcorePercent(Double maxVcorePercent) {
            obj.maxVcorePercent = maxVcorePercent;
            return this;
        }

        public Builder absoluteResources(ResourceAllocation absoluteResources) {
            obj.absoluteResources = absoluteResources;
            return this;
        }

        public Builder absoluteMaxResources(ResourceAllocation absoluteMaxResources) {
            obj.absoluteMaxResources = absoluteMaxResources;
            return this;
        }

        public PartitionResourceConfig build() {
            return obj;
        }
    }

    public String getPartitionName() { return partitionName; }
    public void setPartitionName(String partitionName) { this.partitionName = partitionName; }
    public double getCapacity() { return capacity; }
    public void setCapacity(double capacity) { this.capacity = capacity; }
    public double getMaxCapacity() { return maxCapacity; }
    public void setMaxCapacity(double maxCapacity) { this.maxCapacity = maxCapacity; }
    public boolean isElastic() { return elastic; }
    public void setElastic(boolean elastic) { this.elastic = elastic; }
    public double getElasticityRatio() { return elasticityRatio; }
    public void setElasticityRatio(double elasticityRatio) { this.elasticityRatio = elasticityRatio; }
    public Integer getMemoryMb() { return memoryMb; }
    public void setMemoryMb(Integer memoryMb) { this.memoryMb = memoryMb; }
    public Integer getVcores() { return vcores; }
    public void setVcores(Integer vcores) { this.vcores = vcores; }
    public Integer getMaxMemoryMb() { return maxMemoryMb; }
    public void setMaxMemoryMb(Integer maxMemoryMb) { this.maxMemoryMb = maxMemoryMb; }
    public Integer getMaxVcores() { return maxVcores; }
    public void setMaxVcores(Integer maxVcores) { this.maxVcores = maxVcores; }
    public Double getMemoryPercent() { return memoryPercent; }
    public void setMemoryPercent(Double memoryPercent) { this.memoryPercent = memoryPercent; }
    public Double getVcorePercent() { return vcorePercent; }
    public void setVcorePercent(Double vcorePercent) { this.vcorePercent = vcorePercent; }
    public Double getMaxMemoryPercent() { return maxMemoryPercent; }
    public void setMaxMemoryPercent(Double maxMemoryPercent) { this.maxMemoryPercent = maxMemoryPercent; }
    public Double getMaxVcorePercent() { return maxVcorePercent; }
    public void setMaxVcorePercent(Double maxVcorePercent) { this.maxVcorePercent = maxVcorePercent; }
    public ResourceAllocation getAbsoluteResources() { return absoluteResources; }
    public void setAbsoluteResources(ResourceAllocation absoluteResources) { this.absoluteResources = absoluteResources; }
    public ResourceAllocation getAbsoluteMaxResources() { return absoluteMaxResources; }
    public void setAbsoluteMaxResources(ResourceAllocation absoluteMaxResources) { this.absoluteMaxResources = absoluteMaxResources; }
}
