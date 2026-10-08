package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BranchBalance {
    @JsonProperty("parent_path")
    private String parentPath;

    private String partition;

    @JsonProperty("total_children_capacity")
    private double totalChildrenCapacity;

    @JsonProperty("unallocated_capacity")
    private double unallocatedCapacity;

    @JsonProperty("is_balanced")
    private boolean balanced;

    private String status;
    private String message;

    @JsonProperty("total_children_memory_mb")
    private Integer totalChildrenMemoryMb;

    @JsonProperty("unallocated_memory_mb")
    private Integer unallocatedMemoryMb;

    @JsonProperty("total_children_vcores")
    private Integer totalChildrenVcores;

    @JsonProperty("unallocated_vcores")
    private Integer unallocatedVcores;

    @JsonProperty("ram_is_balanced")
    private Boolean ramIsBalanced;

    @JsonProperty("vcpu_is_balanced")
    private Boolean vcpuIsBalanced;

    public BranchBalance() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final BranchBalance obj = new BranchBalance();

        public Builder parentPath(String parentPath) { obj.parentPath = parentPath; return this; }
        public Builder partition(String partition) { obj.partition = partition; return this; }
        public Builder totalChildrenCapacity(double totalChildrenCapacity) { obj.totalChildrenCapacity = totalChildrenCapacity; return this; }
        public Builder unallocatedCapacity(double unallocatedCapacity) { obj.unallocatedCapacity = unallocatedCapacity; return this; }
        public Builder balanced(boolean balanced) { obj.balanced = balanced; return this; }
        public Builder status(String status) { obj.status = status; return this; }
        public Builder message(String message) { obj.message = message; return this; }
        public Builder totalChildrenMemoryMb(Integer totalChildrenMemoryMb) { obj.totalChildrenMemoryMb = totalChildrenMemoryMb; return this; }
        public Builder unallocatedMemoryMb(Integer unallocatedMemoryMb) { obj.unallocatedMemoryMb = unallocatedMemoryMb; return this; }
        public Builder totalChildrenVcores(Integer totalChildrenVcores) { obj.totalChildrenVcores = totalChildrenVcores; return this; }
        public Builder unallocatedVcores(Integer unallocatedVcores) { obj.unallocatedVcores = unallocatedVcores; return this; }
        public Builder ramIsBalanced(Boolean ramIsBalanced) { obj.ramIsBalanced = ramIsBalanced; return this; }
        public Builder vcpuIsBalanced(Boolean vcpuIsBalanced) { obj.vcpuIsBalanced = vcpuIsBalanced; return this; }

        public BranchBalance build() { return obj; }
    }

    public String getParentPath() { return parentPath; }
    public void setParentPath(String parentPath) { this.parentPath = parentPath; }
    public String getPartition() { return partition; }
    public void setPartition(String partition) { this.partition = partition; }
    public double getTotalChildrenCapacity() { return totalChildrenCapacity; }
    public void setTotalChildrenCapacity(double totalChildrenCapacity) { this.totalChildrenCapacity = totalChildrenCapacity; }
    public double getUnallocatedCapacity() { return unallocatedCapacity; }
    public void setUnallocatedCapacity(double unallocatedCapacity) { this.unallocatedCapacity = unallocatedCapacity; }
    public boolean isBalanced() { return balanced; }
    public void setBalanced(boolean balanced) { this.balanced = balanced; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public Integer getTotalChildrenMemoryMb() { return totalChildrenMemoryMb; }
    public void setTotalChildrenMemoryMb(Integer totalChildrenMemoryMb) { this.totalChildrenMemoryMb = totalChildrenMemoryMb; }
    public Integer getUnallocatedMemoryMb() { return unallocatedMemoryMb; }
    public void setUnallocatedMemoryMb(Integer unallocatedMemoryMb) { this.unallocatedMemoryMb = unallocatedMemoryMb; }
    public Integer getTotalChildrenVcores() { return totalChildrenVcores; }
    public void setTotalChildrenVcores(Integer totalChildrenVcores) { this.totalChildrenVcores = totalChildrenVcores; }
    public Integer getUnallocatedVcores() { return unallocatedVcores; }
    public void setUnallocatedVcores(Integer unallocatedVcores) { this.unallocatedVcores = unallocatedVcores; }
    public Boolean getRamIsBalanced() { return ramIsBalanced; }
    public void setRamIsBalanced(Boolean ramIsBalanced) { this.ramIsBalanced = ramIsBalanced; }
    public Boolean getVcpuIsBalanced() { return vcpuIsBalanced; }
    public void setVcpuIsBalanced(Boolean vcpuIsBalanced) { this.vcpuIsBalanced = vcpuIsBalanced; }
}
