package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClusterResources {
    @JsonProperty("memory_mb")
    private int memoryMb;

    @JsonProperty("vcores")
    private int vcores;

    public ClusterResources() {}

    public ClusterResources(int memoryMb, int vcores) {
        this.memoryMb = memoryMb;
        this.vcores = vcores;
    }

    public int getMemoryMb() { return memoryMb; }
    public void setMemoryMb(int memoryMb) { this.memoryMb = memoryMb; }
    public int getVcores() { return vcores; }
    public void setVcores(int vcores) { this.vcores = vcores; }
}
