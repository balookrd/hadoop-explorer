package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ResourceAllocation {
    @JsonProperty("memory_mb")
    private int memoryMb = 0;

    @JsonProperty("vcores")
    private int vcores = 0;

    public ResourceAllocation() {}

    public ResourceAllocation(int memoryMb, int vcores) {
        this.memoryMb = memoryMb;
        this.vcores = vcores;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    public void setMemoryMb(int memoryMb) {
        this.memoryMb = memoryMb;
    }

    public int getVcores() {
        return vcores;
    }

    public void setVcores(int vcores) {
        this.vcores = vcores;
    }
}
