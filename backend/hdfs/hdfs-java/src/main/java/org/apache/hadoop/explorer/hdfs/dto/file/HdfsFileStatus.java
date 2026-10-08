package org.apache.hadoop.explorer.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HdfsFileStatus {

    @JsonProperty("pathSuffix")
    private String pathSuffix;

    @JsonProperty("type")
    private String type; // "FILE" or "DIRECTORY"

    @JsonProperty("length")
    private long length;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("group")
    private String group;

    @JsonProperty("permission")
    private String permission;

    @JsonProperty("accessTime")
    private long accessTime;

    @JsonProperty("modificationTime")
    private long modificationTime;

    @JsonProperty("blockSize")
    private long blockSize;

    @JsonProperty("replication")
    private int replication;

    @JsonProperty("childrenNum")
    private Integer childrenNum = 0;

    public HdfsFileStatus() {}

    public HdfsFileStatus(String pathSuffix, String type, long length, String owner, String group,
                          String permission, long accessTime, long modificationTime,
                          long blockSize, int replication, Integer childrenNum) {
        this.pathSuffix = pathSuffix;
        this.type = type;
        this.length = length;
        this.owner = owner;
        this.group = group;
        this.permission = permission;
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;
        this.blockSize = blockSize;
        this.replication = replication;
        this.childrenNum = childrenNum != null ? childrenNum : 0;
    }

    public String getPathSuffix() { return pathSuffix; }
    public void setPathSuffix(String pathSuffix) { this.pathSuffix = pathSuffix; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public long getLength() { return length; }
    public void setLength(long length) { this.length = length; }

    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }

    public String getPermission() { return permission; }
    public void setPermission(String permission) { this.permission = permission; }

    public long getAccessTime() { return accessTime; }
    public void setAccessTime(long accessTime) { this.accessTime = accessTime; }

    public long getModificationTime() { return modificationTime; }
    public void setModificationTime(long modificationTime) { this.modificationTime = modificationTime; }

    public long getBlockSize() { return blockSize; }
    public void setBlockSize(long blockSize) { this.blockSize = blockSize; }

    public int getReplication() { return replication; }
    public void setReplication(int replication) { this.replication = replication; }

    public Integer getChildrenNum() { return childrenNum; }
    public void setChildrenNum(Integer childrenNum) { this.childrenNum = childrenNum; }
}
