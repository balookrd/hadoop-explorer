package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Элемент батча создания пофайловой задачи.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskCreateItem {

    @JsonProperty("id")
    private String id;

    @JsonProperty("source_path")
    private String sourcePath;

    @JsonProperty("target_path")
    private String targetPath;

    @JsonProperty("file_size")
    private long fileSize;

    @JsonProperty("skipped")
    private boolean skipped;

    @JsonProperty("max_retries")
    private Integer maxRetries;

    public TaskCreateItem() {}

    public TaskCreateItem(String id, String sourcePath, String targetPath, long fileSize, boolean skipped) {
        this(id, sourcePath, targetPath, fileSize, skipped, null);
    }

    public TaskCreateItem(String id, String sourcePath, String targetPath, long fileSize, boolean skipped, Integer maxRetries) {
        this.id = id;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.fileSize = fileSize;
        this.skipped = skipped;
        this.maxRetries = maxRetries;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getSourcePath() { return sourcePath; }
    public void setSourcePath(String sourcePath) { this.sourcePath = sourcePath; }

    public String getTargetPath() { return targetPath; }
    public void setTargetPath(String targetPath) { this.targetPath = targetPath; }

    public long getFileSize() { return fileSize; }
    public void setFileSize(long fileSize) { this.fileSize = fileSize; }

    public boolean isSkipped() { return skipped; }
    public void setSkipped(boolean skipped) { this.skipped = skipped; }

    public Integer getMaxRetries() { return maxRetries; }
    public void setMaxRetries(Integer maxRetries) { this.maxRetries = maxRetries; }
}
