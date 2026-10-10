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

    @JsonProperty("task_type")
    private String taskType = "FILE";

    @JsonProperty("file_count")
    private int fileCount = 1;

    @JsonProperty("bundle_manifest")
    private String bundleManifest;

    public TaskCreateItem() {}

    public TaskCreateItem(String sourcePath, String targetPath, long fileSize, boolean skipped) {
        this(java.util.UUID.randomUUID().toString(), sourcePath, targetPath, fileSize, skipped, null, "FILE", 1, null);
    }

    public TaskCreateItem(String id, String sourcePath, String targetPath, long fileSize, boolean skipped) {
        this(id, sourcePath, targetPath, fileSize, skipped, null, "FILE", 1, null);
    }

    public TaskCreateItem(String id, String sourcePath, String targetPath, long fileSize, boolean skipped, Integer maxRetries) {
        this(id, sourcePath, targetPath, fileSize, skipped, maxRetries, "FILE", 1, null);
    }

    public TaskCreateItem(String id, String sourcePath, String targetPath, long fileSize, boolean skipped,
                          Integer maxRetries, String taskType, int fileCount, String bundleManifest) {
        this.id = id;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.fileSize = fileSize;
        this.skipped = skipped;
        this.maxRetries = maxRetries;
        this.taskType = taskType != null ? taskType : "FILE";
        this.fileCount = fileCount > 0 ? fileCount : 1;
        this.bundleManifest = bundleManifest;
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

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public int getFileCount() { return fileCount; }
    public void setFileCount(int fileCount) { this.fileCount = fileCount; }

    public String getBundleManifest() { return bundleManifest; }
    public void setBundleManifest(String bundleManifest) { this.bundleManifest = bundleManifest; }
}
